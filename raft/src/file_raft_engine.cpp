// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// @file file_raft_engine.cpp
// FileRaftEngine：RLG2 追加式日志；有 cut 时才整文件重写；state+log 同批落盘。

#include "opskv/raft/file_raft_engine.hpp"

#include "opskv/logging.hpp"
#include "opskv/raft/raft_log.hpp"
#include "opskv/raft/raft_persist.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <string_view>
#include <unordered_map>

namespace opskv::raft {
namespace fs = std::filesystem;

namespace {

size_t entry_record_size(std::string_view raw, size_t off) {
  if (off + 4 + 24 > raw.size()) return 0;
  const auto mag = raw.substr(off, 4);
  if (mag == "RLE2") {
    if (off + 4 + 24 + 4 > raw.size()) return 0;
    uint64_t dlen = 0;
    const auto* p = reinterpret_cast<const uint8_t*>(raw.data() + off + 4 + 16);
    for (int j = 0; j < 8; ++j) dlen = (dlen << 8) | p[j];
    return 4 + 24 + static_cast<size_t>(dlen) + 4;
  }
  if (mag == "RLE1") {
    uint64_t dlen = 0;
    const auto* p = reinterpret_cast<const uint8_t*>(raw.data() + off + 4 + 16);
    for (int j = 0; j < 8; ++j) dlen = (dlen << 8) | p[j];
    return 4 + 24 + static_cast<size_t>(dlen);
  }
  return 0;
}

std::string encode_entries_file(const std::vector<LogEntry>& ents) {
  std::string out;
  out.append("RLG2", 4);
  for (const auto& e : ents) out.append(encode_log_entry(e));
  return out;
}

bool decode_entries_file(std::string_view raw, std::vector<LogEntry>* out) {
  out->clear();
  if (raw.size() < 4) return false;
  if (raw.substr(0, 4) == "RLG2") {
    size_t off = 4;
    while (off < raw.size()) {
      const size_t need = entry_record_size(raw, off);
      if (need == 0 || off + need > raw.size()) return false;
      LogEntry e;
      if (!decode_log_entry(raw.substr(off, need), &e)) return false;
      out->push_back(std::move(e));
      off += need;
    }
    return true;
  }
  // 兼容 RLG1（count + RLE1）
  if (raw.size() < 12 || raw.substr(0, 4) != "RLG1") return false;
  uint64_t n = 0;
  for (int i = 0; i < 8; ++i) n = (n << 8) | static_cast<uint8_t>(raw[4 + i]);
  size_t off = 12;
  out->reserve(static_cast<size_t>(n));
  for (uint64_t i = 0; i < n; ++i) {
    const size_t need = entry_record_size(raw, off);
    if (need == 0 || off + need > raw.size()) return false;
    LogEntry e;
    if (!decode_log_entry(raw.substr(off, need), &e)) return false;
    out->push_back(std::move(e));
    off += need;
  }
  return true;
}

/// 是否可纯追加：无 cut，且 appends 严格接在当前 last_index 之后、无重叠。
bool can_pure_append(const std::vector<LogEntry>& existing, const std::vector<LogEntry>& appends,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts) {
  if (!cuts.empty() || appends.empty()) return false;
  uint64_t expect = existing.empty() ? appends.front().index : existing.back().index + 1;
  for (const auto& e : appends) {
    if (e.index != expect) return false;
    ++expect;
  }
  return true;
}

}  // namespace

class FileLogBatch final : public RaftLogBatch {
 public:
  void append(uint64_t raft_group_id, std::vector<LogEntry> entries) override {
    auto& g = muts_[raft_group_id];
    for (auto& e : entries) g.appends.push_back(std::move(e));
  }
  void cut_logs(uint64_t raft_group_id, uint64_t from, uint64_t to) override {
    if (to <= from) return;
    muts_[raft_group_id].cuts.emplace_back(from, to);
  }
  void put_raft_state(uint64_t raft_group_id, const RaftLocalState& state) override {
    muts_[raft_group_id].state = state;
  }
  bool empty() const override { return muts_.empty(); }

  std::unordered_map<uint64_t, FileRaftEngine::GroupMut> muts_;
};

FileRaftEngine::FileRaftEngine(std::string root_dir, SyncMode sync)
    : root_(std::move(root_dir)), sync_(sync) {
  if (!root_.empty()) {
    std::error_code ec;
    fs::create_directories(root_, ec);
  }
}

std::unique_ptr<RaftLogBatch> FileRaftEngine::make_log_batch(size_t) {
  return std::make_unique<FileLogBatch>();
}

fs::path FileRaftEngine::group_dir(uint64_t id) const {
  return fs::path(root_) / ("g" + std::to_string(id));
}

void FileRaftEngine::sync_path(const std::string& path) const {
  if (sync_ == SyncMode::kNone) return;
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return;
#if defined(F_FULLFSYNC)
  if (sync_ == SyncMode::kAll || sync_ == SyncMode::kData) {
    ::fcntl(fd, F_FULLFSYNC);
  }
#else
  if (sync_ == SyncMode::kData) {
    ::fdatasync(fd);
  } else if (sync_ == SyncMode::kAll) {
    ::fsync(fd);
  }
#endif
  ::close(fd);
}

void FileRaftEngine::sync_dir() const {
  if (sync_ != SyncMode::kAll || root_.empty()) return;
  int dfd = ::open(root_.c_str(), O_RDONLY);
  if (dfd < 0) return;
#if defined(F_FULLFSYNC)
  ::fcntl(dfd, F_FULLFSYNC);
#else
  ::fsync(dfd);
#endif
  ::close(dfd);
}

bool FileRaftEngine::read_state(uint64_t id, RaftLocalState* out) const {
  auto path = group_dir(id) / "raft_state";
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  std::string raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return decode_raft_local_state(raw, out);
}

bool FileRaftEngine::write_state(uint64_t id, const RaftLocalState& st, bool sync) const {
  auto dir = group_dir(id);
  fs::create_directories(dir);
  auto tmp = dir / "raft_state.tmp";
  auto finalp = dir / "raft_state";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    auto blob = encode_raft_local_state(st);
    out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
  }
  if (sync) sync_path(tmp.string());
  fs::rename(tmp, finalp);
  return true;
}

bool FileRaftEngine::read_all_entries(uint64_t id, std::vector<LogEntry>* out) const {
  auto path = group_dir(id) / "raft_log";
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    out->clear();
    return true;
  }
  std::string raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  if (raw.empty()) {
    out->clear();
    return true;
  }
  return decode_entries_file(raw, out);
}

bool FileRaftEngine::write_all_entries(uint64_t id, const std::vector<LogEntry>& ents,
                                      bool sync) const {
  auto dir = group_dir(id);
  fs::create_directories(dir);
  auto tmp = dir / "raft_log.tmp";
  auto finalp = dir / "raft_log";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    auto blob = encode_entries_file(ents);
    out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
  }
  if (sync) sync_path(tmp.string());
  fs::rename(tmp, finalp);
  return true;
}

bool FileRaftEngine::append_entries_file(uint64_t id, const std::vector<LogEntry>& ents,
                                         bool sync) const {
  if (ents.empty()) return true;
  auto dir = group_dir(id);
  fs::create_directories(dir);
  auto path = dir / "raft_log";
  bool write_hdr = true;
  if (fs::exists(path) && fs::file_size(path) > 0) {
    std::ifstream hin(path, std::ios::binary);
    char mag[4]{};
    hin.read(mag, 4);
    if (hin.gcount() != 4 || std::string_view(mag, 4) != "RLG2") return false;
    write_hdr = false;
  }
  std::ofstream out(path, std::ios::binary | std::ios::app);
  if (!out) return false;
  if (write_hdr) out.write("RLG2", 4);
  for (const auto& e : ents) {
    auto blob = encode_log_entry(e);
    out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
  }
  out.flush();
  if (sync) sync_path(path.string());
  return true;
}

size_t FileRaftEngine::apply_group_mut(uint64_t id, GroupMut& mut, bool sync) {
  std::vector<LogEntry> ents;
  read_all_entries(id, &ents);

  const bool pure = can_pure_append(ents, mut.appends, mut.cuts);
  const bool log_touched = !mut.cuts.empty() || !mut.appends.empty();

  size_t bytes = 0;
  if (pure) {
    if (!append_entries_file(id, mut.appends, sync)) {
      for (auto& e : mut.appends) ents.push_back(std::move(e));
      if (!write_all_entries(id, ents, sync)) return 0;
    } else {
      for (const auto& e : mut.appends) bytes += e.data.size() + 40;
      ents.insert(ents.end(), mut.appends.begin(), mut.appends.end());
    }
  } else if (log_touched) {
    for (const auto& [from, to] : mut.cuts) {
      ents.erase(std::remove_if(ents.begin(), ents.end(),
                                [&](const LogEntry& e) { return e.index >= from && e.index < to; }),
                 ents.end());
    }
    for (auto& e : mut.appends) {
      ents.erase(std::remove_if(ents.begin(), ents.end(),
                                [&](const LogEntry& x) { return x.index == e.index; }),
                 ents.end());
      ents.push_back(std::move(e));
    }
    std::sort(ents.begin(), ents.end(),
              [](const LogEntry& a, const LogEntry& b) { return a.index < b.index; });
    if (!write_all_entries(id, ents, sync)) return 0;
    bytes += ents.size() * 32;
  }

  RaftLocalState st;
  if (mut.state) {
    st = *mut.state;
  } else if (!read_state(id, &st)) {
    st.first_index = ents.empty() ? 1 : ents.front().index;
  }
  if (!ents.empty()) {
    st.last_index = ents.back().index;
    if (st.first_index == 0) st.first_index = ents.front().index;
  } else if (mut.state) {
    st.last_index = mut.state->last_index;
  }

  if (mut.state || log_touched) {
    if (!write_state(id, st, sync)) return 0;
    bytes += 64;
  }
  if (sync) sync_dir();
  return bytes;
}

size_t FileRaftEngine::consume(RaftLogBatch& batch, bool sync) {
  if (root_.empty()) return 0;
  auto* fb = dynamic_cast<FileLogBatch*>(&batch);
  if (!fb || fb->empty()) return 0;
  std::lock_guard<std::mutex> lock(mu_);
  const bool do_sync = sync && sync_ != SyncMode::kNone;
  size_t total = 0;
  for (auto& [id, mut] : fb->muts_) {
    total += apply_group_mut(id, mut, do_sync);
  }
  fb->muts_.clear();
  return total;
}

std::optional<RaftLocalState> FileRaftEngine::get_raft_state(uint64_t raft_group_id) const {
  if (root_.empty()) return std::nullopt;
  std::lock_guard<std::mutex> lock(mu_);
  RaftLocalState st;
  if (!read_state(raft_group_id, &st)) return std::nullopt;
  return st;
}

std::optional<LogEntry> FileRaftEngine::get_entry(uint64_t raft_group_id, uint64_t index) const {
  std::vector<LogEntry> ents;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!read_all_entries(raft_group_id, &ents)) return std::nullopt;
  }
  for (const auto& e : ents) {
    if (e.index == index) return e;
  }
  return std::nullopt;
}

size_t FileRaftEngine::fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end,
                                        size_t max_size, std::vector<LogEntry>* to) const {
  if (!to || end <= begin) return 0;
  std::vector<LogEntry> ents;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!read_all_entries(raft_group_id, &ents)) return 0;
  }
  size_t n = 0;
  size_t bytes = 0;
  for (const auto& e : ents) {
    if (e.index < begin || e.index >= end) continue;
    if (bytes + e.data.size() > max_size && n > 0) break;
    to->push_back(e);
    bytes += e.data.size();
    ++n;
  }
  return n;
}

size_t FileRaftEngine::gc(uint64_t raft_group_id, uint64_t from, uint64_t to) {
  auto batch = make_log_batch();
  batch->cut_logs(raft_group_id, from, to);
  return consume(*batch, sync_ != SyncMode::kNone);
}

void FileRaftEngine::clean(uint64_t raft_group_id) {
  if (root_.empty()) return;
  std::lock_guard<std::mutex> lock(mu_);
  std::error_code ec;
  fs::remove_all(group_dir(raft_group_id), ec);
}

bool FileRaftEngine::migrate_legacy_persist_dir(const std::string& legacy_dir,
                                               uint64_t raft_group_id) {
  RaftPersist legacy(legacy_dir, SyncMode::kNone);
  PersistedHardState hs;
  std::vector<LogEntry> ents;
  if (!legacy.load(&hs, &ents)) return false;
  RaftLog log;
  log.load(hs.first_index, hs.snapshot_term, ents);
  save_group_to_engine(*this, raft_group_id, hs, log, sync_ != SyncMode::kNone);
  SKV_INFO << "raft engine migrated legacy dir=" << legacy_dir << " group=" << raft_group_id
           << " entries=" << ents.size();
  return true;
}

}  // namespace opskv::raft
