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
// 实现：按 group 文件布局的 RaftEngine；纯追加 vs rewrite；state 原子替换。
// 生产 Storage 请用 RocksRaftEngine；本实现面向 Meta / UT。

#include "openkv/raft/file_raft_engine.hpp"

#include "openkv/logging.hpp"
#include "openkv/raft/raft_log.hpp"
#include "openkv/raft/raft_log_file.hpp"
#include "openkv/raft/raft_persist.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <string_view>
#include <unordered_map>

namespace openkv::raft {
namespace fs = std::filesystem;

/// 与 FileRaftEngine::friend 同命名空间，才能访问 GroupMut。
class FileLogBatch final : public RaftLogBatch {
 public:
  void append(uint64_t raft_group_id, std::vector<LogEntry> entries) override {
    if (entries.empty()) return;
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

namespace {

bool write_bytes_trunc(const fs::path& path, std::string_view blob) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
  out.flush();
  return static_cast<bool>(out);
}

bool read_entire_file(const fs::path& path, std::string* out) {
  if (!out) return false;
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    out->clear();
    return false;
  }
  out->assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  return true;
}

bool atomic_replace(const fs::path& tmp, const fs::path& final_path) {
  std::error_code ec;
  fs::rename(tmp, final_path, ec);
  return !ec;
}

}  // namespace

FileRaftEngine::GroupPaths FileRaftEngine::GroupPaths::for_group(const fs::path& root,
                                                                uint64_t group_id) {
  GroupPaths p;
  p.dir = root / ("g" + std::to_string(group_id));
  p.state = p.dir / "raft_state";
  p.state_tmp = p.dir / "raft_state.tmp";
  p.log = p.dir / "raft_log";
  p.log_tmp = p.dir / "raft_log.tmp";
  return p;
}

bool FileRaftEngine::GroupPaths::ensure_dir() const {
  std::error_code ec;
  fs::create_directories(dir, ec);
  return !ec || fs::is_directory(dir);
}

FileRaftEngine::FileRaftEngine(std::string root_dir, SyncMode sync)
    : root_(std::move(root_dir)), sync_(sync) {
  if (root_.empty()) return;
  std::error_code ec;
  fs::create_directories(root_, ec);
  if (ec) {
    SKV_WARN << "FileRaftEngine: create root failed path=" << root_ << " ec=" << ec.message();
  }
}

std::unique_ptr<RaftLogBatch> FileRaftEngine::make_log_batch(size_t) {
  return std::make_unique<FileLogBatch>();
}

FileRaftEngine::GroupPaths FileRaftEngine::paths_for(uint64_t group_id) const {
  return GroupPaths::for_group(fs::path(root_), group_id);
}

void FileRaftEngine::sync_file(const fs::path& path) const {
  if (sync_ == SyncMode::kNone) return;
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    SKV_WARN << "FileRaftEngine: sync open failed path=" << path.string();
    return;
  }
#if defined(F_FULLFSYNC)
  if (sync_ == SyncMode::kAll || sync_ == SyncMode::kData) {
    if (::fcntl(fd, F_FULLFSYNC) != 0) {
      SKV_WARN << "FileRaftEngine: F_FULLFSYNC failed path=" << path.string();
    }
  }
#else
  if (sync_ == SyncMode::kData) {
    if (::fdatasync(fd) != 0) {
      SKV_WARN << "FileRaftEngine: fdatasync failed path=" << path.string();
    }
  } else if (sync_ == SyncMode::kAll) {
    if (::fsync(fd) != 0) {
      SKV_WARN << "FileRaftEngine: fsync failed path=" << path.string();
    }
  }
#endif
  ::close(fd);
}

void FileRaftEngine::sync_root_dir() const {
  if (sync_ != SyncMode::kAll || root_.empty()) return;
  const int dfd = ::open(root_.c_str(), O_RDONLY);
  if (dfd < 0) {
    SKV_WARN << "FileRaftEngine: sync root open failed path=" << root_;
    return;
  }
#if defined(F_FULLFSYNC)
  if (::fcntl(dfd, F_FULLFSYNC) != 0) {
    SKV_WARN << "FileRaftEngine: root F_FULLFSYNC failed path=" << root_;
  }
#else
  if (::fsync(dfd) != 0) {
    SKV_WARN << "FileRaftEngine: root fsync failed path=" << root_;
  }
#endif
  ::close(dfd);
}

bool FileRaftEngine::read_state_unlocked(const GroupPaths& p, RaftLocalState* out) const {
  std::string raw;
  if (!read_entire_file(p.state, &raw) || raw.empty()) return false;
  return decode_raft_local_state(raw, out);
}

bool FileRaftEngine::write_state_unlocked(const GroupPaths& p, const RaftLocalState& st,
                                          bool sync) const {
  if (!p.ensure_dir()) {
    SKV_ERROR << "FileRaftEngine: ensure group dir failed dir=" << p.dir.string();
    return false;
  }
  const auto blob = encode_raft_local_state(st);
  if (!write_bytes_trunc(p.state_tmp, blob)) {
    SKV_ERROR << "FileRaftEngine: write state tmp failed path=" << p.state_tmp.string();
    return false;
  }
  if (sync) sync_file(p.state_tmp);
  if (!atomic_replace(p.state_tmp, p.state)) {
    SKV_ERROR << "FileRaftEngine: rename state failed path=" << p.state.string();
    return false;
  }
  return true;
}

bool FileRaftEngine::read_log_unlocked(const GroupPaths& p, std::vector<LogEntry>* out) const {
  if (!out) return false;
  std::string raw;
  if (!read_entire_file(p.log, &raw)) {
    // 文件不存在 = 空日志
    out->clear();
    return true;
  }
  if (raw.empty()) {
    out->clear();
    return true;
  }
  if (!log_file::decode_container(raw, out)) {
    SKV_ERROR << "FileRaftEngine: decode log failed path=" << p.log.string()
              << " bytes=" << raw.size();
    return false;
  }
  return true;
}

bool FileRaftEngine::rewrite_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents,
                                          bool sync) const {
  if (!p.ensure_dir()) {
    SKV_ERROR << "FileRaftEngine: ensure group dir failed dir=" << p.dir.string();
    return false;
  }
  const auto blob = log_file::encode_container(ents);
  if (!write_bytes_trunc(p.log_tmp, blob)) {
    SKV_ERROR << "FileRaftEngine: write log tmp failed path=" << p.log_tmp.string();
    return false;
  }
  if (sync) sync_file(p.log_tmp);
  if (!atomic_replace(p.log_tmp, p.log)) {
    SKV_ERROR << "FileRaftEngine: rename log failed path=" << p.log.string();
    return false;
  }
  return true;
}

bool FileRaftEngine::append_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents,
                                         bool sync) const {
  // 纯追加：ios::app 不截断。魔数仅在空文件写一次；之后只 append RLE 记录。
  // 切勿把 encode_container() 的结果再 append——会在文件中段插入第二个 RLG2。
  if (ents.empty()) return true;
  if (!p.ensure_dir()) {
    SKV_ERROR << "FileRaftEngine: ensure group dir failed dir=" << p.dir.string();
    return false;
  }

  bool write_hdr = true;
  std::error_code ec;
  const auto sz = fs::file_size(p.log, ec);
  if (!ec && sz > 0) {
    std::ifstream hin(p.log, std::ios::binary);
    char mag[4]{};
    hin.read(mag, 4);
    if (hin.gcount() != 4 || std::string_view(mag, 4) != log_file::kMagicRlg2) {
      SKV_WARN << "FileRaftEngine: log not RLG2, refuse append path=" << p.log.string();
      return false;
    }
    write_hdr = false;
  }

  std::ofstream out(p.log, std::ios::binary | std::ios::app);
  if (!out) {
    SKV_ERROR << "FileRaftEngine: open log for append failed path=" << p.log.string();
    return false;
  }
  if (write_hdr) {
    out.write(log_file::kMagicRlg2.data(),
              static_cast<std::streamsize>(log_file::kMagicRlg2.size()));
  }
  for (const auto& e : ents) {
    const auto blob = encode_log_entry(e);
    out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
  }
  out.flush();
  if (!out) {
    SKV_ERROR << "FileRaftEngine: append write failed path=" << p.log.string();
    return false;
  }
  if (sync) sync_file(p.log);
  return true;
}

size_t FileRaftEngine::approx_entry_bytes(const LogEntry& e) {
  return e.data.size() + 40;
}

RaftLocalState FileRaftEngine::merge_state(const GroupMut& mut, const std::vector<LogEntry>& ents,
                                           const RaftLocalState* existing) {
  RaftLocalState st;
  if (mut.state) {
    st = *mut.state;
  } else if (existing) {
    st = *existing;
  } else {
    st.first_index = ents.empty() ? 1 : ents.front().index;
  }

  if (!ents.empty()) {
    st.last_index = ents.back().index;
    if (st.first_index == 0) st.first_index = ents.front().index;
  } else if (mut.state) {
    st.last_index = mut.state->last_index;
  }
  return st;
}

size_t FileRaftEngine::apply_group_mut_unlocked(uint64_t group_id, GroupMut& mut, bool sync) {
  const auto p = paths_for(group_id);

  std::vector<LogEntry> ents;
  if (!read_log_unlocked(p, &ents)) return 0;

  RaftLocalState existing_st;
  const bool had_state = read_state_unlocked(p, &existing_st);

  const bool pure = log_file::can_pure_append(ents, mut.appends, mut.cuts);
  const bool log_touched = !mut.cuts.empty() || !mut.appends.empty();

  size_t bytes = 0;

  if (pure) {
    if (!append_log_unlocked(p, mut.appends, sync)) {
      // 非 RLG2 或 append IO 失败：退化为整文件 rewrite，保证可恢复。
      for (auto& e : mut.appends) ents.push_back(std::move(e));
      if (!rewrite_log_unlocked(p, ents, sync)) return 0;
      for (const auto& e : ents) bytes += approx_entry_bytes(e);
    } else {
      for (const auto& e : mut.appends) bytes += approx_entry_bytes(e);
      ents.insert(ents.end(), mut.appends.begin(), mut.appends.end());
    }
  } else if (log_touched) {
    log_file::apply_mutations(&ents, mut.cuts, std::move(mut.appends));
    mut.appends.clear();
    if (!rewrite_log_unlocked(p, ents, sync)) return 0;
    for (const auto& e : ents) bytes += approx_entry_bytes(e);
  }

  if (!mut.state && !log_touched) return bytes;

  const RaftLocalState st =
      merge_state(mut, ents, had_state ? &existing_st : nullptr);
  if (!write_state_unlocked(p, st, sync)) return 0;
  bytes += 64;

  if (sync) sync_root_dir();
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
    if (!mut.state && mut.appends.empty() && mut.cuts.empty()) continue;
    const size_t n = apply_group_mut_unlocked(id, mut, do_sync);
    if (n == 0) {
      SKV_ERROR << "FileRaftEngine: consume failed group=" << id;
      fb->muts_.clear();
      return 0;
    }
    total += n;
  }
  fb->muts_.clear();
  return total;
}

std::optional<RaftLocalState> FileRaftEngine::get_raft_state(uint64_t raft_group_id) const {
  if (root_.empty()) return std::nullopt;
  std::lock_guard<std::mutex> lock(mu_);
  RaftLocalState st;
  if (!read_state_unlocked(paths_for(raft_group_id), &st)) return std::nullopt;
  return st;
}

std::optional<LogEntry> FileRaftEngine::get_entry(uint64_t raft_group_id, uint64_t index) const {
  if (root_.empty()) return std::nullopt;
  std::vector<LogEntry> ents;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!read_log_unlocked(paths_for(raft_group_id), &ents)) return std::nullopt;
  }
  for (const auto& e : ents) {
    if (e.index == index) return e;
  }
  return std::nullopt;
}

size_t FileRaftEngine::fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end,
                                        size_t max_size, std::vector<LogEntry>* to) const {
  if (!to || end <= begin || root_.empty()) return 0;
  std::vector<LogEntry> ents;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!read_log_unlocked(paths_for(raft_group_id), &ents)) return 0;
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
  fs::remove_all(paths_for(raft_group_id).dir, ec);
  if (ec) {
    SKV_WARN << "FileRaftEngine: clean failed group=" << raft_group_id << " ec=" << ec.message();
  }
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
  SKV_INFO << "FileRaftEngine: migrated legacy dir=" << legacy_dir << " group=" << raft_group_id
           << " entries=" << ents.size();
  return true;
}

}  // namespace openkv::raft
