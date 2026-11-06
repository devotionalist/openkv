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

// @file raft_persist.cpp

#include "opskv/raft/raft_persist.hpp"
#include "opskv/codec.hpp"

#include "opskv/logging.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>

namespace opskv::raft {
namespace fs = std::filesystem;

namespace {



}  // namespace

RaftPersist::RaftPersist(std::string dir, SyncMode sync) : dir_(std::move(dir)), sync_(sync) {
  if (!dir_.empty()) {
    fs::create_directories(dir_);
  }
}

void RaftPersist::sync_file(const std::string& path) const {
  if (sync_ == SyncMode::kNone) return;
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return;
#if defined(F_FULLFSYNC)
  // macOS：更接近真实盘落稳
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

bool RaftPersist::load(PersistedHardState* hs, std::vector<LogEntry>* entries) const {
  if (!enabled()) return false;
  auto hs_path = fs::path(dir_) / "hard.state";
  auto log_path = fs::path(dir_) / "log.entries";
  if (!fs::exists(hs_path)) return false;

  std::ifstream hin(hs_path);
  if (!hin) return false;
  std::string voted;
  if (!(hin >> hs->term >> voted >> hs->commit_index >> hs->last_applied >> hs->first_index >>
        hs->snapshot_term)) {
    return false;
  }
  hs->voted_for = (voted == "-") ? "" : voted;

  entries->clear();
  if (fs::exists(log_path)) {
    std::ifstream lin(log_path);
    std::string line;
    while (std::getline(lin, line)) {
      if (line.empty()) continue;
      std::istringstream iss(line);
      LogEntry e;
      std::string hex;
      if (!(iss >> e.term >> e.index >> hex)) continue;
      if (!from_hex(hex, &e.data)) continue;
      entries->push_back(std::move(e));
    }
  }
  SKV_INFO << "raft persist loaded dir=" << dir_ << " term=" << hs->term
           << " commit=" << hs->commit_index << " applied=" << hs->last_applied
           << " entries=" << entries->size();
  return true;
}

void RaftPersist::save(const PersistedHardState& hs, const RaftLog& log) const {
  if (!enabled() || dir_.empty()) return;
  try {
    fs::create_directories(dir_);
    auto hs_path = fs::path(dir_) / "hard.state.tmp";
    auto hs_final = fs::path(dir_) / "hard.state";
    {
      std::ofstream hout(hs_path, std::ios::trunc);
      hout << hs.term << ' ' << (hs.voted_for.empty() ? "-" : hs.voted_for) << ' '
           << hs.commit_index << ' ' << hs.last_applied << ' ' << hs.first_index << ' '
           << hs.snapshot_term << '\n';
    }
    sync_file(hs_path.string());
    fs::rename(hs_path, hs_final);
    if (sync_ == SyncMode::kAll) {
      // 目录项
      int dfd = ::open(dir_.c_str(), O_RDONLY);
      if (dfd >= 0) {
#if defined(F_FULLFSYNC)
        ::fcntl(dfd, F_FULLFSYNC);
#else
        ::fsync(dfd);
#endif
        ::close(dfd);
      }
    }

    auto log_path = fs::path(dir_) / "log.entries.tmp";
    auto log_final = fs::path(dir_) / "log.entries";
    {
      std::ofstream lout(log_path, std::ios::trunc);
      for (uint64_t i = log.first_index(); i <= log.last_index(); ++i) {
        auto e = log.entry(i);
        if (!e) continue;
        lout << e->term << ' ' << e->index << ' ' << to_hex(e->data) << '\n';
      }
    }
    sync_file(log_path.string());
    fs::rename(log_path, log_final);
  } catch (const std::exception& e) {
    SKV_WARN << "raft persist save failed dir=" << dir_ << " err=" << e.what();
  }
}

}  // namespace opskv::raft
