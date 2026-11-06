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

// @file raft_persist.hpp
// 旧版 per-dir 文本持久化（hard.state + log.entries）。
// 新路径请用 RaftEngine（FileRaftEngine / RocksRaftEngine）；本类仅作迁移与 SyncMode 定义宿主。
//
// 布局：
//   {dir}/hard.state   — term voted_for commit last_applied first_index snap_term
//   {dir}/log.entries — 每行: term index hex(data)

#pragma once

#include "opskv/raft/raft_log.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace opskv::raft {

struct PersistedHardState {
  uint64_t term{0};
  std::string voted_for;
  uint64_t commit_index{0};
  uint64_t last_applied{0};
  uint64_t first_index{1};
  uint64_t snapshot_term{0};
};

/// P2 / Store E1：落盘同步级别（Raft 日志与 RocksDB WAL 共用语义）。
enum class SyncMode {
  kNone = 0,  // 只写缓冲，不 fsync（最快，崩溃可丢尾部）
  kData = 1,  // fdatasync / RocksDB sync + fdatasync
  kAll = 2,   // 元数据+数据 fsync（最稳）
};

inline SyncMode parse_sync_mode(std::string_view s, SyncMode def = SyncMode::kData) {
  if (s == "none" || s == "None" || s == "0") return SyncMode::kNone;
  if (s == "data" || s == "Data" || s == "1") return SyncMode::kData;
  if (s == "all" || s == "All" || s == "2") return SyncMode::kAll;
  return def;
}

inline const char* sync_mode_name(SyncMode m) {
  switch (m) {
    case SyncMode::kNone:
      return "none";
    case SyncMode::kData:
      return "data";
    case SyncMode::kAll:
      return "all";
  }
  return "data";
}

class RaftPersist {
 public:
  explicit RaftPersist(std::string dir, SyncMode sync = SyncMode::kData);

  const std::string& dir() const { return dir_; }
  bool enabled() const { return !dir_.empty(); }
  SyncMode sync_mode() const { return sync_; }
  void set_sync_mode(SyncMode m) { sync_ = m; }

  /// 目录不存在或空则返回 false（冷启动）。
  bool load(PersistedHardState* hs, std::vector<LogEntry>* entries) const;
  void save(const PersistedHardState& hs, const RaftLog& log) const;

 private:
  void sync_file(const std::string& path) const;

  std::string dir_;
  SyncMode sync_{SyncMode::kData};
};

}  // namespace opskv::raft
