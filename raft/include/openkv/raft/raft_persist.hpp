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
// 职责：① SyncMode / PersistedHardState 定义宿主；② 旧版 per-dir 文本持久化（迁移兼容）。
// 架构：新路径一律走 RaftEngine；本类 RaftPersist 仅遗留目录导入与单测。
// 协作：parse_sync_mode / raft_log_sync / kv_apply_sync 被 Storage / RaftNode Options 使用。
// 布局（旧）：{dir}/hard.state · {dir}/log.entries（hex data）。

#pragma once

#include "openkv/raft/raft_log.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace openkv::raft {

struct PersistedHardState {
  uint64_t term{0};
  std::string voted_for;
  uint64_t commit_index{0};
  uint64_t last_applied{0};
  uint64_t first_index{1};
  uint64_t snapshot_term{0};
};

/// Store E1：耐久级别。默认 kData = **耐久靠 Raft log**；KV apply 异步。
enum class SyncMode {
  kNone = 0,  // Raft/KV 都不 sync（最快；崩溃可丢未 sync 的 Raft 尾部）
  kData = 1,  // Raft log fdatasync；RocksDB apply **异步**（崩溃用日志重放补齐）
  kAll = 2,   // Raft + RocksDB 都 sync（最稳，吞吐最低）
};

/// Raft 日志是否要求 sync（none 除外）。
inline bool raft_log_sync(SyncMode m) { return m != SyncMode::kNone; }
/// RocksDB WriteOptions.sync：仅 kAll；kData 靠 Raft 耐久。
inline bool kv_apply_sync(SyncMode m) { return m == SyncMode::kAll; }

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

}  // namespace openkv::raft
