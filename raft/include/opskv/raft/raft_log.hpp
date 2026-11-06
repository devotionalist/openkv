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

// @file raft_log.hpp
// 内存 Raft 日志：追加、冲突截断、快照压缩边界。
//
// 架构位置：每个 RaftNode 持有一份 RaftLog；不单独持久化（demo 简化，生产需 WAL）。

#pragma once

#include "opskv/raft/types.hpp"

#include <cstddef>
#include <optional>

namespace opskv::raft {

/// 连续 index 区间的日志存储；first_index_ 可因 compact 前移。
class RaftLog {
 public:
  /// @param first_index 首条有效日志 index（默认 1；快照后可能 >1）
  explicit RaftLog(uint64_t first_index = 1);

  uint64_t last_index() const;
  uint64_t last_term() const;
  uint64_t first_index() const { return first_index_; }

  /// 按 index 查询；越界返回 nullopt。
  std::optional<LogEntry> entry(uint64_t index) const;
  /// 从 index 起最多 limit 条（且累计 data 字节不超过 max_bytes），用于 AppendEntries。
  std::vector<LogEntry> entries_from(uint64_t index, size_t limit = 64,
                                     size_t max_bytes = SIZE_MAX) const;

  // 追加本地提案（Leader）
  /// Leader 本地追加；返回新条目的 index。
  uint64_t append(uint64_t term, std::string data);

  // Follower 追加：冲突则截断
  /// 校验 prev 匹配后合并 entries；term 冲突处 truncate 再追加。
  bool append_entries(uint64_t prev_index, uint64_t prev_term,
                      const std::vector<LogEntry>& entries);

  /// index 处 term 是否与给定一致（含快照边界 index==first-1）。
  bool match(uint64_t index, uint64_t term) const;
  /// 丢弃 index 及之后所有条目。
  void truncate_from(uint64_t index);

  // 快照压缩：丢弃 last_included_index 及之前的日志
  /// 安装快照后推进 first_index_ 并记录 snapshot_term_。
  void compact_to(uint64_t last_included_index, uint64_t last_included_term);

  uint64_t snapshot_term() const { return snapshot_term_; }
  /// 从持久化装载：替换内存日志。
  void load(uint64_t first_index, uint64_t snapshot_term, std::vector<LogEntry> entries);
  size_t size() const { return entries_.size(); }

 private:
  uint64_t first_index_;
  uint64_t snapshot_term_{0};
  std::vector<LogEntry> entries_;  // 连续区间 [first_index_, last_index]
};

}  // namespace opskv::raft
