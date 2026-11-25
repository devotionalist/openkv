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
// 职责：单 Group **内存** Raft 日志（append / match / truncate / compact / load）。
// 架构：每个 RaftNode 持有一份；**本类不落盘**。
// 协作：落盘由 RaftNode::build_engine_batch_locked → RaftEngine::consume 完成。
// 注意：空日志时 last_index()==first_index-1（与论文一致）；compact 推进 first_index_。

#pragma once

#include <cstddef>
#include <optional>

#include "openkv/raft/core/types.hpp"

namespace openkv::raft {

/* 连续 index 区间的日志存储；first_index_ 可因 compact 前移。 */
class RaftLog {
 public:
  /**
   * 构造空日志窗。
   * @param first_index 首条有效 index；默认 1
   */
  explicit RaftLog(uint64_t first_index = 1);

  /**
   * 末条日志 index；空窗时为 first_index-1。
   * @return last_index
   */
  uint64_t last_index() const;
  /**
   * 末条日志 term；空窗时用 snapshot_term_。
   * @return last_term
   */
  uint64_t last_term() const;
  /**
   * 首条有效日志 index。
   * @return first_index_
   */
  uint64_t first_index() const { return first_index_; }

  /**
   * 按索引查询；越界返回 nullopt。
   * @param index 日志下标
   * @return 条目或 nullopt
   */
  std::optional<LogEntry> entry(uint64_t index) const;
  /**
   * 从索引起最多 limit 条（且累计 data 字节不超过 max_bytes），用于 AppendEntries。
   * @param index 起始下标
   * @param limit 最多条数；默认 64
   * @param max_bytes 累计 data 字节上限；默认无上限
   * @return 条目切片
   */
  std::vector<LogEntry> entries_from(uint64_t index, size_t limit = 64,
                                     size_t max_bytes = SIZE_MAX) const;

  /**
   * Leader 本地追加一条提案。
   * @param term 当前任期
   * @param data 状态机载荷
   * @return 新条目的 index
   */
  uint64_t append(uint64_t term, std::string data);

  /**
   * Follower 合并：校验 prev 后追加；冲突处 truncate 再写。
   * @param prev_index 前一条下标
   * @param prev_term 前一条 term
   * @param entries 本批条目
   * @return prev 是否匹配且合并成功
   */
  bool append_entries(uint64_t prev_index, uint64_t prev_term,
                      const std::vector<LogEntry>& entries);

  /**
   * index 处 term 是否与给定一致（含快照边界 index==first-1）。
   * @param index 日志下标
   * @param term 期望 term
   * @return 是否匹配
   */
  bool match(uint64_t index, uint64_t term) const;
  /**
   * 丢弃 index 及之后所有条目。
   * @param index 截断起点（含）
   */
  void truncate_from(uint64_t index);

  /**
   * 安装快照后推进 first_index_ 并记录 snapshot_term_。
   * @param last_included_index 快照包含的末 index
   * @param last_included_term 对应 term
   */
  void compact_to(uint64_t last_included_index, uint64_t last_included_term);

  /**
   * 快照边界 term（空窗时 last_term 用它）。
   * @return snapshot_term_
   */
  uint64_t snapshot_term() const { return snapshot_term_; }
  /**
   * 从持久化装载：替换内存日志。
   * @param first_index 首 index
   * @param snapshot_term 快照边界 term
   * @param entries 连续条目
   */
  void load(uint64_t first_index, uint64_t snapshot_term, std::vector<LogEntry> entries);
  /**
   * 内存中条目条数（不含已 compact 掉的）。
   * @return entries_.size()
   */
  size_t size() const { return entries_.size(); }

 private:
  uint64_t first_index_;           // 首条有效日志索引
  uint64_t snapshot_term_{0};      // 快照边界 term
  std::vector<LogEntry> entries_;  // 连续区间 [first_index_, last_index]
};

}  // namespace openkv::raft
