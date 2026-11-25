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

// @file replication.hpp
// 日志复制与追赶：心跳/AE、抬 commit、InstallSnapshot（无锁，由 RaftNode::mu_ 保护）。

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "openkv/raft/core/types.hpp"

namespace openkv::raft {

class RaftNode;

/* 管「日志怎么同步给别人、commit 怎么抬、落后了怎么发快照」。 */
class Replication {
 public:
  /**
   * 绑定所属 RaftNode。
   * @param node 宿主节点
   */
  explicit Replication(RaftNode& node) : n_(node) {}

  /**
   * 向各 peer 推送：有日志发 AE，空则 probe/心跳，掉窗则 InstallSnapshot。
   */
  void broadcast_replicate();
  /**
   * need_replicate_ 时才推一轮（Leader）。
   */
  void maybe_replicate();
  /**
   * 按各 peer match 抬 commit_index_（多数派）。
   */
  void maybe_commit();

  /**
   * 某 peer 在途 AE 计数 -1。
   * @param peer_id 对端
   */
  void dec_inflight(const std::string& peer_id);
  /**
   * 尝试占用一个在途名额；已满返回 false。
   * @param peer_id 对端
   * @return 是否占用成功
   */
  bool try_inc_inflight(const std::string& peer_id);

  /**
   * 入站 AE（已持锁）。
   * @param from 对端 peer id
   * @param term 任期
   * @param prev_idx 前一条日志下标
   * @param prev_term 前一条日志 term
   * @param entries 日志条目
   * @param leader_commit Leader 声称的 commit 下标
   * @param success 输出：是否接受
   * @param match_index 输出：本端匹配下标
   */
  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  /**
   * 入站 AE 应答（已持锁；调用方负责 flush/notify）。
   * @param from 对端 peer id
   * @param term 任期
   * @param success 本轮复制是否成功
   * @param match_index 对方已匹配日志下标
   */
  void on_append_entries_reply_locked(const std::string& from, uint64_t term, bool success,
                                      uint64_t match_index);
  /**
   * 入站 InstallSnapshot（已持锁）。
   * @param from 对端 peer id
   * @param term 任期
   * @param last_index 快照末下标
   * @param last_term 对应 term
   * @param data 快照载荷
   * @param success 输出：是否安装成功
   */
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  /**
   * 入站快照应答（已持锁；调用方负责 flush/notify）。
   * @param from 对端 peer id
   * @param term 任期
   * @param success 是否成功
   * @param match_index 安装后匹配下标
   */
  void on_install_snapshot_reply_locked(const std::string& from, uint64_t term, bool success,
                                        uint64_t match_index);

  bool need_replicate_{false};  // 有人请求尽快再推一轮复制
  std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      last_snap_sent_;               // 每 peer 上次发快照时刻（限频）
  uint64_t snapshot_send_count_{0};  // 累计发快照次数

 private:
  RaftNode& n_;  // 宿主节点
};

}  // namespace openkv::raft
