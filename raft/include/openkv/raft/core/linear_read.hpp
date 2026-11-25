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

// @file linear_read.hpp
// 线性读：Leader lease + ReadIndex（多数派心跳确认）。

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <string>
#include <unordered_set>

namespace openkv::raft {

class RaftNode;

/* 管「读能不能本地确认、还是要等多数派 ACK」。 */
class LinearRead {
 public:
  /**
   * 绑定所属 RaftNode。
   * @param node 宿主节点
   */
  explicit LinearRead(RaftNode& node) : n_(node) {}

  /**
   * 当前还在租约窗内、可读本地 commit（须已持 mu_）。
   * @return true 表示租约有效
   */
  bool lease_valid_locked() const;
  /**
   * 收到 follower 心跳/AE 成功应答后刷新租约截止时间。
   */
  void renew_lease_locked();
  /**
   * 记一票 ReadIndex ACK；够多数就叫醒等着的 read_index。
   * @param peer_id 回 ACK 的 peer
   */
  void note_read_index_ack(const std::string& peer_id);
  /**
   * 成功则 *read_commit 为可读水位；内部会放锁发心跳再等。
   * @param read_commit 输出可读 commit
   * @param timeout_ms 等多数 ACK 的超时
   * @return 是否成功
   */
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms);

  std::chrono::steady_clock::time_point lease_expire_{};  // 租约过期时刻
  uint64_t read_index_gen_{0};                            // 单调递增，每次发起 +1
  uint64_t pending_read_gen_{0};                          // 正在等的那轮；0=没有在等
  uint64_t pending_read_commit_{0};                       // 发起时冻结的可读 commit
  std::unordered_set<std::string> read_index_acks_;       // 本轮已 ACK 的 peer
  std::condition_variable read_cv_;                       // 多数达成 / 超时醒来

 private:
  RaftNode& n_;  // 宿主节点
};

}  // namespace openkv::raft
