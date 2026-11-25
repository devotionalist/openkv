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

// @file router.hpp
// 职责：按 region_id 注册/注销邮箱，并 send_raft / send_apply。
// 架构：投递成功可选调用 NotifyFn，驱动 BatchSystem schedule。
// 协作：Store 创建时挂接 notify；destroy Region 时 deregister。

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "openkv/raftstore/mailbox.hpp"

namespace openkv::raftstore {

/* 按 Region 挂 Peer/Apply 邮箱，投递成功后通知 BatchSystem。 */
class Router {
 public:
  using PeerMailbox = Mailbox<PeerMsg>;
  using ApplyMailbox = Mailbox<ApplyControl>;
  // 投递成功后通知 BatchSystem schedule（有活 peer 入队）。
  using NotifyFn = std::function<void(uint32_t region_id)>;

  /**
   * 为 Region 注册 Raft 邮箱；已存在则返回已有。
   * @param region_id Region id
   * @param cap 邮箱容量；0 时用默认
   * @return 邮箱 shared_ptr
   */
  std::shared_ptr<PeerMailbox> register_peer(uint32_t region_id, size_t cap = 4096);
  /**
   * 为 Region 注册 Apply 邮箱；已存在则返回已有。
   * @param region_id Region id
   * @param cap 邮箱容量；0 时用默认
   * @return 邮箱 shared_ptr
   */
  std::shared_ptr<ApplyMailbox> register_apply(uint32_t region_id, size_t cap = 4096);

  /**
   * 注销该 Region 的 Peer/Apply 邮箱。
   * @param region_id Region id
   */
  void deregister(uint32_t region_id);
  /**
   * 挂 Raft 投递成功回调（通常 BatchSystem::schedule_raft）。
   * @param fn region_id → 唤醒对应 worker
   */
  void set_raft_notify(NotifyFn fn) { raft_notify_ = std::move(fn); }
  /**
   * 挂 Apply 投递成功回调（通常 BatchSystem::schedule_apply）。
   * @param fn region_id → 唤醒对应 worker
   */
  void set_apply_notify(NotifyFn fn) { apply_notify_ = std::move(fn); }
  /**
   * 向 Peer 邮箱投一条消息；成功则 raft_notify_。
   * @param region_id Region id
   * @param msg Tick / RaftMessage / SignificantMsg / ApplyRes
   * @return 是否入队成功
   */
  bool send_raft(uint32_t region_id, PeerMsg msg);
  /**
   * 向 Apply 邮箱投一条消息；成功则 apply_notify_。
   * @param region_id Region id
   * @param msg ApplyMsg 或 SnapTask
   * @return 是否入队成功
   */
  bool send_apply(uint32_t region_id, ApplyControl msg);
  /**
   * 向所有已注册 Peer 邮箱投 TickMsg。
   * @return 是否至少投递成功一次
   */
  bool broadcast_tick();
  /**
   * 取 Peer 邮箱；未注册返回空。
   * @param region_id Region id
   * @return shared_ptr 或空
   */
  std::shared_ptr<PeerMailbox> peer_mailbox(uint32_t region_id);
  /**
   * 取 Apply 邮箱；未注册返回空。
   * @param region_id Region id
   * @return shared_ptr 或空
   */
  std::shared_ptr<ApplyMailbox> apply_mailbox(uint32_t region_id);
  /**
   * 已注册 Peer 邮箱的 Region 列表。
   * @return region_id 集合
   */
  std::vector<uint32_t> peer_regions() const;

  /**
   * Peer 邮箱当前深度。
   * @param region_id Region id
   * @return 队列长度；未注册为 0
   */
  size_t peer_queue_depth(uint32_t region_id) const;
  /**
   * Apply 邮箱当前深度。
   * @param region_id Region id
   * @return 队列长度；未注册为 0
   */
  size_t apply_queue_depth(uint32_t region_id) const;

 private:
  mutable std::mutex mu_;                                                // 保护表与 notify
  std::unordered_map<uint32_t, std::shared_ptr<PeerMailbox>> peers_;     // Raft 邮箱
  std::unordered_map<uint32_t, std::shared_ptr<ApplyMailbox>> applies_;  // Apply 邮箱
  NotifyFn raft_notify_;                                                 // Raft 唤醒
  NotifyFn apply_notify_;                                                // Apply 唤醒
};

}  // namespace openkv::raftstore
