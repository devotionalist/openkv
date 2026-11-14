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

#include "openkv/raftstore/mailbox.hpp"

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace openkv::raftstore {

class Router {// 路由器
 public:
  using PeerMailbox = Mailbox<PeerMsg>;// 节点邮箱
  using ApplyMailbox = Mailbox<ApplyControl>;// 应用邮箱
  // 投递成功后通知 BatchSystem schedule（有活 peer 入队）。
  using NotifyFn = std::function<void(uint32_t region_id)>;

  // 注册节点
  std::shared_ptr<PeerMailbox> register_peer(uint32_t region_id, size_t cap = 4096);
  // 注册应用
  std::shared_ptr<ApplyMailbox> register_apply(uint32_t region_id, size_t cap = 4096);

  // 注销节点
  void deregister(uint32_t region_id);
  // 设置raft通知
  void set_raft_notify(NotifyFn fn) { raft_notify_ = std::move(fn); }
  // 设置应用通知
  void set_apply_notify(NotifyFn fn) { apply_notify_ = std::move(fn); }
  // 发送raft消息
  bool send_raft(uint32_t region_id, PeerMsg msg);
  // 发送应用消息
  bool send_apply(uint32_t region_id, ApplyControl msg);
  // 广播tick
  bool broadcast_tick();
  // 获取节点邮箱
  std::shared_ptr<PeerMailbox> peer_mailbox(uint32_t region_id);
  // 获取应用邮箱
  std::shared_ptr<ApplyMailbox> apply_mailbox(uint32_t region_id);
  // 获取节点区域列表
  std::vector<uint32_t> peer_regions() const;

  // 获取节点队列深度
  size_t peer_queue_depth(uint32_t region_id) const;
  // 获取应用队列深度
  size_t apply_queue_depth(uint32_t region_id) const;

 private:
  mutable std::mutex mu_;// 互斥锁
  std::unordered_map<uint32_t, std::shared_ptr<PeerMailbox>> peers_;// 节点邮箱列表
  std::unordered_map<uint32_t, std::shared_ptr<ApplyMailbox>> applies_;// 应用邮箱列表
  NotifyFn raft_notify_;// raft通知
  NotifyFn apply_notify_;// 应用通知
};

}  // namespace openkv::raftstore
