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

// @file peer.hpp
// 职责：单 Region 运行时——RaftNode + PeerMeta + Hibernate/冷卸载。
// 架构：Store 的 peers_ 表元素；活动触达 touch_activity，空闲可 hibernate / unload。
// 协作：BatchSystem 在对应 worker 上串行处理该 Peer 的邮箱消息。

#pragma once

#include "openkv/raft/raft_node.hpp"
#include "openkv/raft/ready.hpp"
#include "openkv/region.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace openkv::raftstore {

struct PeerMeta {
  uint32_t region_id{0};
  openkv::KeyRange range;
  uint64_t epoch{0};
  std::string leader_id;
  std::vector<raft::Peer> peers;
};

/// 运行时 Peer：持有 external_io RaftNode（Ready 由 Store 外提）。
class Peer {
 public:
  Peer(uint32_t region_id, std::shared_ptr<raft::RaftNode> node);

  uint32_t region_id() const { return meta_.region_id; }
  PeerMeta& meta() { return meta_; }
  const PeerMeta& meta() const { return meta_; }

  raft::RaftNode* raft_node() { return node_.get(); }
  std::shared_ptr<raft::RaftNode> raft_node_shared() { return node_; }

  bool hibernating() const { return hibernating_.load(std::memory_order_relaxed); }
  // 设置是否休眠
  void set_hibernating(bool v) { hibernating_.store(v, std::memory_order_relaxed); }
  // 触摸活动
  void touch_activity();
  // 是否空闲
  bool idle_for(std::chrono::milliseconds d) const;
  // 冷卸载后仅保留 meta；唤醒时由 Store 重新 load。
  bool unloaded() const { return unloaded_.load(std::memory_order_relaxed); }
  // 标记为未加载
  void mark_unloaded() {
    unloaded_.store(true, std::memory_order_relaxed);
    node_.reset();
  }
  // 标记为已加载
  void mark_loaded(std::shared_ptr<raft::RaftNode> n) {
    node_ = std::move(n);
    unloaded_.store(false, std::memory_order_relaxed);
  }

  /// 异步 persist 进行中：禁止再 take_ready，直至 kPersistDone。
  bool persist_inflight() const { return persist_inflight_; }
  void set_persist_inflight(bool v) { persist_inflight_ = v; }

  void set_pending_ready(raft::Ready rd) { pending_ready_ = std::move(rd); }
  std::optional<raft::Ready> take_pending_ready() {
    auto out = std::move(pending_ready_);
    pending_ready_.reset();
    return out;
  }
  bool has_pending_ready() const { return pending_ready_.has_value(); }

 private:
  PeerMeta meta_;
  std::shared_ptr<raft::RaftNode> node_;
  std::atomic<bool> hibernating_{false};
  std::atomic<bool> unloaded_{false};
  mutable std::mutex act_mu_;
  std::chrono::steady_clock::time_point last_active_{std::chrono::steady_clock::now()};
  bool persist_inflight_{false};
  std::optional<raft::Ready> pending_ready_;
};

}  // namespace openkv::raftstore
