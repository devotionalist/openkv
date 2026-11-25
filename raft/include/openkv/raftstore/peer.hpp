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

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/ready.hpp"
#include "openkv/region.hpp"

namespace openkv::raftstore {

/* 跟 Region 走的本地元信息（range/epoch/成员），供路由与运维。 */
struct PeerMeta {
  uint32_t region_id{0};          // Region id
  openkv::KeyRange range;         // key 范围
  uint64_t epoch{0};              // 路由纪元
  std::string leader_id;          // 已知 Leader
  std::vector<raft::Peer> peers;  // 成员表快照
};

/* 运行时 Peer：持有 external_io RaftNode（Ready 由 Store 外提）。 */
class Peer {
 public:
  /**
   * 绑定 region 与已构造的 RaftNode。
   * @param region_id Region id
   * @param node 协议节点（external_io）
   */
  Peer(uint32_t region_id, std::shared_ptr<raft::RaftNode> node);

  /**
   * 本 Peer 对应的 Region id。
   * @return region_id
   */
  uint32_t region_id() const { return meta_.region_id; }
  /**
   * 可写元数据。
   * @return PeerMeta 引用
   */
  PeerMeta& meta() { return meta_; }
  /**
   * 只读元数据。
   * @return PeerMeta 常量引用
   */
  const PeerMeta& meta() const { return meta_; }

  /**
   * 当前 RaftNode 裸指针；已卸载则为 nullptr。
   * @return 节点或 nullptr
   */
  raft::RaftNode* raft_node() { return node_.get(); }
  /**
   * 当前 RaftNode 的 shared_ptr；已卸载则为空。
   * @return shared_ptr
   */
  std::shared_ptr<raft::RaftNode> raft_node_shared() { return node_; }

  /**
   * 是否处于 Hibernate（低频 tick，少干活）。
   * @return true 表示休眠中
   */
  bool hibernating() const { return hibernating_.load(std::memory_order_relaxed); }
  /**
   * 设置 Hibernate 标志。
   * @param v true=进入休眠
   */
  void set_hibernating(bool v) { hibernating_.store(v, std::memory_order_relaxed); }
  /**
   * 记录最近活动时间，供 idle_for / Hibernate 判断。
   */
  void touch_activity();
  /**
   * 距上次活动是否已超过给定时长。
   * @param d 空闲阈值
   * @return true 表示已空闲够久
   */
  bool idle_for(std::chrono::milliseconds d) const;
  /**
   * 冷卸载后仅保留 meta；唤醒时由 Store 重新 load。
   * @return true 表示协议状态已卸
   */
  bool unloaded() const { return unloaded_.load(std::memory_order_relaxed); }
  /**
   * 丢掉 RaftNode，标记 unloaded。
   */
  void mark_unloaded() {
    unloaded_.store(true, std::memory_order_relaxed);
    node_.reset();
  }
  /**
   * 重新挂上 RaftNode，清除 unloaded。
   * @param n 重新加载的节点
   */
  void mark_loaded(std::shared_ptr<raft::RaftNode> n) {
    node_ = std::move(n);
    unloaded_.store(false, std::memory_order_relaxed);
  }

  /**
   * 异步 persist 进行中：禁止再 take_ready，直至 kPersistDone。
   * @return true 表示落盘未完成
   */
  bool persist_inflight() const { return persist_inflight_; }
  /**
   * 设置异步 persist 进行中标志。
   * @param v true=正在流水线落盘
   */
  void set_persist_inflight(bool v) { persist_inflight_ = v; }

  /**
   * 暂存待 PersistDone 后再发网/apply 的 Ready。
   * @param rd 已 take 出的 Ready
   */
  void set_pending_ready(raft::Ready rd) { pending_ready_ = std::move(rd); }
  /**
   * 取出并清空暂存 Ready。
   * @return Ready 或 nullopt
   */
  std::optional<raft::Ready> take_pending_ready() {
    auto out = std::move(pending_ready_);
    pending_ready_.reset();
    return out;
  }
  /**
   * 是否有暂存 Ready。
   * @return true 表示还有未 dispatch 的 Ready
   */
  bool has_pending_ready() const { return pending_ready_.has_value(); }

 private:
  PeerMeta meta_;                         // range/epoch/成员
  std::shared_ptr<raft::RaftNode> node_;  // 协议节点；卸载后为空
  std::atomic<bool> hibernating_{false};  // Hibernate 标志
  std::atomic<bool> unloaded_{false};     // 冷卸载标志
  mutable std::mutex act_mu_;             // 保护 last_active_
  std::chrono::steady_clock::time_point last_active_{std::chrono::steady_clock::now()};  // 最近活动
  bool persist_inflight_{false};              // 异步落盘进行中
  std::optional<raft::Ready> pending_ready_;  // 等 PersistDone 的 Ready
};

}  // namespace openkv::raftstore
