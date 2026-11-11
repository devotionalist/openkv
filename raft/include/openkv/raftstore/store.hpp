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

// @file store.hpp
// Store：Router + BatchSystem + Peer 表 + StoreMeta + SnapWorker（raftstore 入口）。

#pragma once

#include "openkv/raftstore/batch_system.hpp"
#include "openkv/raftstore/peer.hpp"
#include "openkv/raftstore/router.hpp"
#include "openkv/raftstore/snap_worker.hpp"
#include "openkv/raftstore/store_meta.hpp"

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace openkv::raftstore {

class Store {
 public:
  struct Options {
    BatchSystem::Options batch;
    SnapWorker::Options snap;
    /// Follower 空闲超过该毫秒进入 Hibernate（0=禁用）。
    uint32_t hibernate_after_ms{5000};
    /// 冷卸载：Hibernate 后再空闲该毫秒可卸协议状态（0=禁用）。
    uint32_t unload_after_ms{0};
    size_t mailbox_capacity{4096};
  };

  using LegacyFactory = std::function<std::shared_ptr<raft::RaftNode>(uint32_t region_id)>;
  using ApplyFn = std::function<void(uint32_t region_id, uint64_t index, const std::string& data)>;
  /// 同 Region 一批 committed entries（可合并 WriteBatch）；未设置则退回逐条 ApplyFn。
  using ApplyBatchFn =
      std::function<void(uint32_t region_id, const std::vector<raft::LogEntry>& entries)>;
  using PersistFn = std::function<void(uint32_t region_id, const raft::Ready& rd)>;
  using TransportFn =
      std::function<void(uint32_t region_id, const std::string& to, const std::string& type,
                         const std::string& payload)>;
  using SnapProviderFn =
      std::function<std::string(uint32_t region_id, uint64_t* last_idx, uint64_t* last_term)>;
  using ReloadFn = std::function<std::shared_ptr<raft::RaftNode>(uint32_t region_id)>;

  Store(Options opts, LegacyFactory factory);
  ~Store();

  void set_apply_fn(ApplyFn f) { apply_fn_ = std::move(f); }
  void set_apply_batch_fn(ApplyBatchFn f) { apply_batch_fn_ = std::move(f); }
  void set_persist_fn(PersistFn f) { persist_fn_ = std::move(f); }
  void set_transport_fn(TransportFn f) { transport_fn_ = std::move(f); }
  void set_snap_provider(SnapProviderFn f) { snap_provider_ = std::move(f); }
  void set_reload_fn(ReloadFn f) { reload_fn_ = std::move(f); }

  void start();
  void stop();

  /// StoreTicker：向所有 Peer 邮箱投递 Tick（替代 MultiRaft::tick_all）。
  void schedule_tick();
  /// 仅驱动指定 Region 的 Ready（propose 后调用；批内多个请求合并为一次 persist/sync）。
  void drive_ready(uint32_t region_id);

  raft::RaftNode* get(uint32_t region_id);
  std::shared_ptr<raft::RaftNode> get_shared(uint32_t region_id);
  raft::RaftNode& get_or_create(uint32_t region_id);
  std::shared_ptr<raft::RaftNode> take(uint32_t region_id);
  bool erase(uint32_t region_id);
  void mark_hot(uint32_t region_id);

  bool send_raft_message(uint32_t region_id, RaftMessage msg);
  /// 入站 Raft RPC 唯一步进入口（与 Router 邮箱同路径；同步返回行协议响应）。
  /// StorageNode::handle_raft_* 禁止直调 RaftNode::on_*。
  std::string apply_inbound_raft(uint32_t region_id, RaftMessage msg);
  std::vector<uint32_t> leader_shards() const;
  std::vector<uint32_t> all_shards() const;

  StoreMeta& meta() { return meta_; }
  const StoreMeta& meta() const { return meta_; }
  Router& router() { return *router_; }
  BatchSystem& batch_system() { return *batch_; }

  void update_peer_meta(uint32_t region_id, openkv::KeyRange range, uint64_t epoch);
  bool apply_queue_overloaded(uint32_t region_id) const;

 private:
  void on_raft_batch(uint32_t region_id, std::vector<PeerMsg> msgs);
  void on_apply_batch(uint32_t region_id, std::vector<ApplyControl> msgs);
  /// process_ready=false：只 tick，由调用方（如 on_raft_batch）在批末统一 process_ready。
  void handle_tick(Peer& peer, bool process_ready);
  /// 只 step / 唤醒；Ready 一律由 on_raft_batch 批末合并。
  void handle_raft_message(Peer& peer, const RaftMessage& msg);
  /// process_ready：入站同步路径在 step 成功后立刻 Ready；批处理路径传 false。
  std::string step_raft_message(Peer& peer, const RaftMessage& msg, bool process_ready);
  void process_ready_legacy(Peer& peer);
  void maybe_hibernate_or_unload(Peer& peer);
  void wake_peer(Peer& peer);
  std::shared_ptr<Peer> peer_locked(uint32_t region_id);
  raft::RaftNode* peer_node(Peer& peer);

  Options opts_;
  LegacyFactory factory_;
  ApplyFn apply_fn_;
  ApplyBatchFn apply_batch_fn_;
  PersistFn persist_fn_;
  TransportFn transport_fn_;
  SnapProviderFn snap_provider_;
  ReloadFn reload_fn_;

  std::shared_ptr<Router> router_;
  std::unique_ptr<BatchSystem> batch_;
  std::unique_ptr<SnapWorker> snap_;
  StoreMeta meta_;

  mutable std::mutex mu_;
  std::unordered_map<uint32_t, std::shared_ptr<Peer>> peers_;
};

}  // namespace openkv::raftstore
