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
// 职责：raftstore **入口**——Router + BatchSystem + Peer 表 + StoreMeta + SnapWorker。
// 架构：所有入站 Raft RPC / Tick / propose 后的 Ready 都汇聚于此；组 commit（ready_coalesce_*）。
// 协作：MultiRaft 持有 Store；persist_fn→RaftNode::persist_ready；apply_*→KV；transport_fn→gRPC。
// 注意：handle_raft_* 禁止绕过 Store 直调 RaftNode::on_*；入站用 apply_inbound_raft。

#pragma once

#include "openkv/raftstore/batch_system.hpp"
#include "openkv/raftstore/peer.hpp"
#include "openkv/raftstore/router.hpp"
#include "openkv/raftstore/snap_worker.hpp"
#include "openkv/raftstore/store_meta.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openkv::raftstore {

/// 本机 Multi-Raft 运行时中枢：调度、Ready 攒批、入站 RPC、Peer 生命周期。
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
    /// Ready group-commit：攒满 N 条 request_ready 再 flush（≤1 关闭计数）。
    uint32_t ready_coalesce_n{32};
    /// Ready group-commit：最长等待 ms（0=仅按条数；与 n≤1 且 ms=0 时立即 flush）。
    uint32_t ready_coalesce_ms{1};
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
  /// propose 热路径：按 ready_coalesce_n/ms 攒批后再 drive_ready（group commit）。
  void request_ready(uint32_t region_id);

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

  struct CoalesceState {
    uint32_t pending{0};
    bool armed{false};
    std::chrono::steady_clock::time_point deadline{};
  };
  void coalesce_loop();
  std::mutex coalesce_mu_;
  std::condition_variable coalesce_cv_;
  std::unordered_map<uint32_t, CoalesceState> coalesce_;
  std::atomic<bool> coalesce_running_{false};
  std::thread coalesce_thr_;
};

}  // namespace openkv::raftstore
