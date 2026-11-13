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

// @file batch_system.hpp
// 职责：Raft / Apply **双 worker 池**；有活 Region 经 ScheduleQueue 唤醒，mailbox drain-until-empty。
// 架构：region_id % N 映射到固定 worker，保证同 Region 串行；跨 Region 并行。
// 协作：Router 投递成功后 notify → schedule；Store 注册 RaftHandler / ApplyHandler。
// 注意：单 Region 热点时多 worker 帮不上——吞吐受该 Region 的 persist/apply 流水线限制。

#pragma once

#include "openkv/raftstore/router.hpp"
#include "openkv/raftstore/schedule_queue.hpp"

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

namespace openkv::raftstore {

/// 双池调度器：Raft 池跑协议/Ready，Apply 池跑状态机；同 Region 固定 worker 串行。
class BatchSystem {
 public:
  struct Options {
    size_t raft_workers{8};
    size_t apply_workers{8};
    size_t batch_size{256};
    int poll_timeout_ms{5};
    /// Apply / Snap 队列深度限流（Phase 3 QoS）；0=不限。
    size_t max_apply_queue_depth{8192};
    size_t max_snap_queue_depth{64};
  };

  using RaftHandler = std::function<void(uint32_t region_id, std::vector<PeerMsg>)>;
  using ApplyHandler = std::function<void(uint32_t region_id, std::vector<ApplyControl>)>;

  BatchSystem(std::shared_ptr<Router> router, Options opts);
  ~BatchSystem();

  void set_raft_handler(RaftHandler h) { raft_handler_ = std::move(h); }
  void set_apply_handler(ApplyHandler h) { apply_handler_ = std::move(h); }

  void start();
  void stop();
  bool running() const { return running_.load(std::memory_order_relaxed); }

  /// Router 投递成功后调用：把 region 交给对应 worker（去重）。
  void schedule_raft(uint32_t region_id);
  void schedule_apply(uint32_t region_id);

  bool apply_overloaded(uint32_t region_id) const;
  bool snap_overloaded() const;
  void note_snap_enqueued() { ++snap_inflight_; }
  void note_snap_done() {
    if (snap_inflight_ > 0) --snap_inflight_;
  }

 private:
  void raft_worker_loop(size_t worker_id);
  void apply_worker_loop(size_t worker_id);

  std::shared_ptr<Router> router_;
  Options opts_;
  RaftHandler raft_handler_;
  ApplyHandler apply_handler_;
  std::atomic<bool> running_{false};
  std::vector<std::thread> raft_threads_;
  std::vector<std::thread> apply_threads_;
  std::vector<ScheduleQueue> raft_sched_;
  std::vector<ScheduleQueue> apply_sched_;
  std::atomic<uint64_t> snap_inflight_{0};
};

}  // namespace openkv::raftstore
