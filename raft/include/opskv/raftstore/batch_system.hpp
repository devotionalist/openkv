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
// Raft / Apply 双 worker 池；批量 poll mailbox（BatchSystem）。

#pragma once

#include "opskv/raftstore/router.hpp"

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

namespace opskv::raftstore {

class BatchSystem {
 public:
  struct Options {
    size_t raft_workers{2};
    size_t apply_workers{2};
    size_t batch_size{64};
    int poll_timeout_ms{10};
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
  std::atomic<uint64_t> snap_inflight_{0};
};

}  // namespace opskv::raftstore
