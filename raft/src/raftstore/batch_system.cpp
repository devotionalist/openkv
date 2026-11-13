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

// @file batch_system.cpp
// 实现：raft/apply 线程循环——pop ScheduleQueue，drain mailbox，调 Store handler。

#include "openkv/raftstore/batch_system.hpp"

namespace openkv::raftstore {

BatchSystem::BatchSystem(std::shared_ptr<Router> router, Options opts)
    : router_(std::move(router)), opts_(opts) {
  if (opts_.raft_workers == 0) opts_.raft_workers = 1;
  if (opts_.apply_workers == 0) opts_.apply_workers = 1;
  if (opts_.batch_size == 0) opts_.batch_size = 256;
  raft_sched_ = std::vector<ScheduleQueue>(opts_.raft_workers);
  apply_sched_ = std::vector<ScheduleQueue>(opts_.apply_workers);
}

BatchSystem::~BatchSystem() { stop(); }

void BatchSystem::start() {
  if (running_.exchange(true)) return;
  raft_threads_.reserve(opts_.raft_workers);
  apply_threads_.reserve(opts_.apply_workers);
  for (size_t i = 0; i < opts_.raft_workers; ++i) {
    raft_threads_.emplace_back([this, i] { raft_worker_loop(i); });
  }
  for (size_t i = 0; i < opts_.apply_workers; ++i) {
    apply_threads_.emplace_back([this, i] { apply_worker_loop(i); });
  }
}

void BatchSystem::stop() {
  if (!running_.exchange(false)) return;
  for (auto& s : raft_sched_) s.wake();
  for (auto& s : apply_sched_) s.wake();
  for (auto& t : raft_threads_) {
    if (t.joinable()) t.join();
  }
  for (auto& t : apply_threads_) {
    if (t.joinable()) t.join();
  }
  raft_threads_.clear();
  apply_threads_.clear();
}

void BatchSystem::schedule_raft(uint32_t region_id) {
  if (raft_sched_.empty()) return;
  raft_sched_[region_id % raft_sched_.size()].push(region_id);
}

void BatchSystem::schedule_apply(uint32_t region_id) {
  if (apply_sched_.empty()) return;
  apply_sched_[region_id % apply_sched_.size()].push(region_id);
}

bool BatchSystem::apply_overloaded(uint32_t region_id) const {
  if (opts_.max_apply_queue_depth == 0) return false;
  return router_->apply_queue_depth(region_id) >= opts_.max_apply_queue_depth;
}

bool BatchSystem::snap_overloaded() const {
  if (opts_.max_snap_queue_depth == 0) return false;
  return snap_inflight_.load() >= opts_.max_snap_queue_depth;
}

void BatchSystem::raft_worker_loop(size_t worker_id) {
  // region_id % raft_workers == worker_id 的 Peer 串行跑在这里 → Store::on_raft_batch。
  auto& sched = raft_sched_[worker_id];
  while (running_.load(std::memory_order_acquire)) {
    auto rid_opt = sched.pop(opts_.poll_timeout_ms, running_);
    if (!rid_opt) continue;
    const uint32_t rid = *rid_opt;
    auto mb = router_->peer_mailbox(rid);
    if (!mb || !raft_handler_) continue;
    // drain-until-empty：同 Region 连续多批再让出，减少调度抖动。
    for (;;) {
      auto batch = mb->recv_batch(opts_.batch_size, 0);
      if (batch.empty()) break;
      raft_handler_(rid, std::move(batch));
    }
  }
}

void BatchSystem::apply_worker_loop(size_t worker_id) {
  // ApplyMsg / SnapTask → Store::on_apply_batch → KV WriteBatch / SnapWorker。
  auto& sched = apply_sched_[worker_id];
  while (running_.load(std::memory_order_acquire)) {
    auto rid_opt = sched.pop(opts_.poll_timeout_ms, running_);
    if (!rid_opt) continue;
    const uint32_t rid = *rid_opt;
    auto mb = router_->apply_mailbox(rid);
    if (!mb || !apply_handler_) continue;
    for (;;) {
      auto batch = mb->recv_batch(opts_.batch_size, 0);
      if (batch.empty()) break;
      apply_handler_(rid, std::move(batch));
    }
  }
}

}  // namespace openkv::raftstore
