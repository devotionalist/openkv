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

#include "opskv/raftstore/batch_system.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

namespace opskv::raftstore {

BatchSystem::BatchSystem(std::shared_ptr<Router> router, Options opts)
    : router_(std::move(router)), opts_(opts) {
  if (opts_.raft_workers == 0) opts_.raft_workers = 1;
  if (opts_.apply_workers == 0) opts_.apply_workers = 1;
  if (opts_.batch_size == 0) opts_.batch_size = 64;
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
  for (auto& t : raft_threads_) {
    if (t.joinable()) t.join();
  }
  for (auto& t : apply_threads_) {
    if (t.joinable()) t.join();
  }
  raft_threads_.clear();
  apply_threads_.clear();
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
  while (running_) {
    auto regions = router_->peer_regions();
    if (regions.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.poll_timeout_ms));
      continue;
    }
    // 分片：worker_id 处理 region_id % N == worker_id
    bool did = false;
    for (uint32_t rid : regions) {
      if ((rid % opts_.raft_workers) != worker_id) continue;
      auto mb = router_->peer_mailbox(rid);
      if (!mb) continue;
      auto batch = mb->recv_batch(opts_.batch_size, 0);
      if (batch.empty()) continue;
      did = true;
      if (raft_handler_) raft_handler_(rid, std::move(batch));
    }
    if (!did) {
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.poll_timeout_ms));
    }
  }
}

void BatchSystem::apply_worker_loop(size_t worker_id) {
  while (running_) {
    auto regions = router_->peer_regions();
    if (regions.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.poll_timeout_ms));
      continue;
    }
    bool did = false;
    // 跨 Region 合并：同一 worker 一轮内收集多 region 的 ApplyMsg（WriteBatch 合并点）
    std::vector<std::pair<uint32_t, std::vector<ApplyControl>>> merged;
    for (uint32_t rid : regions) {
      if ((rid % opts_.apply_workers) != worker_id) continue;
      auto mb = router_->apply_mailbox(rid);
      if (!mb) continue;
      auto batch = mb->recv_batch(opts_.batch_size, 0);
      if (batch.empty()) continue;
      did = true;
      merged.emplace_back(rid, std::move(batch));
    }
    if (apply_handler_) {
      for (auto& [rid, batch] : merged) apply_handler_(rid, std::move(batch));
    }
    if (!did) {
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.poll_timeout_ms));
    }
  }
}

}  // namespace opskv::raftstore
