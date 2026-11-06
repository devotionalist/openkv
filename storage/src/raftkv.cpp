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

// @file raftkv.cpp

#include "opskv/storage/raftkv.hpp"

#include "opskv/raft/raw_node.hpp"

namespace opskv::storage {

RaftKV::RaftKV(raftstore::Store* store) : store_(store) {}

raft::RaftNode* RaftKV::node(uint32_t region_id) {
  if (!store_) return nullptr;
  return store_->get(region_id);
}

raft::RawNode* RaftKV::raw(uint32_t region_id) {
  (void)region_id;
  // Peer 生产路径以 external_io RaftNode 挂 legacy 槽；RawNode 适配器供单测。
  return nullptr;
}

bool RaftKV::is_leader(uint32_t region_id) const {
  auto* n = store_ ? store_->get(region_id) : nullptr;
  return n && n->is_leader();
}

raft::SoftState RaftKV::soft_state(uint32_t region_id) const {
  auto* n = store_ ? const_cast<raftstore::Store*>(store_)->get(region_id) : nullptr;
  return n ? n->soft_state() : raft::SoftState{};
}

void RaftKV::flush_ready(uint32_t region_id) {
  if (!store_) return;
  // 投递 Tick 让 BatchSystem/同步路径推进 Ready（Store::handle_tick → process_ready）。
  store_->mark_hot(region_id);
  store_->schedule_tick();
}

bool RaftKV::async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                         std::string* error) {
  auto* n = node(region_id);
  if (!n) {
    if (error) *error = "no_region";
    return false;
  }
  if (!n->is_leader()) {
    if (error) *error = "not_leader";
    return false;
  }
  if (!n->propose(std::move(data), assigned_index)) {
    if (error) *error = "propose_failed";
    return false;
  }
  flush_ready(region_id);
  return true;
}

bool RaftKV::sync_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                        std::string* error, uint32_t wait_ms) {
  uint64_t idx = 0;
  if (!async_write(region_id, std::move(data), &idx, error)) return false;
  if (assigned_index) *assigned_index = idx;
  if (apply_wait_ && !apply_wait_(region_id, idx, wait_ms)) {
    if (error) *error = "apply_timeout";
    return false;
  }
  return true;
}

}  // namespace opskv::storage
