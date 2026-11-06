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

// @file multi_raft.cpp
// MultiRaft：薄封装，生命周期与 tick 交给 raftstore::Store。

#include "opskv/raft/multi_raft.hpp"

#include "opskv/raftstore/store.hpp"

namespace opskv::raft {

MultiRaft::MultiRaft(GroupFactory factory) : MultiRaft(std::move(factory), Options{}) {}

MultiRaft::MultiRaft(GroupFactory factory, Options opts) : opts_(opts) {
  raftstore::Store::Options so;
  so.hibernate_after_ms = opts_.hibernate_after_ms;
  so.unload_after_ms = opts_.unload_after_ms;
  so.batch.raft_workers = opts_.raft_workers == 0 ? 2 : opts_.raft_workers;
  so.batch.apply_workers = opts_.apply_workers == 0 ? 2 : opts_.apply_workers;
  store_ = std::make_unique<raftstore::Store>(so, std::move(factory));
}

MultiRaft::~MultiRaft() { stop(); }

void MultiRaft::start() {
  if (store_) store_->start();
}

void MultiRaft::stop() {
  if (store_) store_->stop();
}

RaftNode* MultiRaft::get(uint32_t shard_id) { return store_->get(shard_id); }

std::shared_ptr<RaftNode> MultiRaft::get_shared(uint32_t shard_id) {
  return store_->get_shared(shard_id);
}

RaftNode& MultiRaft::get_or_create(uint32_t shard_id) {
  return store_->get_or_create(shard_id);
}

bool MultiRaft::erase(uint32_t shard_id) { return store_->erase(shard_id); }

std::shared_ptr<RaftNode> MultiRaft::take(uint32_t shard_id) { return store_->take(shard_id); }

void MultiRaft::mark_hot(uint32_t shard_id) { store_->mark_hot(shard_id); }

void MultiRaft::tick_all() { store_->schedule_tick(); }

std::vector<uint32_t> MultiRaft::leader_shards() const { return store_->leader_shards(); }

std::vector<uint32_t> MultiRaft::all_shards() const { return store_->all_shards(); }

void MultiRaft::update_peer_meta(uint32_t region_id, opskv::KeyRange range, uint64_t epoch) {
  store_->update_peer_meta(region_id, std::move(range), epoch);
}

}  // namespace opskv::raft

