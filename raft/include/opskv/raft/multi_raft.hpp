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

// @file multi_raft.hpp
// 本机托管的全部分片 Raft Group；内部委托 raftstore::Store（BatchSystem 双 FSM）。

#pragma once

#include "opskv/raft/raft_node.hpp"
#include "opskv/region.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace opskv::raftstore {
class Store;
}

namespace opskv::raft {

// 管理本节点上托管的全部 Raft Group（按 shard_id）
class MultiRaft {
 public:
  /// 工厂：给定 shard_id 构造新的 RaftNode（含 apply/send 闭包）。
  using GroupFactory = std::function<std::shared_ptr<RaftNode>(uint32_t shard_id)>;

  struct Options {
    /// 兼容旧冷热参数；Hibernate 由 Store.hibernate_after_ms 接管。
    uint32_t cold_tick_every{5};
    uint32_t hot_idle_ticks{20};
    /// Follower 空闲进入 Hibernate；仍会低频 cold tick 保选举。
    uint32_t hibernate_after_ms{5000};
    uint32_t unload_after_ms{0};
    size_t raft_workers{2};
    size_t apply_workers{2};
  };

  explicit MultiRaft(GroupFactory factory);
  MultiRaft(GroupFactory factory, Options opts);
  ~MultiRaft();

  /// 未创建则返回 nullptr。
  RaftNode* get(uint32_t shard_id);
  std::shared_ptr<RaftNode> get_shared(uint32_t shard_id);
  RaftNode& get_or_create(uint32_t shard_id);
  bool erase(uint32_t shard_id);
  std::shared_ptr<RaftNode> take(uint32_t shard_id);
  void mark_hot(uint32_t shard_id);
  /// 经 Router 投递 Tick（BatchSystem Raft worker 处理）。
  void tick_all();
  std::vector<uint32_t> leader_shards() const;
  std::vector<uint32_t> all_shards() const;

  const Options& options() const { return opts_; }
  raftstore::Store& store() { return *store_; }
  const raftstore::Store& store() const { return *store_; }

  void start();
  void stop();
  /// 同步 Region range/epoch 到 StoreMeta / Peer。
  void update_peer_meta(uint32_t region_id, opskv::KeyRange range, uint64_t epoch);

 private:
  Options opts_;
  std::unique_ptr<raftstore::Store> store_;
};

}  // namespace opskv::raft
