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
// 职责：本机多 Raft Group 门面（按 shard/region id）；委托 Store，并持有出站 gRPC。
// 架构：薄封装——生命周期、tick、Ready 攒批、Hibernate 均在 Store / BatchSystem。
// 协作：StorageNode 持有 MultiRaft；GroupFactory 构造各 shard 的 RaftNode。
// 注意：新增调度参数请改 Store::Options，并在 multi_raft.cpp 映射。

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/net/grpc_transport.hpp"
#include "openkv/region.hpp"

namespace openkv::raftstore {
class Store;
}

namespace openkv::raft {

/* 管理本节点上托管的全部 Raft Group（按 shard_id） */
class MultiRaft {
 public:
  /* 工厂：给定 shard_id 构造新的 RaftNode（含 apply/send 闭包）。 */
  using GroupFactory = std::function<std::shared_ptr<RaftNode>(uint32_t shard_id)>;

  /* 调度与攒批参数；会映射到 Store::Options。 */
  struct Options {
    uint32_t cold_tick_every{5};        // 兼容旧冷热：每 N 次 tick 才扫冷分片
    uint32_t hot_idle_ticks{20};        // 兼容旧热闲阈值；Hibernate 以 hibernate_after_ms 为准
    uint32_t hibernate_after_ms{5000};  // Follower 空闲进入 Hibernate；仍会低频 cold tick 保选举
    uint32_t unload_after_ms{0};        // Hibernate 后再空闲该毫秒可卸协议状态；0=禁用
    size_t raft_workers{8};             // BatchSystem Raft worker 数
    size_t apply_workers{8};            // BatchSystem Apply worker 数
    uint32_t ready_coalesce_n{32};      // propose 后 Ready 攒批条数阈值（≤1 关闭）
    uint32_t ready_coalesce_ms{1};      // propose 后 Ready 攒批最长等待 ms（0=仅按条数）
    size_t persist_workers{2};          // 异步 Raft persist IO 线程（0=raft worker 同步落盘）
  };

  /**
   * 用默认 Options 构造；不自动 start。
   * @param factory 按 shard 创建 RaftNode 的工厂
   */
  explicit MultiRaft(GroupFactory factory);
  /**
   * 带自定义 Options 构造；不自动 start。
   * @param factory 按 shard 创建 RaftNode 的工厂
   * @param opts 调度 / 攒批 / persist 参数
   */
  MultiRaft(GroupFactory factory, Options opts);
  ~MultiRaft();

  /**
   * 查已存在的 Group；未创建返回 nullptr。
   * @param shard_id 分片 / Region id
   * @return 裸指针；生命周期仍由 Store 管
   */
  RaftNode* get(uint32_t shard_id);
  /**
   * 查已存在的 Group 的 shared_ptr；未创建返回空。
   * @param shard_id 分片 / Region id
   * @return 持有引用的 shared_ptr，或空
   */
  std::shared_ptr<RaftNode> get_shared(uint32_t shard_id);
  /**
   * 取已有 Group，没有则经工厂创建并挂进 Store。
   * @param shard_id 分片 / Region id
   * @return 可写引用（保证存在）
   */
  RaftNode& get_or_create(uint32_t shard_id);
  /**
   * 从本机表里删掉该 Group（不保证对端也销毁）。
   * @param shard_id 分片 / Region id
   * @return 原先是否存在
   */
  bool erase(uint32_t shard_id);
  /**
   * 取出 shared_ptr 并从本机表移除（调用方接管生命周期）。
   * @param shard_id 分片 / Region id
   * @return 取出的节点；不存在则空
   */
  std::shared_ptr<RaftNode> take(uint32_t shard_id);
  /**
   * 标记分片活跃，推迟 Hibernate / 冷卸载。
   * @param shard_id 分片 / Region id
   */
  void mark_hot(uint32_t shard_id);
  /**
   * 经 Router 投递 Tick（由 BatchSystem Raft worker 处理）。
   */
  void tick_all();
  /**
   * 本机当前自认是 Leader 的 shard 列表。
   * @return shard_id 集合
   */
  std::vector<uint32_t> leader_shards() const;
  /**
   * 本机已加载的全部 shard。
   * @return shard_id 集合
   */
  std::vector<uint32_t> all_shards() const;

  /**
   * 构造时传入的 Options 只读视图。
   * @return Options 引用
   */
  const Options& options() const { return opts_; }
  /**
   * 底层 Store（调度 / Peer / Ready）。
   * @return Store 引用
   */
  raftstore::Store& store() { return *store_; }
  /**
   * 底层 Store 只读视图。
   * @return Store 常量引用
   */
  const raftstore::Store& store() const { return *store_; }
  /**
   * 出站 gRPC（构造时已接到 Store 的 transport_fn）。
   * @return 传输对象引用
   */
  GrpcRaftTransport& transport() { return transport_; }
  /**
   * 出站 gRPC 只读视图。
   * @return 传输对象常量引用
   */
  const GrpcRaftTransport& transport() const { return transport_; }

  /**
   * 启动：先出站传输，再 Store（BatchSystem / Snap / coalesce）。
   */
  void start();
  /**
   * 停止：先 Store，再出站传输，避免半停状态发 RPC。
   */
  void stop();
  /**
   * 同步 Region range/epoch 到 StoreMeta / Peer。
   * @param region_id Region id
   * @param range key 范围
   * @param epoch 路由纪元；变了说明拆合过
   */
  void update_peer_meta(uint32_t region_id, openkv::KeyRange range, uint64_t epoch);

 private:
  Options opts_;                             // 构造时冻结的调度参数
  GrpcRaftTransport transport_;              // 出站 gRPC
  std::unique_ptr<raftstore::Store> store_;  // 真正跑 Multi-Raft 的中枢
};

}  // namespace openkv::raft
