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

// @file meta_fsm.hpp
// Meta Raft 状态机命令：编解码 + 应用到 ClusterState。
// 所有改变 ClusterState 的操作必须编码为 MetaCommand，经 Leader propose，
// 多数派提交后在各副本 on_apply 中按相同顺序执行 apply_meta_command。

#pragma once

#include "opskv/meta/cluster_state.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::meta {

/// Raft log payload 操作码（数值稳定，勿改已用值以免破坏持久化日志）。
enum class MetaOp : uint8_t {
  kRegister = 1,           ///< 注册/刷新节点
  kHeartbeat = 2,          ///< Storage 心跳 + Leader 上报
  kSetLearner = 3,         ///< Region 加入 Learner（扩容）
  kPromote = 4,            ///< Learner 提升为 Voter
  kBumpRoute = 5,          ///< 仅递增路由版本
  kSplitRegion = 6,        ///< 分裂 Region（编码仅带 split_key；peer_ids 走默认继承左副本）
  kMergeRegions = 7,       ///< 合并相邻 Region
  kRemoveReplica = 8,      ///< 移除副本
  kPlaceRegion = 9,        ///< 原子替换 Region 副本集
  kAllocTs = 10,           ///< TSO 批分配
  kAdvanceSafePoint = 11,  ///< 推进 GC SafePoint
  kReportMinStartTs = 12,  ///< 上报活跃最小 start_ts
  kSweepStale = 13,        ///< H2：按 TTL 将失联 Storage 标 Offline
  kSetNodeState = 14,      ///< S3：DRAIN / ACTIVATE / OFFLINE
  kUpsertMigrate = 15,     ///< S2：持久化迁移任务
  kEraseMigrate = 16,      ///< S2：删除迁移任务记录
  kEraseTombstone = 17,    ///< 删除 Tombstone Store 记录（对齐 remove-tombstone）
};

/// 一条 Meta 状态机命令（编码进 Raft log data）。
/// 各字段按 op 选择性使用；未用字段保持默认即可。
struct MetaCommand {
  MetaOp op{MetaOp::kRegister};  ///< 操作类型

  NodeInfo node;  ///< kRegister：完整节点信息；kHeartbeat：仅用 node_id；
                  ///< kReportMinStartTs：可用 node.node_id 作 reporter

  std::vector<uint32_t> leader_shards;  ///< kHeartbeat：本节点担任 Leader 的 Region 列表

  uint32_t shard_id{0};  ///< RegionId（历史名 shard_id）：
                         ///< kSetLearner/kPromote/kRemoveReplica/kPlaceRegion/kSplitRegion；
                         ///< kMergeRegions 时为 left RegionId

  Replica learner;  ///< kSetLearner：待加入的 Learner；kPromote：用 learner.node_id

  std::string split_key;  ///< kSplitRegion：分裂点（落入左 Region 的右边界）

  uint32_t merge_right_id{0};  ///< kMergeRegions：右 RegionId

  std::vector<Replica> place_replicas;  ///< kPlaceRegion：最终副本集

  uint32_t ts_count{1};  ///< kAllocTs：分配个数；
                         ///< kAdvanceSafePoint：retain（0=用 ClusterState 默认）；
                         ///< kSweepStale：ttl_ms

  uint64_t ts_value{0};  ///< kReportMinStartTs：min_start_ts；
                         ///< kSetNodeState：NodeState 整型值

  PersistMigrate migrate;  ///< kUpsertMigrate：完整任务；kEraseMigrate：仅用 task_id
};

/// 将命令编码为二进制（Raft LogEntry.data）。
std::string encode_meta_command(const MetaCommand& cmd);

/// 解码；格式错误返回 false，*out 未定义。
bool decode_meta_command(std::string_view data, MetaCommand* out);

/// 将已提交命令应用到 ClusterState（所有副本相同顺序执行）。
/// 注意：alloc_ts / bump_route 等本身非幂等，依赖 Raft 不重复 apply 同一 index。
void apply_meta_command(ClusterState& state, const MetaCommand& cmd);

}  // namespace opskv::meta
