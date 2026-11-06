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

// @file cluster_state.hpp
// Meta 集群内存状态机：节点、Range 路由表（Region）、Learner / 分裂 / 合并、
// TSO、GC SafePoint、可持久化迁移任务。所有写路径须经 Raft apply 后调用，
// 以保证多 Meta 副本强一致。

#pragma once

#include "opskv/region.hpp"

#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace opskv::meta {

/// 节点角色：Compute（网关，不持 Region 数据）/ Storage（存数据 + Multi-Raft）。
enum class NodeRole { kCompute, kStorage };

/// 节点生命周期：
/// - kJoining：已注册，尚未完全参与调度
/// - kActive：Up，可被 suggest_placement / 调度选中
/// - kDraining：手动 Offline 腾空中，不再接收新放置
/// - kOffline：心跳超时 Down，调度排除并补副本
/// - kTombstone：Region/Leader 已为 0，可安全 prune
enum class NodeState { kJoining, kActive, kDraining, kOffline, kTombstone };

/// 集群中一个 Meta/Storage/Compute 节点的描述。
struct NodeInfo {
  std::string node_id;                 ///< 全局唯一 ID（如 storage-0）
  NodeRole role{NodeRole::kStorage};   ///< 角色：计算或存储
  std::string address;                 ///< 可拨号地址 host:port（TCP 线协议）
  NodeState state{NodeState::kJoining};///< 当前生命周期状态
  int64_t last_heartbeat_ms{0};        ///< 最近一次心跳墙钟毫秒（用于 H2 sweep）
};

/// Region 上的一个 Raft 副本（voter 或 learner）。
struct Replica {
  std::string node_id;   ///< 所在 Storage 节点 ID
  std::string address;   ///< 该节点地址（路由下发给 Gateway/客户端）
  bool is_leader{false}; ///< 是否为该 Region 当前 Leader（由心跳刷新）
  bool is_learner{false};///< true=Learner（只复制不投票）；false=Voter
};

/// 一个有序 key 区间 + 副本集，对应一组 Multi-Raft。
struct RegionInfo {
  opskv::RegionId region_id{0}; ///< Region 编号（全局递增分配）
  opskv::KeyRange range;        ///< [start, end) 有序区间；空 start/end 表示 ±∞
  std::vector<Replica> replicas;    ///< 副本列表（含 Leader / Learner 标记）
  uint64_t epoch{1};  ///< split/merge/place 后递增；Txn/Admin 用于 stale epoch 校验
};

/// Gateway / 客户端可见的路由快照。
struct RoutingTable {
  uint64_t version{0};  ///< 路由版本；WATCH_ROUTE / 缓存失效依赖此值单调递增
  /// 兼容字段：等于 regions.size()（历史名 shard_count，Hash 时代遗留）。
  uint32_t shard_count{0};
  std::vector<RegionInfo> regions; ///< 全量 Region 列表（无序亦可，locate 按 range）
};

/// S2：可随 ClusterState 快照 / Raft 恢复的迁移任务摘要。
struct PersistMigrate {
  std::string task_id;                  ///< 任务 ID（与 RegionScheduler 一致）
  uint32_t region_id{0};                ///< 目标 Region
  int state{0};                         ///< MigrateState 整型快照
  std::vector<std::string> desired_peers;///< 期望最终副本 node_id 集合
};

/// Meta 权威内存状态。线程安全（内部 mu_）；快照经 serialize/load_snapshot。
class ClusterState {
 public:
  /// Bootstrap：创建 1 个覆盖全 key 空间的 Region，按 RF 放置副本。
  /// @param replica_factor 目标副本数（RF）
  /// @param storage_nodes 初始 Storage 节点（取前 RF 个作为副本）
  void bootstrap_range(uint32_t replica_factor, const std::vector<NodeInfo>& storage_nodes);

  /// 兼容旧调用：忽略 shard_count，委托 bootstrap_range。
  void bootstrap(uint32_t /*shard_count*/, uint32_t replica_factor,
                 const std::vector<NodeInfo>& storage_nodes) {
    bootstrap_range(replica_factor, storage_nodes);
  }

  /// 注册或刷新节点信息；返回节点 ID。Joining→可心跳。
  std::string register_node(NodeInfo node);

  /// 刷新心跳时间，并按 leader_regions 更新各 Region 的 is_leader 标记（H3）。
  /// Joining/Offline 节点心跳成功会升为 Active；Leader 变更时 bump route_ver。
  /// @return false 若 node_id 未知
  bool heartbeat(const std::string& node_id, const std::vector<uint32_t>& leader_regions);

  /// 仅刷新租约（last_heartbeat / Joining→Active），不改路由 Leader 标记。
  /// 供 Meta Leader 在无 *L 变更时本地 touch，避免每拍 HEARTBEAT 都打 Raft。
  /// @return false 若 node_id 未知
  bool touch_lease(const std::string& node_id);

  /// 若按 leader_regions 应用心跳会导致任一 Region 的 is_leader 变化则 true。
  bool heartbeat_changes_leaders(const std::string& node_id,
                                 const std::vector<uint32_t>& leader_regions) const;

  /// H2：列出已超时未心跳的 Storage id（不修改状态；由 Leader 再发 kSetNodeState）。
  std::vector<std::string> list_stale_storage(int64_t ttl_ms) const;

  /// H2：超过 ttl_ms 未心跳的 Storage → Offline，并清除其 Leader 标记。
  /// @return 新下线的节点数量
  size_t sweep_stale(int64_t ttl_ms);

  /// S3：显式设置节点状态（Drain / Activate / Offline / Tombstone）。
  /// Drain/Offline/Tombstone 时清该节点 Leader 标记并 bump route_ver。
  /// @return false 若节点不存在
  bool set_node_state(const std::string& node_id, NodeState state);

  /// 统计某 Storage 上仍挂着的 Region / Leader 副本数（路由表视角）。
  size_t region_count_on(const std::string& node_id) const;
  size_t leader_count_on(const std::string& node_id) const;

  /// Draining 且 region_count=0、leader_count=0 的节点（只读；调用方经 Raft 写 Tombstone）。
  std::vector<std::string> list_empty_draining() const;

  /// 仅 Tombstone 节点可擦除（对齐 remove-tombstone）；返回 false 若非 tomb / 不存在。
  bool erase_tombstone(const std::string& node_id);

  /// 为迁出 avoid 上的副本构造 RF 目标集：保留非 avoid 的现有 voter，再从轻载 Active 补齐。
  std::vector<std::string> suggest_replace_away(uint32_t region_id,
                                                 const std::vector<std::string>& avoid) const;

  /// Balance：若某 Active Store 过载，返回 (region_id, desired_peers)；否则 nullopt。
  std::optional<std::pair<uint32_t, std::vector<std::string>>> suggest_balance_move() const;

  /// S2：upsert 迁移任务到 migrates_（写入快照可恢复）。
  void upsert_migrate(PersistMigrate task);
  /// S2：删除已完成/取消的迁移任务记录。
  void erase_migrate(const std::string& task_id);
  /// S2：列出全部持久化迁移任务。
  std::vector<PersistMigrate> list_migrates() const;

  /// 返回当前路由表副本（含 version / regions）。
  RoutingTable routing_table() const;
  /// 列出节点；role 有值则按角色过滤。
  std::vector<NodeInfo> list_nodes(std::optional<NodeRole> role = std::nullopt) const;

  /// 将 learner 加入指定 Region 的副本集（扩容第一步）。
  /// 若同 node_id 已存在则跳过（不更新地址/标记），返回 true。
  bool set_learner(uint32_t region_id, const Replica& learner);
  /// Learner → Voter：清除 is_learner。
  bool promote_learner(uint32_t region_id, const std::string& node_id);
  /// 从 Region 副本集移除指定节点。
  bool remove_replica(uint32_t region_id, const std::string& node_id);
  /// 原子替换 Region 副本集（PLACE 编排完成后发布路由），并递增 epoch。
  bool place_replicas(uint32_t region_id, std::vector<Replica> replicas);
  /// 仅递增 route_.version（扩容完成后通知 Gateway 刷新）。
  void bump_route_version();

  /// 在 split_key 处分裂 region；成功返回新（右）RegionId，失败返回 nullopt。
  /// 右 Region 默认继承左副本集；若 peer_ids 非空则使用指定副本（须已在 nodes_）。
  std::optional<opskv::RegionId> split_region(opskv::RegionId region_id,
                                                  std::string split_key,
                                                  const std::vector<std::string>& peer_ids = {});

  /// 合并相邻 Region：要求 left.end == right.start；成功后删除 right，扩展 left.end。
  bool merge_regions(opskv::RegionId left_id, opskv::RegionId right_id);

  /// 按 ID 查节点 / Region；不存在返回 nullopt。
  std::optional<NodeInfo> get_node(const std::string& id) const;
  std::optional<RegionInfo> find_region(opskv::RegionId id) const;
  /// 按 key 落点查找 RegionId（二分/线性扫描 range）。
  std::optional<opskv::RegionId> locate(std::string_view key) const;

  /// 选 RF 个负载最低的 Active Storage；avoid 中的节点降权（分裂打散用）。
  std::vector<std::string> suggest_placement(
      const std::vector<std::string>& avoid = {}) const;

  /// 若存在右邻接 Region（left.end == right.start）返回其 id。
  /// left.end 为空（+∞）时无右邻，返回 nullopt。
  std::optional<opskv::RegionId> adjacent_right(opskv::RegionId left_id) const;

  /// 集群配置的目标副本因子 RF。
  uint32_t replica_factor() const;

  /// TSO：分配 count 个单调时间戳，返回区间起始（含）。须经 Raft apply。
  uint64_t alloc_ts(uint32_t count);
  /// 当前下一个待分配时间戳（不含）；只读。
  uint64_t next_ts() const;

  /// GC safe point：无事务应以 start_ts < safe_point 做快照读；只前进不后退。
  uint64_t gc_safe_ts() const;
  /// T2：按 reporter 上报活跃最小 start_ts（取各 reporter 最小值；MAX=该源无活跃）。
  /// @param reporter 空则用默认槽；Gateway/Storage 分源上报
  void report_min_start_ts(uint64_t min_start_ts, const std::string& reporter = {});
  /// 各 reporter 聚合后的全局最小活跃 start_ts（推进 GC 的上界约束）。
  uint64_t min_start_ts_hint() const;
  /// 按 next_ts - retain 与 min_start_ts_hint 计算并推进；返回新水位。
  uint64_t advance_gc_safe_ts(uint64_t retain_ts);
  /// 设置默认 retain（advance 时 ts_count=0 用此值）。
  void set_gc_retain_ts(uint64_t retain);
  uint64_t gc_retain_ts() const;

  /// Raft InstallSnapshot：导出全量状态（节点/路由/TSO/GC/迁移）。
  std::string serialize() const;
  /// 从快照装载；格式不匹配返回 false。
  bool load_snapshot(std::string_view data);

 private:
  /// 可变 Region 指针（调用方已持 mu_）。
  RegionInfo* mutable_region(opskv::RegionId id);

  mutable std::mutex mu_;                                    ///< 保护下列全部字段
  std::unordered_map<std::string, NodeInfo> nodes_;          ///< node_id → 节点
  RoutingTable route_;                                       ///< 权威路由表
  uint32_t replica_factor_{3};                                ///< 目标 RF
  opskv::RegionId next_region_id_{1};                    ///< 下一个可分配 RegionId
  uint64_t next_ts_{1};  ///< Percolator 逻辑时钟，从 1 起；alloc_ts 递增
  uint64_t gc_safe_ts_{0};                                   ///< 已发布的 GC 水位
  uint64_t min_start_ts_hint_{std::numeric_limits<uint64_t>::max()}; ///< 聚合后的活跃下限
  uint64_t gc_retain_ts_{1000};  ///< safe ≈ next_ts - retain（可被 hint 压低）
  std::unordered_map<std::string, PersistMigrate> migrates_; ///< task_id → 持久化迁移
  std::unordered_map<std::string, uint64_t> min_start_reporters_;  ///< T2：reporter → min_start
};

}  // namespace opskv::meta
