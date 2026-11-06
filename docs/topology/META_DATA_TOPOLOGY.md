# Meta 数据拓扑结构

本文描述 **Meta 内存权威状态**（`ClusterState`）的数据层次，以及**各字段的含义与作用**。  
代码入口：`cluster_state.hpp` · `meta_fsm.hpp` · `scaler.hpp` · `region_scheduler.hpp`。

图例：`字段名 — 含义 / 作用`

---

## 1. 数据结构总图（字段 + 含义）

```mermaid
classDiagram
  direction TB

  class ClusterState {
    +mu_ : 互斥锁 — 保护全部状态字段
    +nodes_ : 节点表 — 集群成员注册与调度依据
    +route_ : 路由表 — Gateway 定位 Region / 副本
    +replica_factor_ : RF — 目标副本数 suggest/欠副本判定
    +next_region_id_ : 下一 Region 号 — split 时分配 ID
    +next_ts_ : TSO 时钟 — 下一个待分配逻辑时间戳
    +gc_safe_ts_ : GC 水位 — 低于此 start_ts 的版本可回收
    +min_start_ts_hint_ : 活跃下限 — 推进 GC 的上界约束
    +gc_retain_ts_ : 保留量 — safe≈next_ts-retain
    +migrates_ : 迁移摘要 — 随快照恢复的迁移任务
    +min_start_reporters_ : 分源上报 — Gateway/Storage 各自 min_start
  }

  class NodeInfo {
    +node_id : 唯一 ID — 全局标识节点
    +role : 角色 — Compute 网关 / Storage 存数据
    +address : 地址 — TCP host:port 拨号
    +state : 生命周期 — Joining/Active/Draining/Offline
    +last_heartbeat_ms : 心跳时间 — H2 超时 sweep 依据
  }

  class RoutingTable {
    +version : 路由版本 — WATCH/缓存失效单调递增
    +shard_count : Region 数 — 兼容字段=regions.size
    +regions : Region 列表 — 全量拓扑与副本
  }

  class RegionInfo {
    +region_id : Region 编号 — 定位与 Admin 目标
    +range : Key 区间 — 有序半开 start≤key＜end
    +replicas : 副本集 — 该组 Multi-Raft 成员
    +epoch : 拓扑世代 — split/merge/place 后递增防陈旧
  }

  class KeyRange {
    +start_key : 下界含 — 空串=空间起点ε
    +end_key : 上界不含 — 空串=+∞
  }

  class Replica {
    +node_id : 所在节点 — 引用 NodeInfo
    +address : 冗余地址 — 下发路由免再查 nodes_
    +is_leader : 是否 Leader — 心跳刷新 *L
    +is_learner : 是否 Learner — true只复制不投票
  }

  class PersistMigrate {
    +task_id : 任务 ID — 与调度器一致
    +region_id : 目标 Region — 要迁的分片
    +state : 阶段整型 — MigrateState 快照
    +desired_peers : 期望副本 — 最终 node_id 列表
  }

  ClusterState "1" *-- "0..*" NodeInfo : nodes_
  ClusterState "1" *-- "1" RoutingTable : route_
  ClusterState "1" *-- "0..*" PersistMigrate : migrates_
  RoutingTable "1" *-- "0..*" RegionInfo : regions
  RegionInfo "1" *-- "1" KeyRange : range
  RegionInfo "1" *-- "0..*" Replica : replicas
  Replica ..> NodeInfo : node_id 引用
  PersistMigrate ..> RegionInfo : region_id 引用
```

---

## 2. 包含关系展开（字段 · 含义 · 作用）

```mermaid
flowchart TB
  CS["ClusterState 权威状态<br/>────────────────────────<br/>mu_ — 保护并发读写<br/>replica_factor_ — 目标 RF，调度/suggest 用<br/>next_region_id_ — split 分配新 RegionId<br/>next_ts_ — TSO：发 start_ts/commit_ts<br/>gc_safe_ts_ — GC：可回收的最大水位<br/>min_start_ts_hint_ — 活跃事务下限，压低 GC<br/>gc_retain_ts_ — 默认保留量 retain<br/>min_start_reporters_ — 分源 min_start 聚合"]

  CS --> NODES["nodes_ 成员表"]
  CS --> ROUTE["route_ 路由"]
  CS --> MIG["migrates_ 迁移持久化"]

  NODES --> NI["NodeInfo<br/>────────────────────────<br/>node_id — 全局唯一标识<br/>role — Compute=网关 / Storage=数据面<br/>address — 对端 TCP 拨号<br/>state — Joining 注册中 · Active 可调度<br/>　　　　 Draining 迁出中 · Offline 排除<br/>last_heartbeat_ms — 存活判定；超时→Offline"]

  ROUTE --> RT["RoutingTable<br/>────────────────────────<br/>version — 变更通知 Gateway 刷新缓存<br/>shard_count — 历史兼容，=regions 个数<br/>regions[] — 全量 Region 拓扑"]

  RT --> RI["RegionInfo<br/>────────────────────────<br/>region_id — 分片 ID，Admin/路由定位<br/>range — 负责的 key 区间<br/>replicas[] — 该区间数据在哪些节点<br/>epoch — 拓扑变了客户端须重试"]

  RI --> KR["KeyRange<br/>────────────────────────<br/>start_key — 区间起点含；空=从最小 key<br/>end_key — 区间终点不含；空=直到 +∞"]

  RI --> REP["Replica<br/>────────────────────────<br/>node_id — 副本落在哪个 Storage<br/>address — 客户端直连该副本的地址<br/>is_leader — 写/读 Leader；线协议 *L<br/>is_learner — 扩容中未投票；线协议 *N"]

  MIG --> PM["PersistMigrate<br/>────────────────────────<br/>task_id — 重启后对上调度任务<br/>region_id — 正在迁移哪块区间<br/>state — Pending/Placing/… 进度阶段<br/>desired_peers — 迁完后应有的副本集合"]
```

---

## 3. 字段词典（按结构）

### `ClusterState`

| 字段 | 含义 | 作用 |
|------|------|------|
| `mu_` | 互斥锁 | 多线程安全读写状态 |
| `nodes_` | 节点字典 | REGISTER/心跳/调度选点的权威来源 |
| `route_` | 路由表 | `GET_ROUTE` / `LOCATE` / Gateway 缓存内容 |
| `replica_factor_` | 目标 RF | `suggest_placement`、欠副本扫描 |
| `next_region_id_` | 下一 Region 号 | `split_region` 分配新 ID |
| `next_ts_` | TSO 逻辑时钟 | `alloc_ts` 发放事务时间戳 |
| `gc_safe_ts_` | GC SafePoint | Storage 可删掉更旧的 MVCC 版本 |
| `min_start_ts_hint_` | 全局最小活跃 start_ts | 推进 GC 时不能超过活跃事务 |
| `gc_retain_ts_` | 保留窗口 | `advance_gc_safe_ts` 默认 `next_ts - retain` |
| `migrates_` | 迁移任务摘要 | Meta 重启后 `restore_persisted` |
| `min_start_reporters_` | reporter→min_start | 多源上报取 min，避免单源漏报 |

### `NodeInfo`

| 字段 | 含义 | 作用 |
|------|------|------|
| `node_id` | 节点唯一名 | 心跳、副本、调度全部用它引用 |
| `role` | Compute / Storage | 过滤调度对象；Compute 不持 Region |
| `address` | `host:port` | Meta→Storage RPC、路由下发给客户端 |
| `state` | 生命周期 | Active 可放置；Draining/Offline 排除 |
| `last_heartbeat_ms` | 最近心跳 | H2 `sweep_stale` 判失联 |

### `RoutingTable` / `RegionInfo` / `KeyRange` / `Replica`

| 结构.字段 | 含义 | 作用 |
|-----------|------|------|
| `RoutingTable.version` | 路由世代号 | `WATCH_ROUTE`、Gateway 失效旧缓存 |
| `RoutingTable.shard_count` | Region 个数 | 兼容旧协议字段 |
| `RoutingTable.regions` | Region 数组 | 完整拓扑下发 |
| `RegionInfo.region_id` | 分片 ID | 定位、Admin、迁移目标 |
| `RegionInfo.range` | key 区间 | `locate(key)` 落点 |
| `RegionInfo.replicas` | 副本列表 | 客户端连哪台、谁是 Leader |
| `RegionInfo.epoch` | 拓扑世代 | 陈旧 epoch → 客户端刷新路由重试 |
| `KeyRange.start_key` | 区间下界（含） | 空 = 从空间起点 |
| `KeyRange.end_key` | 区间上界（不含） | 空 = +∞ |
| `Replica.node_id` | 副本所在节点 | 关联 `NodeInfo` |
| `Replica.address` | 副本地址 | 直连 Storage，少一次查表 |
| `Replica.is_leader` | Leader 标记 | 写路径打到 Leader；`*L` |
| `Replica.is_learner` | Learner 标记 | 扩容追赶中；不参与投票；`*N` |

### `PersistMigrate`

| 字段 | 含义 | 作用 |
|------|------|------|
| `task_id` | 任务 ID | 与内存 `MigrateTask` 对齐、去重 |
| `region_id` | 目标分片 | 知道迁哪一块 |
| `state` | 阶段快照 | 重启后从何处恢复 |
| `desired_peers` | 期望副本集 | place/commit 的目标成员 |

---

## 4. `MetaCommand`（Raft 载荷字段 · 含义 · 作用）

```mermaid
flowchart TB
  MC["MetaCommand — 写入 Raft log 的一条变更<br/>────────────────────────────<br/>op — 操作类型，决定 apply 走哪条分支<br/>node — 注册/心跳主体；也可带 reporter id<br/>leader_shards — 心跳上报：我是哪些 Region 的 Leader<br/>shard_id — 目标 RegionId；合并时=左 Region<br/>learner — 要加/提升的副本描述<br/>split_key — 分裂点 key<br/>merge_right_id — 合并的右 RegionId<br/>place_replicas — PLACE 最终副本全集<br/>ts_count — 分配个数 / GC retain / sweep TTL<br/>ts_value — min_start_ts 或 NodeState 整型<br/>migrate — 迁移 upsert 全量 / erase 用 task_id"]
```

| 字段 | 含义 | 作用 |
|------|------|------|
| `op` | MetaOp 枚举 | 状态机分支选择 |
| `node` | 节点信息 | `kRegister` 写入 `nodes_`；心跳只用 id |
| `leader_shards` | Leader Region 列表 | 刷新 `Replica.is_leader`（H3） |
| `shard_id` | RegionId | Learner/Promote/Place/Split/Merge-left |
| `learner` | 副本描述 | 扩容加 Learner / Promote |
| `split_key` | 分裂点 | 左/右区间分界 |
| `merge_right_id` | 右 Region | 与 left 合并 |
| `place_replicas` | 最终副本集 | 原子替换 `RegionInfo.replicas` |
| `ts_count` | 计数/参数 | TSO 批大小；或 retain；或 ttl_ms |
| `ts_value` | 数值载荷 | 上报 min_start；或设置 NodeState |
| `migrate` | 迁移摘要 | 写入/删除 `migrates_` |

---

## 5. 运行时任务（字段 · 含义 · 作用）

```mermaid
classDiagram
  class ScaleTask {
    +task_id : 任务唯一 ID — 查询/日志关联
    +state : 扩容阶段 — Pending→…→Completed/Failed
    +new_node_id : 新 Storage — 要扩进集群的节点
    +shard_ids : Region 列表 — 对这些分片加副本
    +error : 失败原因 — 仅 Failed 有意义
    +progress : 0~1 — 运维进度提示
  }

  class MigrateTask {
    +task_id : 任务唯一 ID — 取消/查询
    +state : 迁移阶段 — Pending→Placing→UpdatingRoute→终态
    +region_id : 目标分片 — 迁哪一块
    +desired_peers : 期望成员 — place/commit 目标
    +error : 失败原因
    +progress : 0~1 进度
  }

  note for ScaleTask "仅内存；不进 ClusterState 快照"
  note for MigrateTask "全量内存；摘要→PersistMigrate 进快照"
```

| 结构 | 字段 | 含义 | 作用 |
|------|------|------|------|
| ScaleTask | `task_id` | 扩容任务 ID | `get_task` / 日志 |
| | `state` | 生命周期阶段 | worker 推进状态机 |
| | `new_node_id` | 新节点 | AddLearner/Promote 目标 |
| | `shard_ids` | 要扩的 Region | 空=全部 Region |
| | `error` | 失败信息 | 运维排障 |
| | `progress` | 进度 | UI/观测 |
| MigrateTask | `task_id` | 迁移任务 ID | cancel / 去重 |
| | `state` | 阶段 | place→更新路由 |
| | `region_id` | 目标 Region | 扫描入队键 |
| | `desired_peers` | 目标副本集 | 数据面+Meta PLACE |
| | `error` / `progress` | 同上 | 同上 |

---

## 6. Key 空间示例（字段语义）

```mermaid
flowchart LR
  R0["RegionInfo<br/>region_id=1 — 分片号<br/>range start=ε end=k1 — 负责前半 key<br/>epoch=2 — 已分裂过<br/>replicas×3 — RF 副本"]
  R0 --> L["Replica<br/>node_id=s0 — 落点<br/>is_leader=true — 写打这里<br/>is_learner=false — 有投票权"]
  R0 --> F["Replica<br/>node_id=s1<br/>is_leader=false — Follower<br/>is_learner=false"]
  R0 --> N["Replica<br/>node_id=s2<br/>is_learner=true — 扩容追赶中<br/>无投票"]
```

---

## 7. 物理拓扑与字段流向

```mermaid
flowchart TB
  GW["Gateway<br/>消费 RoutingTable.version<br/>+ RegionInfo.replicas.address / is_leader<br/>+ next_ts_ 发来的 TSO"]
  M0["Meta Leader<br/>写 ClusterState 全字段"]
  S0["Storage<br/>NodeInfo 注册/心跳<br/>刷新 last_heartbeat_ms<br/>与 replicas.is_leader"]

  GW -->|GET_ROUTE / GET_TS| M0
  S0 -->|HEARTBEAT| M0
  M0 -->|ADMIN_* 按 Replica.address| S0
```

---

## 8. 相关文档

- [DATA_TOPOLOGY_INDEX.md](DATA_TOPOLOGY_INDEX.md) — 各模块拓扑索引  
- [META_PRODUCTION.md](../production/META_PRODUCTION.md)  
- [flows/05-meta-ha.md](../flows/05-meta-ha.md)  
- 同格式：[RAFT](RAFT_DATA_TOPOLOGY.md) · [STORE](STORE_DATA_TOPOLOGY.md) · [COMPUTE](COMPUTE_DATA_TOPOLOGY.md) · [COMMON](COMMON_DATA_TOPOLOGY.md)  

