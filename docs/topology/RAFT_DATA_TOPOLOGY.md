# Raft 数据拓扑结构

单分片 `RaftNode` + 本机 `MultiRaft` + 持久化 / 传输。  
图例：`字段 — 含义 / 作用`  
代码：`raft/include/opskv/raft/*.hpp`

---

## 1. 总图（字段 + 含义）

```mermaid
classDiagram
  direction TB

  class MultiRaft {
    +factory_ : 工厂 — 按 shard 创建 RaftNode
    +opts_ : 冷热参数 — cold_tick_every / hot_idle_ticks
    +groups_ : map — shard_id→GroupMeta
    +global_tick_ : 全局计数 — 冷组降频 tick
  }

  class GroupMeta {
    +node : RaftNode — 该 Region 的 Raft 组
    +idle_ticks : 空闲计数 — 超阈视为冷
    +force_hot : 强制热 — 读写后必 tick
  }

  class RaftNode {
    +opts_ : Options — 超时/持久化/批复制等
    +role_ : 角色 — Follower/Candidate/Leader/Learner
    +term_ : 任期 — 选举与日志版本
    +voted_for_ : 本任期投票对象
    +leader_id_ : 已知 Leader
    +log_ : RaftLog — 复制日志
    +commit_index_ : 已提交水位
    +last_applied_ : 状态机已 apply
    +next_index_ : 每 peer 下一条待发
    +match_index_ : 每 peer 已匹配
    +persist_ : 可选落盘
  }

  class RaftLog {
    +first_index_ : 首条有效 index — 快照后前移
    +snapshot_term_ : 快照边界 term
    +entries_ : 连续日志 — term/index/data
  }

  class LogEntry {
    +term : 写入时任期 — 冲突检测
    +index : 日志位置 — 提交/apply 依据
    +data : 载荷 — KV Command 或 ConfChange
  }

  class SoftState {
    +role : 可观测角色
    +leader_id : 当前 Leader
    +term / commit_index / last_applied
    +election_count : 选主次数 O1
    +snapshot_send_count : 发快照次数
    +replication_lag : Leader 复制延迟
    +in_joint : 是否 Joint 配置
  }

  MultiRaft "1" *-- "0..*" GroupMeta : groups_
  GroupMeta "1" *-- "1" RaftNode : node
  RaftNode "1" *-- "1" RaftLog : log_
  RaftLog "1" *-- "0..*" LogEntry : entries_
  RaftNode ..> SoftState : soft_state 快照
```

---

## 2. 核心类型字段词典

### `Peer` / `LogEntry` / `ConfChange`

| 结构.字段 | 含义 | 作用 |
|-----------|------|------|
| `Peer.id` | 节点 ID | Raft 成员标识 |
| `Peer.address` | 地址 | GrpcRaftTransport 拨号 |
| `Peer.learner` | 是否 Learner | 不参与投票/quorum |
| `LogEntry.term` | 条目任期 | AppendEntries 匹配/冲突截断 |
| `LogEntry.index` | 条目下标 | 提交与 apply 顺序 |
| `LogEntry.data` | 业务/配置载荷 | 状态机解释 |
| `ConfChange.type` | 变更类型 | Add/Remove/Learner/Joint |
| `ConfChange.peer` | 目标成员 | 成员变更对象 |
| `ConfChange.joint_change` | Joint 内实际变更 | EnterJoint 携带 |

### `RaftLog`

| 字段 | 含义 | 作用 |
|------|------|------|
| `first_index_` | 首条有效 index | compact/快照后丢弃更早日志 |
| `snapshot_term_` | 快照处 term | match(prev) 边界校验 |
| `entries_` | 内存连续日志 | 复制与本地 propose |

### `RaftNode`（关键状态）

| 字段 | 含义 | 作用 |
|------|------|------|
| `role_` | 当前角色 | 决定能否 propose / 选举 |
| `term_` | 当前任期 | 拒绝过期 RPC |
| `voted_for_` | 本任期已投票 | 选举安全 |
| `leader_id_` | 已知 Leader | redirect / SoftState |
| `commit_index_` | 多数派已提交 | 可读/可 apply 上限 |
| `last_applied_` | 已应用到状态机 | Facade 可见水位 |
| `next_index_` | peer→下一发送 index | 复制游标 |
| `match_index_` | peer→已匹配 index | 算 commit / catch-up |
| `inflight_` | peer 未确认消息数 | 流控 max_inflight |
| `in_joint_` / `joint_*_voters_` | Joint 配置 | 双多数派提交 |
| `lease_expire_` | Leader lease 截止 | 本地线性读捷径 |
| `election_count_` 等 | O1 计数 | SoftState / 指标 |

### `RaftNode::Options`

| 字段 | 含义 | 作用 |
|------|------|------|
| `shard_id` | 分片/Region | MultiRaft 键 |
| `self_id` / `peers` | 本机与同伴 | 组网 |
| `election_timeout_ms` / `heartbeat_ms` | 超时 | 选举与心跳 |
| `persist_dir` | 落盘目录 | 宕机恢复 |
| `compact_threshold` | 日志条数阈值 | 触发快照压缩 |
| `enable_pre_vote` | PreVote | 防打断任期 |
| `check_quorum` | 多数派存活检查 | Leader 让位 |
| `max_ae_entries` / `max_ae_bytes` | AE 批大小 | 复制批处理 |
| `leader_lease_ms` | lease 时长 | 线性读优化 |
| `persist_sync` / `persist_batch_n` | 同步级别/批写 | 耐久 vs 吞吐 |

### `PersistedHardState` / `MultiRaft::Options`

| 字段 | 含义 | 作用 |
|------|------|------|
| `term` / `voted_for` | hard state | 重启恢复选举态 |
| `commit_index` / `last_applied` | 提交与 apply | 恢复后补 apply |
| `first_index` / `snapshot_term` | 日志边界 | 重建 RaftLog |
| `cold_tick_every` | 冷组 tick 间隔 | 降 CPU |
| `hot_idle_ticks` | 变冷阈值 | 冷热调度 |

---

## 3. 传输与帧

```mermaid
flowchart TB
  TT["GrpcRaftTransport<br/>────────────────────────<br/>peers_ — peer 拨号表<br/>nodes_ — shard→RaftNode 回调<br/>queue_ — 优先级出站 Job<br/>Job.priority — Vote＞AE＞Snap<br/>RaftService stub — gRPC 出站"]
  TT --> JOB["Job<br/>shard / self_id / peer_id<br/>type / payload / priority / seq"]
```

| 结构.字段 | 含义 | 作用 |
|-----------|------|------|
| `Job.type` | Vote / AE / Snap | 选 stub RPC |
| `Job.shard` | Region/shard | 交给对应 RaftNode |
| `out_raft_` / `out_sync_` | 出站计数 | STATS |

---

## 4. 包含关系示意

```mermaid
flowchart TB
  MR["MultiRaft 本机多组"] --> GM["GroupMeta × N"]
  GM --> RN["RaftNode"]
  RN --> RL["RaftLog + LogEntry[]"]
  RN --> HS["HardState 字段<br/>term / voted_for / commit"]
  RN --> PI["peer 进度<br/>next_index / match_index"]
  RN -.-> RP["RaftPersist 目录<br/>hard.state + log.entries"]
```

---

## 相关

- [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md)  
- [DATA_TOPOLOGY_INDEX.md](DATA_TOPOLOGY_INDEX.md)  
