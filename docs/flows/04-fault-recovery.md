# 流程：故障恢复

**设计总览：** [DATA_HA.md](../architecture/DATA_HA.md) §3.2 · 演练：[FAULT_DRILL.md](../ops/FAULT_DRILL.md)。

**场景：** Storage 宕机再启；或落后过久超出本地日志窗口。

**关键代码：** `RaftPersist` · `RaftNode::load_from_disk` / `recover_apply` · `RAFT_AE` · `RAFT_SNAP` · `KvEngine::{create,apply}_snapshot`

```mermaid
flowchart TD
  A[进程崩溃 / kill -9] --> B[重启 storage_node]
  B --> C[StorageNode::start]
  C --> D[每 shard 构造 RaftNode<br/>persist_dir=raft_shard_N]
  D --> E[load_from_disk<br/>hard.state + log.entries]
  E --> F[恢复 term / voted_for<br/>commit / last_applied / log]
  F --> G[recover_apply<br/>补齐 last_applied → commit]
  G --> H[KvEngine 打开既有 RocksDB]
  H --> I[tick_loop 选举 / 跟随]

  I --> J{本机落后于 Leader?}
  J -->|日志窗口内| K[Leader 发 RAFT_AE<br/>AppendEntries 追赶]
  K --> L[on_append_entries<br/>写日志 → commit → apply]
  L --> M[与 Leader 对齐]

  J -->|next_index &lt; first_index<br/>已 compact| N[Leader InstallSnapshot]
  N --> O[snap_provider<br/>KvEngine::create_snapshot]
  O --> P[RAFT_SNAP term last_idx last_term hex]
  P --> Q[Follower on_install_snapshot]
  Q --> R[KvEngine::apply_snapshot<br/>清空后全量写入]
  R --> S[log.compact_to<br/>commit=applied=last_idx]
  S --> M
```

## 启动装载细节

```mermaid
flowchart LR
  HS[hard.state<br/>term voted commit<br/>applied first snap_term] --> LN[RaftNode 内存状态]
  LE[log.entries] --> LN
  LN --> RA[recover_apply<br/>仅 apply 缺口]
  RA --> RD[RocksDB 已有数据<br/>put 幂等可重放]
```

## 何时走 Snapshot

| 条件 | 动作 |
|------|------|
| Leader `log.size() > compact_threshold(128)` 且已 apply | `maybe_compact` 丢弃前缀日志 |
| Follower `next_index < log.first_index()` | 无法 AE 对齐 → `InstallSnapshot` |
| AE 失败 | Leader 递减 `next_index`，可能最终落入快照窗口 |

## 验证

- 单测：`RaftPersistTest.SaveAndRecoverApplied` / `InstallSnapshotCompactsAndApplies`
- 冒烟：杀 `storage-1` 再启，日志出现 `recovered term=`
