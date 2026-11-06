# 生产级 Multi-Raft 要求与路线图

本文定义 OpsKV 达到 **生产级 Multi-Raft** 的能力要求、验收标准与分阶段实现。  
对标：工业界 Multi-Raft 运行时（如 CockroachDB），而非仅 HashiCorp 单组 Raft 库。

相关实现：`raft/` · 故障演练 [FAULT_DRILL.md](../ops/FAULT_DRILL.md) · 选举流程 [flows/06-raft-election-commit.md](../flows/06-raft-election-commit.md)

---

## 1. 目标画像

| 维度 | 生产目标 |
|------|----------|
| 规模 | 单 Store **10⁴～10⁵** Region；跨机房 RF=3/5 |
| 延迟 | 同城 P99 写由盘/网决定，Raft 层不显著放大 |
| 隔离 | 单组慢复制 / 选主不拖垮同进程其它组 |
| 正确 | 不丢已提交、不双主、成员变更安全 |
| 可运维 | 指标 / 限流 / 迁移 / 混沌门禁 |

---

## 2. 能力要求总表

### 2.1 正确性与协议（P0）

| ID | 要求 | 说明 | 状态 |
|----|------|------|------|
| C1 | **PreVote** | 选举前试探，避免分区节点抬 term 打断 Leader | **已实现** |
| C2 | **CheckQuorum** | Leader 联系不到多数派则让位 | **已实现** |
| C3 | ReadIndex / Lease Read | 线性读不经全量 propose | **已实现**（`read_index` + lease） |
| C4 | Joint Consensus | 两阶段成员变更 | **已实现** |
| C5 | 流式 Snapshot | 分片传输，不堵心跳 | **已实现**（`RAFT_SNAPC`） |
| C6 | 崩溃一致 WAL | 日志/hard state 可靠落盘 | **增强**（可配 fsync / group persist） |
| C7 | Leadership transfer | 目标追平后转主 | **已实现**（追平门闩 + abdicate + CAMPAIGN） |

### 2.2 Multi-Raft 运行时（P0/P1）

| ID | 要求 | 状态 |
|----|------|------|
| M1 | 冷热 tick | **已实现** |
| M2 | 消息优先级 | **已实现** |
| M3 | 长连接复用 | **已实现** |
| M4 | Propose / AE 批处理 | **已实现** |
| M5 | 异步 Apply + 背压 | **已实现** |
| M6 | 每 peer 流控 | **已实现** |
| M7 | Region 调度器 | **已实现** |

### 2.3 极致性能（P2）

| ID | 要求 | 状态 |
|----|------|------|
| P1 | 二进制帧（去行协议 + hex） | **已废止**（Raft 出站仅 gRPC protobuf；无 SKV1/行帧） |
| P2 | Group commit / 可配 fsync | **已实现**（`SyncMode` + `persist_batch_n`） |
| P3 | Lease read、Follower 安全读水位 | **已实现**（Leader lease；Follower 仍 redirect） |
| P4 | 冷 Region 内存卸载 | 部分（冷热 tick 降频；不解载状态机） |
| P5 | QoS：过载拒绝、慢盘隔离 | 部分（apply 背压 + 出站队列丢弃计数） |

### 2.4 可观测与门禁（P1）

| ID | 要求 | 状态 |
|----|------|------|
| O1 | 每 Region：角色/term/复制延迟/选主/snap | **已实现**（`SoftState` + `ADMIN_REGION_STATS`） |
| O2 | Store 级队列深度、出站丢弃 | **已实现**（STATS `qdepth`/`qdrop`） |
| O3 | fault-drill + 万 Region 启停 | fault-drill 有；[`scripts/bench_empty_regions.sh`](../../scripts/bench_empty_regions.sh) 烟雾 |

---

## 3. 分阶段路线

### Phase A / B

已完成（见变更记录）。

### Phase C — 极致性能与可观测

- [x] C3 / P3 ReadIndex + Leader Lease Read  
- [x] P1 `SKV1` 二进制帧（与行协议双栈）  
- [x] P2 SyncMode + group persist  
- [x] O1 Region SoftState / ADMIN_REGION_STATS  
- [x] O2 传输队列深度与丢弃  
- [x] O3 空 Region 批量脚本  
- [x] P4 冷 Region 内存卸载（`Peer::mark_unloaded` + `Store.unload_after_ms`）
- [x] P5 Apply/Snap 队列深度 QoS（`BatchSystem::max_*_queue_depth`）

**验收**：`opskv_unit_tests`（含 `RaftReadIndex*` / `RaftFrame*` / `RawNode*` / `Raftstore*`）全绿。

---

## 3.1 Storage 全栈模块（C + 单 Store RocksDB）

组件对照：[COMPONENT_MAP.md](../architecture/COMPONENT_MAP.md)。

| 模块 | 本仓库 |
|------|--------|
| RawNode / Ready | `RawNode` + `Ready`（`external_io`） |
| Peer / Apply | `raftstore::Peer` + `Store` 双 mailbox |
| BatchSystem | `raftstore::BatchSystem` |
| 单 Store RocksDB | `ShardEngineManager` → **`data_dir/kv` 一份引擎**；Region 靠 `KeyRange` |
| Scheduler + latch | [`storage/scheduler.hpp`](../../storage/include/opskv/storage/scheduler.hpp) |
| RaftKV | [`storage/raftkv.hpp`](../../storage/include/opskv/storage/raftkv.hpp) |
| Hibernate | 默认 `hibernate_after_ms=5000` + 低频 cold tick |
| Snap | `SnapWorker`（合并 / 重试 / Stats） |
| StoreMeta | `raftstore::StoreMeta` |
| WriteBatch | `KvEngine::write_batch` + Apply 合并点 |
| gRPC | `proto/` 强制；主端口 Store/Raft/Admin |
| Meta 调度 | `RegionScheduler::maybe_enqueue_from_scan`；`SKV_PD_AUTO_BALANCE=1` 主动 balance |
| Coprocessor | [`storage/coprocessor.hpp`](../../storage/include/opskv/storage/coprocessor.hpp) + `ADMIN_COP` |

`StorageNode`：组装 Store / RaftKV / Scheduler / 单 Store 引擎；写经 Scheduler→RaftKV→propose；入站 Raft 经 `Store::apply_inbound_raft`。

### Phase 状态（C + 单 Store RocksDB）

| Phase | 状态 | 要点 |
|-------|------|------|
| S0 单 Store RocksDB | 完成 | `data_dir/kv`；`migrate_legacy_shards_if_needed` |
| S1 Ready 写路径 | 完成 | Storage 固定 Ready 外提（已移除双轨） |
| S2 Router + Hibernate | 完成 | 入站 `apply_inbound_raft`；默认 `hibernate_after_ms=5000`；Snap→SnapWorker |
| S3 Scheduler + RaftKV + WB | 完成 | latch + RaftKV；Apply 展平合并点 + `KvEngine::write_batch` |
| S4 gRPC | 完成 | `GrpcRaftTransport` + C++ gRPC Server；IT 默认 gRPC |
| S5 PD + Coprocessor | 完成 | 主动扫描迁移；Coprocessor UT；本文档 |

---

## 4. 配置开关（增量）

| 字段 | 默认 | 含义 |
|------|------|------|
| `leader_lease_ms` | `500` | Leader lease；0=禁用，线性读总走 ReadIndex |
| `enable_lease_read` | `true` | 允许 lease 本地确认 |
| `persist_sync` | `kData` | `kNone` / `kData` / `kAll` |
| `persist_batch_n` | `1` | 累积 N 次 dirty 再落盘；tick 强制 flush |
| （原 `TcpTransport::set_binary_rpc`） | — | 已删除；出站仅 `GrpcRaftTransport` |
| ~~`SKV_RAWNODE_STORE`~~ | — | **已移除**；Storage 仅 raftstore Ready |
| `SKV_PD_AUTO_BALANCE` | unset/off | `1` 开启 PD 式 auto-balance |

---

## 5. 协议补充（Phase C）

### ReadIndex / Lease

- `RaftNode::read_index(&commit)`：lease 有效则立即返回；否则广播心跳并等多数派 ACK。  
- `StoreFacade::get(..., linearizable=true)`：Leader 上先 `read_index`，再等 `last_applied >= commit`。

### 二进制帧 `SKV1`

```
magic[4]='SKV1' | type(u8) | shard(u32 LE) | len(u32 LE) | payload
```

payload 暂为原行协议正文；响应同帧封装。服务端自动识别 magic。

### 可观测

- `ADMIN_REGION_STATS <rid>` → role/term/commit/applied/elections/snaps/lag/joint  
- `STATS` → 追加 `qdepth=` `qdrop=`

---

## 6. 与「换库」的关系

生产级 Multi-Raft 的护城河在 **运行时调度、批处理、存储/网络一体**。协议层同构 raft-rs `RawNode` API，**不**引入 Rust FFI；运行时 raftstore（BatchSystem / 双 FSM / Hibernate / SnapWorker）。

---

## 7. 变更记录

| 日期 | 内容 |
|------|------|
| 2026-10-21 | Phase A：PreVote、CheckQuorum、冷热 tick、优先级、SNAPC、长连接 |
| 2026-10-21 | Phase B：M4/M5/M6、C4 Joint、M7 RegionScheduler |
| 2026-10-21 | Phase C：ReadIndex/Lease、SKV1 帧、fsync/group persist、O1/O2/O3 |
| 2026-10-23 | RawNode/Ready、raftstore BatchSystem、Hibernate/SnapWorker/StoreMeta、卸载与 QoS |
