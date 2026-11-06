# OpsKV 组件与写路径对照

本仓库角色、Storage 内部模块与 Phase 状态。  
实现细节见 [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md)；整体拓扑见 [ARCHITECTURE.md](ARCHITECTURE.md)。

**结论（一句话）**：Region + Multi-Raft + 单 Store RocksDB + Scheduler / RaftKV / Store 邮箱；Storage 写路径固定 raftstore Ready；Meta 调度深度与 Coprocessor 仍是简化版。

---

## 1. 逻辑拓扑

```
Go Client / skv
        │ gRPC KV (:19000)
        ▼
Compute Gateway（无状态）
        │ Storage gRPC（Store / Raft / Admin）
        ▼
StorageNode ×N
        ▲
        │ HEARTBEAT / PLACE / SPLIT…
        ▼
Meta Raft ×3（路由 + TSO/GC + Scaler/Scheduler）
```

| 维度 | 本仓库 |
|------|--------|
| 存算边界 | **强制** Gateway → Storage；Gateway 无本地数据 |
| 控制面 | Meta 一体：路由 + TSO/GC + Scaler/Scheduler |
| 数据面协议 | 全链路强制 gRPC（无行协议双栈） |
| Raft 库 | 自研 `RaftNode` / `RawNode`（无 FFI） |

---

## 2. 角色与组件

### 2.1 集群角色

| 角色 | 本仓库 | 说明 |
|------|--------|------|
| 客户端 | Go Client + `skv` | 无 SQL；路由/重试在 Gateway |
| 元数据 | **Meta**（Raft×3） | 一体式控制面 |
| 数据节点 | **StorageNode** | 单进程组装 Scheduler / RaftKV / Store / 引擎 |
| KV 引擎 | `ShardEngineManager` → `data_dir/kv` | 单 Store 一份 DB；Region 靠 `KeyRange` |
| Raft 日志 | `RaftEngine` + `RaftLogBatch` | Storage=`RocksRaftEngine`；Meta/UT=`FileRaftEngine` |

### 2.2 Storage 内部

| 模块 | 本仓库 | 说明 |
|------|--------|------|
| 调度 latch | `StorageNode` + `storage/scheduler/` | Latch 简化版 |
| 写入口 | `storage/raftkv.hpp` | propose / 读；经 Store Ready 外提 |
| Peer / Apply | `raftstore::Peer` + `Store` 双 mailbox | 双 FSM 形 |
| 批处理 | `raftstore::BatchSystem` | 跨 Region Apply 展平 + `write_batch` |
| Ready | `RawNode` + `external_io` | Storage **仅** raftstore Ready 外提 |
| 快照 | `SnapWorker` | 同 Region 合并 / 失败重试 / Stats |
| 元数据 | `raftstore::StoreMeta` | epoch / range / leader |
| 休眠 | `hibernate_after_ms`（默认 5000） | 冷 tick |
| 服务端扫描 | `ADMIN_COP` | key/value 前缀、keys/count_only |

### 2.3 Meta 能力

| 能力 | Meta | 状态 |
|------|------|------|
| Region / Store 拓扑 | `ClusterState` + 路由表 | 有 |
| TSO / GC 协调 | Raft 持久化路径 | 有 |
| 心跳驱动调度 | `HEARTBEAT` + 负载字段 | 有 |
| 迁移 / Replace | `RegionScheduler` 扫描入队 | 有 |
| 主动 balance | `SKV_PD_AUTO_BALANCE=1` | 可选；默认关 |
| 热点 / 跨 DC Operator | — | **无 / 弱** |

---

## 3. 写路径（生产）

```
Client → gRPC Gateway → Storage gRPC → StorageNode
  → Scheduler(latch) → RaftKV → RaftNode(external_io)
  → Store Ready → persist_fn / transport_fn / Apply mailbox → 单 Store RocksDB
```

入站 Raft：`StorageNode::handle_raft_*` → `Store::apply_inbound_raft`（禁止直调协议状态机）。

---

## 4. Phase 状态

| Phase | 状态 | 要点 |
|-------|------|------|
| S0 单 Store RocksDB | 完成 | `data_dir/kv`；`migrate_legacy_shards_if_needed` |
| S1 Ready 写路径 | 完成 | Storage 固定 Ready 外提 |
| S2 Router + Hibernate | 完成 | `apply_inbound_raft`；Hibernate 默开；Snap→SnapWorker |
| S3 Scheduler + RaftKV + WB | 完成 | latch + RaftKV；Apply 合并点 |
| S4 gRPC | 完成 | 全链路强制 gRPC |
| S5 调度 + Coprocessor | 完成 | 主动扫描迁移；`ADMIN_COP` |

门禁参考：`opskv_unit_tests` + `scripts/it-scale-live.sh`。

---

## 5. 配置开关

| 变量 / 开关 | 默认 | 含义 |
|-------------|------|------|
| `SKV_PD_AUTO_BALANCE` | off | `1` = Meta 主动 balance |
| `hibernate_after_ms` | `5000` | Follower 空闲进 Hibernate；`0` 禁用 |
| gRPC | 强制 | 主端口 gRPC |

---

## 6. 相关文档

| 文档 | 内容 |
|------|------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 存算分离与本地拓扑 |
| [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md) | Raftstore / Ready / Phase |
| [META_PRODUCTION.md](../production/META_PRODUCTION.md) | Meta 能力表 |
| [STORE_PRODUCTION.md](../production/STORE_PRODUCTION.md) | Store 生产要求 |
| [RANGE_SHARDING.md](RANGE_SHARDING.md) | Region Range 模型 |
| [docs/NETWORKING.md](../NETWORKING.md) | gRPC / Seastar（公开） |
| [SEASTAR_NET.md](SEASTAR_NET.md) | 可选 Seastar |
