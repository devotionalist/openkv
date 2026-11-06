# OpsKV — 存算分离分布式 KV 架构

**文档总目录（分类）：** [README.md](../README.md)

处理流程图：[flows/README.md](../flows/README.md) · 运维 CLI：[SKV_CLI.md](../ops/SKV_CLI.md)  
组件对照：[COMPONENT_MAP.md](COMPONENT_MAP.md) · **高可用**：[DATA_HA.md](DATA_HA.md) · **扩容**：[DATA_SCALE.md](DATA_SCALE.md) · **缩容**：[DATA_SCALE_IN.md](DATA_SCALE_IN.md) · **错误码**：[ERROR_CODES.md](ERROR_CODES.md) · 分片：[RANGE_SHARDING.md](RANGE_SHARDING.md) · [SHARDING_STRATEGIES.md](SHARDING_STRATEGIES.md)  
事务调研：[TRANSACTION_SURVEY.md](TRANSACTION_SURVEY.md) · 待做：[BACKLOG.md](../BACKLOG.md)

## 设计目标

| 目标 | 方案 |
|------|------|
| 网络协议 | **全链路强制 gRPC**（见 [NETWORKING.md](../NETWORKING.md)、[flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md)） |
| 高性能异步 IO | Seastar **可选**（默认 OFF；业务路径未接入，见 [SEASTAR_NET.md](SEASTAR_NET.md)） |
| 持久化存储 | RocksDB（单 Store 一份 KV；Region 靠 KeyRange） |
| 强一致 | Multi-Raft（每 Region 一个 Raft Group，默认 RF=3）；生产要求见 [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md) |
| 对外 API | gRPC → Go Compute Gateway（`KVService` + `DSService`，见 [DATA_STRUCTURES.md](DATA_STRUCTURES.md)） |
| 存算分离 | Gateway 无状态；Storage 只负责复制与落盘 |
| 在线伸缩 | Region 分裂 / 合并 + Raft 成员变更 + 数据迁移 |

## 逻辑架构（当前实现）

本地默认可运行形态：**Go Gateway + C++ Meta/Storage（gRPC）+ RocksDB**；Seastar 默认不编。

```
┌─────────────┐
│  Go Client  │  /  skv (运维)
└──────┬──────┘
       │ gRPC KV + DS API  (:19000)
       ▼
┌──────────────────────────────────────────┐
│     Compute Gateway (Go, 无状态)          │
│  Range 路由缓存 ← Meta GET_ROUTE          │
│  Leader hint + not_leader 重试            │
└───────┬──────────────────────────────────┘
        │ Storage gRPC (Store/Raft/Admin)
        ▼
┌──────────────────────────────────────────┐
│     Storage ×6 (:9100–9105)               │
│  每 Region 一组 Raft (RF=3) + RocksDB     │
│  Region 可 place 到任意 RF 个节点子集      │
└──────────────────────────────────────────┘
        ▲
        │ 拓扑 / Region 表 / SPLIT·MERGE·PLACE（gRPC）
┌───────┴───────┐
│ Meta Raft ×3  │  :8500–8502；多数派强一致
└───────────────┘
```

默认网络为 **gRPC**（强制）：KV / Meta / Admin / 节点间 Raft 同端口多路复用。`OPSKV_USE_SEASTAR=ON` 仅链接库与占位头文件，**尚不**把 Meta/Storage IO 迁到 reactor。详见 [NETWORKING.md](../NETWORKING.md)、[SEASTAR_NET.md](SEASTAR_NET.md)、[flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md)。

## 本地默认拓扑

| 角色 | 地址 |
|------|------|
| meta-0/1/2 | `127.0.0.1:8500`–`8502` |
| storage-0..5 | `127.0.0.1:9100`–`9105`（Region 0 初始在 0/1/2） |
| gateway | `127.0.0.1:19000` |
| **web 管控模块** | `http://127.0.0.1:19100`（独立进程 `web/`，`make admin-run`） |

启停：`./build/tools/skv start [--fresh]` / `stop` / `status`。可视化运维见 [ADMIN_CONSOLE.md](../ops/ADMIN_CONSOLE.md)。

## 分片与一致性

- Key 落在有序 **Region** `[start, end)`（`end` 空 = +∞）；`locate(key)` → `region_id`。
- 开机：**Region 0** 覆盖全 key 空间，副本落在 **前 RF 个** Storage（不必占满全部 Store）。
- 每个 Region 对应一个 **Raft Group**，默认 **副本因子 3**；可用 `skv place` 迁到其它节点子集。
- 多数派 = 2：挂掉 1 个副本所在节点，其余仍可选主并提供读写。
- **写路径：** Client → Gateway → Region Leader（跟 `not_leader redirect=`）→ AppendEntries → 多数派提交 → 各副本 RocksDB Apply。
- **读路径（默认强一致）：** 仅 Leader + TSO/MVCC；`linearizable=false` 可读任意副本本地快照（可能落后）。Watch 见 [flows/11-watch.md](../flows/11-watch.md)。

高可用两面模型、故障域与验收：[DATA_HA.md](DATA_HA.md)。分片细节：[RANGE_SHARDING.md](RANGE_SHARDING.md)、[flows/01-write-put.md](../flows/01-write-put.md)、[flows/09-range-split-merge.md](../flows/09-range-split-merge.md)。

## 存算分离边界

| 层 | 实现 | 职责 | 状态 |
|----|------|------|------|
| Compute | Go `compute-gateway` | 路由、重试、Leader 缓存；（鉴权/限流可扩展） | 无本地数据 |
| Storage | C++ `storage_node` | Multi-Raft、RocksDB、WAL、快照、ADMIN 迁移 | 有状态 |
| Meta | C++ `meta_server` ×3 | 拓扑、Region 表、SPLIT/MERGE/LOCATE | Raft 3 副本 HA |

Compute 重启不影响数据；Storage 按 Raft 冗余；Meta 挂 1 台数据面仍可用。

## 在线扩容 / 分裂

**加节点（默认：RF 不变 Replace）：**

1. 拉起新 Storage → Meta `Joining`（未进调度）
2. 运维 `join` → `Active` → `balance` / `place`（Learner → 追赶 → Voter → 踢旧）
3. 发布 `PLACE_REGION`；**不要**把 Helm 加副本当成扩容完成

详解：[DATA_SCALE.md](DATA_SCALE.md) · [扩容缩容设计.md](../flows/扩容缩容设计.md)。

**Region 分裂 / 合并（切区间，非腾盘）：**

1. 手动 `skv split/merge`，或 `SKV_SPLIT_KEYS` / `SKV_SPLIT_BYTES` 自动分裂（默认关）
2. Meta 更新 Range 表 → `ADMIN_PREPARE_SPLIT` / `ADMIN_SPLIT`（同机 **不搬 KV**）
3. Gateway 拉新 `GET_ROUTE`；跨机打散另走 `place` / `balance`

分裂前 **PREPARE 写暂停**；事务为 Percolator。见 [TRANSACTION_SURVEY.md](TRANSACTION_SURVEY.md)、[flows/09-range-split-merge.md](../flows/09-range-split-merge.md)。

## 目录结构

```
proto/           gRPC 协议
common/          配置、Region、日志等公共类型
raft/            轻量 Multi-Raft
storage/         RocksDB + Storage Node（gRPC；raftstore Ready）
compute/         C++ 路由库占位（生产 Gateway 为 Go）
meta/            集群状态 + Meta Raft + Scaler（gRPC）
client/go/       Go SDK + compute-gateway
web/             独立 Web 管控模块（HTTP 运维台 → gRPC）
tools/skv/       C++ 运维 CLI
docs/            全部文档（架构 / 运维 / 测试 / 流程图）
conf/            本地集群配置
tests/           GoogleTest
deploy/          Docker Compose + Helm chart（k8s HA，见 flows/K8S_HELM.md）
```

## 技术栈

- **C++20**：Meta / Storage / Raft / skv；**gRPC C++ 强制**；RocksDB（FetchContent）；Seastar 可选未接入业务
- **Go 1.22+**：`client/go` SDK、`compute-gateway`（对外 gRPC）
- 自研轻量 Multi-Raft + raftstore Ready 外提
