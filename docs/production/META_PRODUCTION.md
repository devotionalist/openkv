# 生产级 Meta（管控面）要求与路线图

本文定义 OpenKV **Meta** 达到生产级的能力要求。  
Store 见 [STORE_PRODUCTION.md](STORE_PRODUCTION.md)；Raft 运行时见 [MULTI_RAFT_PRODUCTION.md](MULTI_RAFT_PRODUCTION.md)。

---

## 1. Meta 管什么

| 能力域 | OpenKV Meta 现状 |
|--------|------------------|
| 共识底座 | **单 Raft 组 + `ClusterState` 快照**（已持久化） |
| Store 存活 | **REGISTER + HEARTBEAT + sweep offline** |
| 路由表 | `GET_ROUTE`；Gateway **长轮询 / Watch** |
| 调度 | **Scheduler + 任务快照恢复** |
| TSO | `GET_TS`（批分配） |
| 扩缩容 | **`SCALE_OUT` + ConfChange + catch-up**；Drain / Offline / Activate |
| 安全 | Admin ACL；TLS 走 sidecar |
| 可观测 | `METRICS` / `STATS` |

当前 Meta 已是可故障演练的管控面（3 副本 Raft、路由/TSO/GC、split/merge/place、快照重加入）。后续重点：**调度闭环深化、客户端契约、TSO/安全扩展**。

---

## 2. 目标画像

| 维度 | 生产目标 |
|------|----------|
| 真相 | Meta 路由/成员与 Storage 实际托管、Raft 角色一致；无「幽灵节点」入调度 |
| 可用 | 单 Meta 挂：数据面靠缓存可短时读；换主后 TSO/路由秒级恢复（已有演练基线） |
| 调度 | 扩容 / 缩容 / 替换 / Evacuate 可编排、可恢复、可观测 |
| 契约 | 稳定客户端（redirect/backoff）；Watch 或等价低延迟路由推送 |
| 安全运维 | TLS、Admin ACL、Prometheus、备份/重加入 Runbook |

---

## 3. 能力总表

### 3.1 存活与路由真相（P0）

| ID | 要求 | 状态 |
|----|------|------|
| **H1** | Storage **REGISTER + 周期 HEARTBEAT**（含本机 Leader Region）写入 Meta Raft | **已实现** |
| **H2** | 心跳超时 → `Offline`，**禁止**再被 `suggest_placement` 选中 | **已实现**（`kSweepStale`） |
| **H3** | 路由表 Leader 标记与 Storage 上报对齐（宣称时摘其它 `*L`，bump `route_ver`） | **已实现** |
| **H4** | Meta Raft 持久化 + ClusterState 快照重加入 | **已实现**（FAULT_DRILL 重加入） |

### 3.2 客户端契约与 API（P1）

| ID | 要求 | 状态 |
|----|------|------|
| **A1** | 统一 Meta Client：多地址、redirect、退避；GW / Storage / skv 共用 | **已增强** |
| **A2** | **WatchRoutingTable**（按 `route_ver` 推送）或长轮询 | **已实现**（`WATCH_ROUTE` + Gateway 长轮询） |
| **A3** | Follower 安全读路由 / SafePoint（有界陈旧）或本地缓存 | **已实现** |
| **A4** | 单一协议真相：全链路 gRPC；无主端口行协议双栈 | **已实现**（见 docs/NETWORKING.md） |

### 3.3 调度与成员变更（P1）

| ID | 要求 | 状态 |
|----|------|------|
| **S1** | **Scale-out 端到端**：`SCALE_OUT` → ConfChange + **catch-up 门闩** → promote | **已实现** |
| **S2** | RegionScheduler：按 draining/欠副本扫描；任务 Raft/快照可恢复 | **已实现** |
| **S3** | **Drain / Decommission** API：`DRAIN_NODE` / `OFFLINE_NODE` / `ACTIVATE_NODE` | **已实现** |
| **S4** | 新 Storage 接入：Register → Joining 池 → `JOIN_NODE` → Active | **已实现** |

### 3.4 TSO / GC（P1）

| ID | 要求 | 状态 |
|----|------|------|
| **T1** | TSO 批分配、时钟单调校验；多 Gateway `min_start` 聚合 | **已增强** |
| **T2** | GC SafePoint：Storage 侧也上报活跃 `start_ts` | **已实现** |

### 3.5 安全与运维（P2）

| ID | 要求 | 状态 |
|----|------|------|
| **O1** | TLS + Admin ACL（与 Store O1 对齐） | **已实现**（ACL）；TLS 走 sidecar |
| **O2** | Prometheus：TSO QPS、route_ver、调度队列、Meta Raft lag | **已实现**（`METRICS`） |
| **O3** | Meta 数据目录备份 / 重加入 / bootstrap 对齐 Runbook | **部分**（FAULT_DRILL） |

---

## 4. 与 Store / Raft 文档的边界

| 层 | 负责 | 不负责 |
|----|------|--------|
| **Meta** | 全局拓扑、TSO、GC 水位、调度决策、路由版本 | 单 Region 日志复制、引擎落盘 |
| **Store** | Region 数据、Admin 入 Raft、epoch、Watch 日志 | 全局谁该持有哪些 Region 的「权威调度」 |
| **Multi-Raft** | 单组/多组协议与运行时 | 跨 Region 放置策略 |

Split/Merge：**Meta 改路由表 + Storage 改数据与成员** 必须同事务语义上可恢复（今日路径可用，生产要求失败可重试、无半成品路由）。

---

## 5. 分阶段建议

### Meta Phase M1（P0）

- [x] H1：Storage `REGISTER` + 周期 `HEARTBEAT`（`SKV_STORE_HB_SEC`，默认 2s）
- [x] H2：`kSweepStale`（`SKV_META_STORE_TTL_MS`，默认 10s）→ Offline；调度排除
- [x] H3：心跳刷新 `*L` 并 bump `route_ver`
- [x] 验收：`ClusterStateTest.SweepStaleExcludesPlacement` / `HeartbeatUpdatesLeader`

### Meta Phase M2（调度闭环）

- [x] S3：`DRAIN_NODE` / `OFFLINE_NODE` / `ACTIVATE_NODE`（Raft `kSetNodeState`）
- [x] S1：`SCALE_OUT` / catch-up 门闩 + Storage ConfChange
- [x] S2：迁移任务 Raft/快照可恢复；扫描 draining/offline/欠副本
- [x] 验收：`ScalerTest.CatchUpGateWaits`；`RestorePersistedTasks`；`MigratePersistRoundTrip`

### Meta Phase M3（契约与扩展）

- [x] A2：`WATCH_ROUTE since [timeout_ms]`；Gateway 长轮询
- [x] A3：`GET_ROUTE` / `GET_SAFE_POINT` Follower 可读
- [x] A1：Go MetaRPC 指数退避；Storage/Go 共用 redirect 模式
- [x] T1/T2：TSO 批上限 + Storage/GW 分源 `min_start`
- [x] O2：Meta `METRICS` Prometheus 文本
- [x] O1：Admin ACL（`SKV_ADMIN_TOKEN` + `AUTH`）；TLS 由 sidecar 终止
- [x] 验收：`MetaHaTest` WATCH/METRICS；`MinStartAggregatesReporters`；`AdminAclTest.*`

**暂缓：** 多租户 catalog、跨机房 Meta 联邦、独立 TSO 服务进程。

---

## 6. 现状可依赖（不要推倒重来）

- 3× Meta Raft、`persist_dir`、ClusterState `MSK1` 快照与重加入  
- `SPLIT_REGION` / `MERGE` / `PLACE` / `SUGGEST_PLACE` + Storage 编排  
- Gateway：`GET_ROUTE` / `GET_TS` / `REPORT_MIN_START_TS` + not_leader redirect  
- 故障演练 F5–F7、F12；单测 `meta_ha` / `cluster_state` / `region_scheduler` / `scaler`

---

## 7. 变更记录

| 日期 | 内容 |
|------|------|
| 2026-10-21 | 初版：Meta 生产级要求与分阶段 |
| 2026-10-21 | **M1–M3 / O1**：心跳、调度、WATCH、ACL、METRICS |
| 2026-10-26 | **S4 / A4**：Joining 接入；全链路 gRPC 文档闭环 |
| 2026-11-11 | 只保留 OpenKV 能力与状态（去掉外部对照） |
