# OpenKV 流程图（flows）

与当前实现对齐：**C++ `gateway_node` + Meta/Storage gRPC + Range Region + Multi-Raft RF=3**。

上级入口：[docs/en/](../en/README.md) · [中文索引](../zh/README.md) · 网络 [NETWORKING.md](../NETWORKING.md) · 架构 [ARCHITECTURE.md](../architecture/ARCHITECTURE.md)

---

## 怎么读

| 你想… | 从这里开始 |
|-------|------------|
| 一条 Put/Get 怎么走完 | [A. KV API](#a-kv-api客户端可见) → [01](01-write-put.md) / [02](02-read-get.md) |
| 事务 / CAS / TTL / Scan / Watch / DS | [A](#a-kv-api客户端可见) 对应小节 |
| Leader 切换、宕机恢复、Meta HA | [B. 一致性与 HA](#b-一致性--raft--ha) |
| Split / Place / 扩缩容 | [C. Region](#c-region-生命周期与扩缩容) |
| Gateway 内部、异步、批写、TSO | [D. 内部实现](#d-gateway-与写路径内部) |
| Helm / 门禁 / gRPC 强制 | [E. 部署与工程](#e-部署--门禁--工程变更) |

**建议顺序（新人）：** 01 → 02 → 08 → 06 → 07 → 09 → 18 → 22 → 23。

---

## 文档约定

每篇尽量包含：

1. **路径 / 角色** — Client、Gateway、Meta、Storage、Raft 谁做什么  
2. **关键代码** — 可跳进源码的符号或路径  
3. **流程图** — Mermaid（VS Code / GitHub / Cursor 可预览）  
4. **边界与失败** — 错误 token、重试、不做的事  
5. **验收** — 可执行命令；结果记入 [TEST_CASES.md](../test/TEST_CASES.md)

编号文件名保持稳定（外链不改）；正文按「当前行为」写，历史变更只保留必要一句。

---

## 端到端鸟瞰

```text
SDK / skv / Web
      │  gRPC kv.v1 / ds.v1  (:19000)
      ▼
gateway_node（无状态：路由 · TSO 批缓存 · 重试 · Watch 轮询）
      │  gRPC Store / Admin / Meta（类型化）
      ▼
StorageNode × N（Scheduler latch → RaftKV → raftstore Ready → RocksDB）
      ▲
      │  HEARTBEAT / SPLIT / PLACE / GET_ROUTE / GET_TS …
      ▼
Meta Raft × 3（ClusterState · TSO · Scaler / RegionScheduler）
```

短 Put/Delete：**本地 `alloc_ts(2)` → `StoreService.Propose(ONEPC)`（1 次 Propose）**。  
多 key Txn：**Prewrite → Commit**。线性读：**TSO + Leader `TXN_GET`**。

组件对照：[COMPONENT_MAP.md](../architecture/COMPONENT_MAP.md)。

---

## A. KV API（客户端可见）

| 流程 | 文件 | 要点 |
|------|------|------|
| 写入 Put / Delete | [01-write-put.md](01-write-put.md) | Locate → ONEPC → 多数派 → apply → ProposalQueue |
| Raw vs Txn | [27-raw-vs-txn.md](27-raw-vs-txn.md) | RawPut/Get/Delete 无 TSO；与 Put 同 key 勿混用 |
| Compute 网络优化 | [28-compute-net-opt.md](28-compute-net-opt.md) | 二进制 Get、stub 缓存、TSO async、Batch fan-in |
| 读取 Get | [02-read-get.md](02-read-get.md) | 线性一致 vs 副本读；无 ReadIndex |
| not_leader 重定向 | [08-not-leader-redirect.md](08-not-leader-redirect.md) | `rememberLeader` / `tryOrder` |
| Watch | [11-watch.md](11-watch.md) | Gateway 轮询 `WATCH_POLL`；revision=apply index |
| Hash / Set / List / ZSet | [12-data-structures.md](12-data-structures.md) | MetaKey blob RMW；总览 [DATA_STRUCTURES](../architecture/DATA_STRUCTURES.md) |
| 版本 CAS | [13-version-cas.md](13-version-cas.md) | apply 内校验；MVCC commit_ts vs plain revision |
| KV TTL | [14-kv-ttl.md](14-kv-ttl.md) | `expire_at_ms`；读过滤 + TTL Checker |
| Scan / ListPrefix | [16-scan-list-prefix.md](16-scan-list-prefix.md) | 跨 Region `TXN_SCAN`；无旁路索引 |
| 多语言 SDK | [15-multi-client-sdk.md](15-multi-client-sdk.md) | 独立仓 `openkv-client-{go,rust,java,typescript}` |

错误码总表：[ERROR_CODES.md](../architecture/ERROR_CODES.md)。

---

## B. 一致性 / Raft / HA

总览：[DATA_HA.md](../architecture/DATA_HA.md) · 演练：[FAULT_DRILL.md](../ops/FAULT_DRILL.md)

| 流程 | 文件 | 要点 |
|------|------|------|
| Raft 选举 / 心跳 / 提交 | [06-raft-election-commit.md](06-raft-election-commit.md) | `maybe_commit`、选票、AE/SNAP |
| Storage 启动 | [07-storage-startup.md](07-storage-startup.md) | MultiRaft 工厂、WAL、tick、gRPC listen |
| 故障恢复 | [04-fault-recovery.md](04-fault-recovery.md) | load WAL → AE 追赶 / InstallSnapshot |
| 进程优雅关机 | [31-graceful-shutdown.md](31-graceful-shutdown.md) | SIGTERM：拒新 RPC + 排空在途 |
| Meta HA | [05-meta-ha.md](05-meta-ha.md) | Meta×3、管控命令、TSO |

---

## C. Region 生命周期与扩缩容

总览：[DATA_SCALE.md](../architecture/DATA_SCALE.md) · [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md) · 分片 [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md)

| 流程 | 文件 | 要点 |
|------|------|------|
| Split / Merge | [09-range-split-merge.md](09-range-split-merge.md) | PREPARE→SPLIT；同机不搬 KV |
| Place | [10-region-placement.md](10-region-placement.md) | Learner→追赶→Promote→踢旧副本 |
| 扩缩容设计 | [扩容缩容设计.md](扩容缩容设计.md) | join / Balance Replace / Draining |
| Scale-out（加副本） | [03-scale-out.md](03-scale-out.md) | RF↑；默认扩容请看 DATA_SCALE |

SOP：[REGION_OPS.md](../ops/REGION_OPS.md) · [SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md) · [SCALE_LIVE_SOP.md](../ops/SCALE_LIVE_SOP.md)

---

## D. Gateway 与写路径内部

实现细节与线程/批处理模型；**对外 API 见 A**。

| 主题 | 文件 | 要点 |
|------|------|------|
| C++ Gateway | [18-cpp-gateway.md](18-cpp-gateway.md) | `gateway_node` 组件、端口、启动参数 |
| Compute 代码地图 | [29-compute-code-map.md](29-compute-code-map.md) | 路径矩阵、双栈、粗糙点与注释基线 |
| Gateway Watch UAF | [33-gateway-watch-uaf.md](33-gateway-watch-uaf.md) | cancel 后 reactor UAF → exit 139；weak_ptr 修复 |
| Storage compact 风暴 | [34-storage-compact-storm.md](34-storage-compact-storm.md) | apply 前缀 compact + 批 report；metrics 坏 map 不 dtor |
| 统一异步 Store | [30-unified-async-store.md](30-unified-async-store.md) | 去 WorkPool sync；StoreService 扩 Txn/Scan/Watch |
| TSO 热路径 | [16-gateway-tso-perf.md](16-gateway-tso-perf.md) | 无锁区间环；批 5000；剩余&lt;2000 预取 |
| 异步网络 | [21-async-network.md](21-async-network.md) | CallbackService；`propose_async` / `wait_async` |
| 异步线程模型 | [22-async-threading-model.md](22-async-threading-model.md) | 接收线程 vs WorkPool vs apply 回调 |
| 架构硬化 | [19-architecture-hardening.md](19-architecture-hardening.md) | CV 等 apply、latch 持到 apply、approx split、Meta CV |
| 写路径批处理 | [23-write-path-batching.md](23-write-path-batching.md) | Ready group commit、apply WriteBatch、多 Region |
| 耐久：Raft vs KV | [24-persist-raft-vs-kv.md](24-persist-raft-vs-kv.md) | 默认 data=Raft sync / KV 异步 |
| 写路径三刀 | [25-write-path-knives.md](25-write-path-knives.md) | Ready 攒批、二进制 ONEPC、workers 8/8 |
| Raftstore schedule | [26-raftstore-schedule-pipeline.md](26-raftstore-schedule-pipeline.md) | schedule 唤醒 + drain-until-empty |
| 后续优化项 | [20-next-optimizations.md](20-next-optimizations.md) | Meta FSM snap、ONEPC WriteBatch、BatchSystem CV |
| 全接口压测 | [17-gateway-api-bench.md](17-gateway-api-bench.md) | `api-bench-all.sh` / skill `openkv-api-bench`；改完必跑；基线记 TEST_CASES |

生产写路径总图：[COMPONENT_MAP §3](../architecture/COMPONENT_MAP.md) · [MULTI_RAFT_PRODUCTION](../production/MULTI_RAFT_PRODUCTION.md)

---

## E. 部署 / 门禁 / 工程变更

| 主题 | 文件 |
|------|------|
| gRPC 强制（拆除行协议双栈） | [GRPC_MANDATORY.md](GRPC_MANDATORY.md) |
| 数据面硬化（Cop / Snap / 转主） | [DATA_PLANE_HARDENING.md](DATA_PLANE_HARDENING.md) |
| 集成覆盖扩展 | [IT_COVERAGE_EXPANSION.md](IT_COVERAGE_EXPANSION.md) |
| Helm HA | [K8S_HELM.md](K8S_HELM.md) |
| Docker builder 缓存 | [32-docker-builder-cache.md](32-docker-builder-cache.md) | `openkv-builder` + BuildKit build/ccache |
| K8s 加节点 vs 运维页加入 | [K8S_SCALE.md](K8S_SCALE.md) |

公开步骤：[USER_GUIDE.md](../USER_GUIDE.md) §3–§4。

---

## 相关总览

| 文档 | 用途 |
|------|------|
| [ARCHITECTURE.md](../architecture/ARCHITECTURE.md) | 存算分离总览 |
| [COMPONENT_MAP.md](../architecture/COMPONENT_MAP.md) | 组件与写路径 |
| [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md) | Region 有序区间 |
| [NETWORKING.md](../NETWORKING.md) | gRPC / 端口 |
| [FAULT_DRILL.md](../ops/FAULT_DRILL.md) | 故障演练 |
| [INTEGRATION_TEST.md](../test/INTEGRATION_TEST.md) | 集成场景编号 |
| [TEST_CASES.md](../test/TEST_CASES.md) | 用例与执行记录 |
| [BACKLOG.md](../BACKLOG.md) | 待办 |
