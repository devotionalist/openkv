# OpenKV 待做计划（Backlog）

依据：`COMPONENT_MAP` / `META_PRODUCTION` / `STORE_PRODUCTION` / `MULTI_RAFT_PRODUCTION` 与近期缺口。  
文档总目录（默认英文）：[English](./en/README.md) · [中文](./zh/README.md)。更新：2026-11-12。

近期已交付（勿重复）：ProposalQueue · ONEPC · Region auto-split · recover_local_regions · 架构硬化见 [flows/19-architecture-hardening.md](flows/19-architecture-hardening.md)。

---

## 0. 建议优先级

| 优先级 | 项 | 理由 |
|--------|----|------|
| P1 | §2 Meta 调度加深（热点 / balance / 备份） | 控制面 |
| P2 | §4 跨 Region 2PC | 能力加深 |
| 暂缓 | §6 联邦/多租户等 | 无硬依赖 |

---

## 1. 文档 / 工程卫生

~~D1–D3~~ → 见 §7：公开 [docs/NETWORKING.md](NETWORKING.md)；README / ARCHITECTURE / META A4 已对齐 gRPC 强制。

---

## 2. 控制面 / Meta 调度

| ID | 内容 | 状态 / 出处 |
|----|------|-------------|
| ~~M1~~ | ~~A4~~ | → §7 已闭环 |
| M3 | PD 级调度加深：热点、跨 DC、完整 Operator | `COMPONENT_MAP` Meta 弱/无 |
| M4 | 主动 balance 产品化策略 | `SKV_PD_AUTO_BALANCE` 默认关（避与 SCALE_OUT 打架） |
| M5 | Meta 备份 / bootstrap Runbook 补全 | O3 **部分**（见 `FAULT_DRILL`） |
| — | 多租户 catalog、跨机房 PD 联邦、独立 TSO 进程 | **暂缓**（`META_PRODUCTION` M3） |

~~M2 S4 Store 接入~~ → 见 §7。

---

## 3. 数据面 / Raftstore / Store

~~R1–R5~~ → 见 §7 与 [flows/DATA_PLANE_HARDENING.md](flows/DATA_PLANE_HARDENING.md)。

后续可选（非本轮）：完整 Coprocessor DAG、Snap 跨节点断点续传协议增强、Scheduler 进程级拆库。

---

## 4. 事务

MVCC 事务 **P1–P3 已落地**（TSO、乐观/悲观、CheckTxnStatus/Resolve、epoch）。见 [flows/13-version-cas.md](flows/13-version-cas.md)。

| ID | 内容 | 说明 |
|----|------|------|
| T1 | **跨 Region 完整 2PC** | `STORE_PRODUCTION` §6 **暂缓**；恢复时按序： |
| | | 1. Primary 锁带 `secondaries[]` |
| | | 2. `CheckTxnStatus` 回传 + 崩溃 Resolve |
| | | 3. 锁 TTL + `LOCK_HEARTBEAT` |
| | | 4. Gateway 按 Region 分组 prewrite / async resolve |
| | | 5. 跨 Region 单测 + 杀进程演练 |

---

## 5. 运维 / 安全 / 测试

| ID | 内容 | 说明 |
|----|------|------|
| O1 | 进程内 TLS | 现推荐 stunnel/Envoy sidecar |
| O2 | `make release-gate` 常态绿 | unit → e2e → it-web/region/txn → fault-drill（含 F2） |
| O3 | `integ-extra` / `fault-drill-extra` 周跑 | disk-fault、partition 破坏性更强 |

详见 [flows/IT_COVERAGE_EXPANSION.md](flows/IT_COVERAGE_EXPANSION.md)、[TEST_CASES.md](test/TEST_CASES.md)。

---

## 6. 曾提起未做

（暂无登记项。历史讨论过的改名 / 联邦等已取消或并入上文「暂缓」。）

---

## 7. 已完成（勿重复开坑）

便于对照，避免把已交付当待办：

- 全链路强制 gRPC（拆除行协议双栈）
- Meta M1–M3：心跳/失联 offline、SCALE_OUT catch-up、WATCH_ROUTE、分源 min_start、METRICS、Admin ACL
- **M2 / Meta S4**：`REGISTER` → Joining 资源池 → `JOIN_NODE`/`ACTIVATE` → Active；Helm 扩容 + 运维页加入（见 `K8S_SCALE` / USER_GUIDE）
- **R1**：Storage **仅** raftstore Ready 外提（已移除 `SKV_RAWNODE_STORE` / 节点内 IO 双轨）
- **R2–R5 数据面硬化**：模块边界约定；Coprocessor key/value 前缀 + keys/count_only + `ADMIN_COP`；SnapWorker 合并/重试/Stats；转主前追平门闩（见 DATA_PLANE_HARDENING）
- 单 Store RocksDB + RaftEngine；raftstore BatchSystem / Hibernate / SnapWorker 骨架
- Helm HA（Meta×3 + Storage×N + Gateway + Web）；运维页 JOIN / 拓扑
- MVCC 同 Region 事务 P1–P3；GC SafePoint
- 文档统一在 `docs/`（USER_GUIDE · NETWORKING · architecture/ops/test/flows · 管控台截图）
- **D1–D3 / A4**：README 与 ARCHITECTURE 对齐 gRPC 强制；Meta A4 标已实现

---

## 8. 相关文档

分类总目录：[README.md](README.md)。

| 文档 | 用途 |
|------|------|
| [ARCHITECTURE.md](architecture/ARCHITECTURE.md) | 总览 |
| [COMPONENT_MAP.md](architecture/COMPONENT_MAP.md) | 组件对照与 Phase |
| [META_PRODUCTION.md](production/META_PRODUCTION.md) | Meta 缺口与阶段 |
| [STORE_PRODUCTION.md](production/STORE_PRODUCTION.md) | Store + 跨 Region 2PC backlog |
| [MULTI_RAFT_PRODUCTION.md](production/MULTI_RAFT_PRODUCTION.md) | Raft 运行时 |
| [flows/13-version-cas.md](flows/13-version-cas.md) | 版本 CAS / 事务相关 |
| [NETWORKING.md](NETWORKING.md) | gRPC 网络栈 |
| [flows/README.md](flows/README.md) | 流程图分类索引 |
