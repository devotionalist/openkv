# OpsKV 文档

仓库全部设计 / 运维 / 测试文档集中在本目录。

快速入口也见根目录 [README.md](../README.md)。

---

## 怎么找

| 你想… | 去 |
|-------|-----|
| 本地 / Helm 怎么用 | [USER_GUIDE.md](./USER_GUIDE.md) |
| gRPC / Seastar | [NETWORKING.md](./NETWORKING.md) |
| 架构长什么样 | [architecture/](#1-架构与模型) |
| 某条读写/扩容流程 | [flows/](#6-流程图-flows) |
| 扩缩容怎么操作 | [ops/](#4-运维--sop) |
| 门禁怎么跑 | [test/](#5-测试与演练) |
| 还缺什么 | [BACKLOG.md](./BACKLOG.md) |

---

## 入门（公开上手）

| 文档 | 说明 |
|------|------|
| [USER_GUIDE.md](./USER_GUIDE.md) | 用户使用手册：安装、读写、管控台、Helm、扩容 |
| [NETWORKING.md](./NETWORKING.md) | gRPC 强制 / Seastar 可选；组件与端口 |
| [images/console-overview.png](./images/console-overview.png) | Web 管控台截图（`:19100`） |

---

## 1. 架构与模型 — `architecture/`

| 文档 | 说明 |
|------|------|
| [ARCHITECTURE.md](./architecture/ARCHITECTURE.md) | 存算分离总览、本地拓扑 |
| [DATA_HA.md](./architecture/DATA_HA.md) | **数据高可用**：Meta×3 + Region RF=3、故障域、验收索引 |
| [DATA_SCALE.md](./architecture/DATA_SCALE.md) | **数据扩容**：加节点≠加入、join→balance、Place、与 Split 边界 |
| [DATA_SCALE_IN.md](./architecture/DATA_SCALE_IN.md) | **数据缩容**：Draining→迁光→Tombstone、勿强杀、Helm 顺序 |
| [ERROR_CODES.md](./architecture/ERROR_CODES.md) | **各节点错误码**：Storage / Meta / Gateway / DS / Raft token 与重试策略 |
| [COMPONENT_MAP.md](./architecture/COMPONENT_MAP.md) | 组件 / Storage 内部 / 写路径 / Phase |
| [RANGE_SHARDING.md](./architecture/RANGE_SHARDING.md) | Region 有序区间分片 |
| [SHARDING_STRATEGIES.md](./architecture/SHARDING_STRATEGIES.md) | 分片策略对比 |
| [TRANSACTION_SURVEY.md](./architecture/TRANSACTION_SURVEY.md) | 事务方案调研与现状 |
| [DATA_STRUCTURES.md](./architecture/DATA_STRUCTURES.md) | Hash / Set / List / ZSet |
| [SEASTAR_NET.md](./architecture/SEASTAR_NET.md) | Seastar 开关与阶段（业务未接入） |

---

## 2. 生产要求 — `production/`

| 文档 | 说明 |
|------|------|
| [META_PRODUCTION.md](./production/META_PRODUCTION.md) | Meta 控制面 |
| [STORE_PRODUCTION.md](./production/STORE_PRODUCTION.md) | Storage / Store |
| [MULTI_RAFT_PRODUCTION.md](./production/MULTI_RAFT_PRODUCTION.md) | Multi-Raft / raftstore |
| [DEPS.md](./production/DEPS.md) | FetchContent 依赖 |
| [JEMALLOC.md](./production/JEMALLOC.md) | jemalloc |

---

## 3. 数据拓扑 — `topology/`

索引：[DATA_TOPOLOGY_INDEX.md](./topology/DATA_TOPOLOGY_INDEX.md)

| 模块 | 文档 |
|------|------|
| Meta | [META_DATA_TOPOLOGY.md](./topology/META_DATA_TOPOLOGY.md) |
| Raft | [RAFT_DATA_TOPOLOGY.md](./topology/RAFT_DATA_TOPOLOGY.md) |
| Storage | [STORE_DATA_TOPOLOGY.md](./topology/STORE_DATA_TOPOLOGY.md) |
| Compute | [COMPUTE_DATA_TOPOLOGY.md](./topology/COMPUTE_DATA_TOPOLOGY.md) |
| Common | [COMMON_DATA_TOPOLOGY.md](./topology/COMMON_DATA_TOPOLOGY.md) |

---

## 4. 运维 / SOP — `ops/`

| 文档 | 说明 |
|------|------|
| [REGION_OPS.md](./ops/REGION_OPS.md) | Split / Merge / Place / Scatter |
| [SCALE_OUT_SOP.md](./ops/SCALE_OUT_SOP.md) | 扩容 SOP |
| [SCALE_LIVE_SOP.md](./ops/SCALE_LIVE_SOP.md) | 在线扩缩容 |
| [SKV_CLI.md](./ops/SKV_CLI.md) | `skv` CLI |
| [ADMIN_CONSOLE.md](./ops/ADMIN_CONSOLE.md) | Web 管控台 |
| [FAULT_DRILL.md](./ops/FAULT_DRILL.md) | 故障演练 |

K8s：[flows/K8S_HELM.md](./flows/K8S_HELM.md) · [flows/K8S_SCALE.md](./flows/K8S_SCALE.md)

---

## 5. 测试与演练 — `test/`

| 文档 | 说明 |
|------|------|
| [TEST_CASES.md](./test/TEST_CASES.md) | 用例清单与执行记录 |
| [INTEGRATION_TEST.md](./test/INTEGRATION_TEST.md) | 集成场景编号 |
| [flows/IT_COVERAGE_EXPANSION.md](./flows/IT_COVERAGE_EXPANSION.md) | Web/Region/Txn/磁盘门禁 |

---

## 6. 流程图 — `flows/`

分类索引：[flows/README.md](./flows/README.md)（读写 · Raft/故障 · Region/扩缩容 · 工程变更 · K8s）。

---

## 7. 规划

| 文档 | 说明 |
|------|------|
| [BACKLOG.md](./BACKLOG.md) | 待做计划与已完成对照 |

---

## 目录树

```
docs/
├── README.md              ← 本文件
├── USER_GUIDE.md
├── NETWORKING.md
├── BACKLOG.md
├── images/
├── architecture/
├── production/
├── topology/
├── ops/
├── test/
└── flows/
```
