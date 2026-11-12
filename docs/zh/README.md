> 语言: [English](../en/README.md) · [中文](./README.md)

# OpenKV 文档

**默认语言为英文**：[docs/en/](../en/README.md)。本页为中文索引；根目录快速入口见 [README.md](../../README.md)。

---

## 怎么找

| 你想… | 去 |
|-------|-----|
| **接客户端（Go/Rust/Java/TS）** | [独立 SDK 仓](#0-客户端独立仓库) · [15-multi-client-sdk](../flows/15-multi-client-sdk.md) |
| 本地 / Helm 怎么用 | [USER_GUIDE.md](../USER_GUIDE.md) · 英文 [user-guide](../en/user-guide.md) |
| gRPC 网络 | [NETWORKING.md](../NETWORKING.md) · 英文 [networking](../en/networking.md) |
| 架构长什么样 | [architecture/](#1-架构与模型) · 英文摘要 [architecture](../en/architecture.md) |
| 某条读写/扩容流程 | [flows/](#6-流程图-flows) |
| 扩缩容怎么操作 | [ops/](#4-运维--sop) |
| 门禁怎么跑 | [test/](#5-测试与演练) |
| 还缺什么 | [BACKLOG.md](../BACKLOG.md) |
| 怎么贡献代码 | [contributing](../en/contributing.md) · [中文](contributing.md) |

---

## 0. 客户端（独立仓库）

对外 SDK **不在本仓**，请用：

| 语言 | 仓库 |
|------|------|
| Go | [openkv-client-go](https://github.com/devotionalist/openkv-client-go) |
| Rust | [openkv-client-rust](https://github.com/devotionalist/openkv-client-rust) |
| Java | [openkv-client-java](https://github.com/devotionalist/openkv-client-java) |
| TypeScript | [openkv-client-typescript](https://github.com/devotionalist/openkv-client-typescript) |

本仓 `client/go` 仅 **examples / 工具**（对外 SDK 见独立仓）。说明：[flows/15-multi-client-sdk.md](../flows/15-multi-client-sdk.md)。

---

## 入门（公开上手）

| 文档 | 说明 |
|------|------|
| [USER_GUIDE.md](../USER_GUIDE.md) | 用户使用手册：安装、读写、管控台、Helm、扩容 |
| [NETWORKING.md](../NETWORKING.md) | gRPC 强制；组件与端口 |
| [images/console-overview.png](../images/console-overview.png) | Web 管控台截图（`:19100`） |

---

## 1. 架构与模型 — `architecture/`

| 文档 | 说明 |
|------|------|
| [ARCHITECTURE.md](../architecture/ARCHITECTURE.md) | 存算分离总览、本地拓扑 |
| [DATA_HA.md](../architecture/DATA_HA.md) | **数据高可用**：Meta×3 + Region RF=3、故障域、验收索引 |
| [DATA_SCALE.md](../architecture/DATA_SCALE.md) | **数据扩容**：加节点≠加入、join→balance、Place、与 Split 边界 |
| [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md) | **数据缩容**：Draining→迁光→Tombstone、勿强杀、Helm 顺序 |
| [ERROR_CODES.md](../architecture/ERROR_CODES.md) | **各节点错误码**：Storage / Meta / Gateway / DS / Raft token 与重试策略 |
| [COMPONENT_MAP.md](../architecture/COMPONENT_MAP.md) | 组件 / Storage 内部 / 写路径 / Phase |
| [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md) | Region 有序区间分片 |
| [DATA_STRUCTURES.md](../architecture/DATA_STRUCTURES.md) | Hash / Set / List / ZSet |

---

## 2. 生产要求 — `production/`

| 文档 | 说明 |
|------|------|
| [META_PRODUCTION.md](../production/META_PRODUCTION.md) | Meta 控制面 |
| [STORE_PRODUCTION.md](../production/STORE_PRODUCTION.md) | Storage / Store |
| [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md) | Multi-Raft / raftstore |
| [DEPS.md](../production/DEPS.md) | FetchContent 依赖 |
| [JEMALLOC.md](../production/JEMALLOC.md) | jemalloc |

---

## 3. 数据拓扑 — `topology/`

索引：[DATA_TOPOLOGY_INDEX.md](../topology/DATA_TOPOLOGY_INDEX.md)

| 模块 | 文档 |
|------|------|
| Meta | [META_DATA_TOPOLOGY.md](../topology/META_DATA_TOPOLOGY.md) |
| Raft | [RAFT_DATA_TOPOLOGY.md](../topology/RAFT_DATA_TOPOLOGY.md) |
| Storage | [STORE_DATA_TOPOLOGY.md](../topology/STORE_DATA_TOPOLOGY.md) |
| Compute | [COMPUTE_DATA_TOPOLOGY.md](../topology/COMPUTE_DATA_TOPOLOGY.md) |
| Common | [COMMON_DATA_TOPOLOGY.md](../topology/COMMON_DATA_TOPOLOGY.md) |

---

## 4. 运维 / SOP — `ops/`

| 文档 | 说明 |
|------|------|
| [REGION_OPS.md](../ops/REGION_OPS.md) | Split / Merge / Place / Scatter |
| [SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md) | 扩容 SOP |
| [SCALE_LIVE_SOP.md](../ops/SCALE_LIVE_SOP.md) | 在线扩缩容 |
| [SKV_CLI.md](../ops/SKV_CLI.md) | `skv` CLI |
| [ADMIN_CONSOLE.md](../ops/ADMIN_CONSOLE.md) | Web 管控台 |
| [FAULT_DRILL.md](../ops/FAULT_DRILL.md) | 故障演练 |

K8s：[flows/K8S_HELM.md](../flows/K8S_HELM.md) · [flows/K8S_SCALE.md](../flows/K8S_SCALE.md)

---

## 5. 测试与演练 — `test/`

| 文档 | 说明 |
|------|------|
| [TEST_CASES.md](../test/TEST_CASES.md) | 用例清单与执行记录 |
| [INTEGRATION_TEST.md](../test/INTEGRATION_TEST.md) | 集成场景编号 |
| [flows/IT_COVERAGE_EXPANSION.md](../flows/IT_COVERAGE_EXPANSION.md) | Web/Region/Txn/磁盘门禁 |

---

## 6. 流程图 — `flows/`

分类索引：[flows/README.md](../flows/README.md)：

| 分区 | 内容 |
|------|------|
| A | KV API（Put/Get/CAS/TTL/Scan/Watch/DS/SDK） |
| B | 一致性 / Raft / HA |
| C | Region 生命周期与扩缩容 |
| D | Gateway 与写路径内部（TSO / 异步 / 批写） |
| E | 部署 / 门禁 / 工程变更 |

新人建议：`01 → 02 → 08 → 06 → 07 → 09 → 18 → 22 → 23`。

---

## 7. 规划与贡献

| 文档 | 说明 |
|------|------|
| [BACKLOG.md](../BACKLOG.md) | 待做计划与已完成对照 |
| [贡献与提交规则](contributing.md) | 仅 PR、所有者批准、tag/Release 限制 |

---

## 目录树

```
docs/
├── README.md              ← 语言入口（英/中）
├── en/                    ← 英文入门
├── zh/                    ← 中文索引与贡献说明
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

协议：[Apache License 2.0](../../LICENSE)
