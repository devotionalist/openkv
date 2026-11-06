# OpsKV 处理流程图索引

每个流程一份独立 Markdown（内含 Mermaid）。与当前实现对齐：**Go Gateway + Meta/Storage gRPC + Range Region + Multi-Raft RF=3**。

上级目录：[`docs/README.md`](../README.md) · 公开网络说明：[docs/NETWORKING.md](../NETWORKING.md)

---

## A. 读写与数据结构

| 流程 | 文件 |
|------|------|
| 写入 Put | [01-write-put.md](01-write-put.md) |
| 读取 Get | [02-read-get.md](02-read-get.md) |
| not_leader 重定向 | [08-not-leader-redirect.md](08-not-leader-redirect.md) |
| Watch | [11-watch.md](11-watch.md) |
| Hash / Set / List / ZSet | [12-data-structures.md](12-data-structures.md) · [DATA_STRUCTURES.md](../architecture/DATA_STRUCTURES.md) |

---

## B. Raft / 启动 / 故障

**设计总览（两面 HA、故障域、验收）：** [DATA_HA.md](../architecture/DATA_HA.md) · 演练手册：[FAULT_DRILL.md](../ops/FAULT_DRILL.md)

| 流程 | 文件 |
|------|------|
| Raft 选举 / 心跳 / 提交 | [06-raft-election-commit.md](06-raft-election-commit.md) |
| Storage 节点启动 | [07-storage-startup.md](07-storage-startup.md) |
| 故障恢复（WAL / AE / Snapshot） | [04-fault-recovery.md](04-fault-recovery.md) |
| Meta HA | [05-meta-ha.md](05-meta-ha.md) |

---

## C. Region / 扩缩容

**设计总览：** [DATA_SCALE.md](../architecture/DATA_SCALE.md)（扩容）· [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md)（缩容 Draining→Tombstone）

| 流程 | 文件 |
|------|------|
| Range 分裂 / 合并 | [09-range-split-merge.md](09-range-split-merge.md) |
| Region 放置 place | [10-region-placement.md](10-region-placement.md) |
| 扩容 / 缩容设计 | [扩容缩容设计.md](扩容缩容设计.md) |
| Scale-out（加副本 · 抬 RF） | [03-scale-out.md](03-scale-out.md) |

配套 SOP：[REGION_OPS.md](../ops/REGION_OPS.md) · [SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md) · [SCALE_LIVE_SOP.md](../ops/SCALE_LIVE_SOP.md)

---

## D. 工程变更 / 数据面

| 流程 | 文件 |
|------|------|
| gRPC 强制 | [GRPC_MANDATORY.md](GRPC_MANDATORY.md) |
| 数据面硬化（Cop / Snap / 转主） | [DATA_PLANE_HARDENING.md](DATA_PLANE_HARDENING.md) |
| 集成覆盖扩展 | [IT_COVERAGE_EXPANSION.md](IT_COVERAGE_EXPANSION.md) |

---

## E. Kubernetes

| 流程 | 文件 |
|------|------|
| Helm HA 部署 | [K8S_HELM.md](K8S_HELM.md) |
| 加节点 vs 运维页加入 | [K8S_SCALE.md](K8S_SCALE.md) |

公开步骤：[docs/USER_GUIDE.md](../USER_GUIDE.md) §3–§4。

---

## 相关总览

[ARCHITECTURE.md](../architecture/ARCHITECTURE.md) · [COMPONENT_MAP.md](../architecture/COMPONENT_MAP.md) · [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md) · [FAULT_DRILL.md](../ops/FAULT_DRILL.md) · [INTEGRATION_TEST.md](../test/INTEGRATION_TEST.md) · [MULTI_RAFT_PRODUCTION.md](../production/MULTI_RAFT_PRODUCTION.md)

> VS Code / GitHub / Cursor 可直接预览 Mermaid。
