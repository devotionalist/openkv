# 后续优化项（Meta snap · WriteBatch · BatchSystem）

**状态：** 部分已落地，其余待做  
**相关：** [19-architecture-hardening](19-architecture-hardening.md) · [23-write-path-batching](23-write-path-batching.md)

本页跟踪**下一刀**工程项；已完成的写路径批处理见 23，勿在此重复当待办。

---

## 项

| ID | 内容 | 状态 |
|----|------|------|
| N1 | Meta compact 前写 `fsm_<gid>.snap`；`load_from_disk` 后 `snap_applier` 恢复 ClusterState | 已做 / 回归 `MetaFsm`·`FsmSnap` |
| N1b | **本地 FSM 快照多份 + 坏文件回退（待重新设计）** | 见下方「N1b」；现状单文件覆盖 |
| N3 | ONEPC apply 经 `KvEngine::write_batch` 一次写 data+write（含 expire） | 与 23 apply 批一并推进 |
| N4 | BatchSystem：mailbox `recv_batch`+CV；`SKV_RAFT_WORKERS` / `SKV_APPLY_WORKERS` | 已做：默认 8/8 + Helm；见 [25](25-write-path-knives.md) |
| N4b | BatchSystem schedule 通道 + drain-until-empty（禁扫全表） | 已做；见 [26](26-raftstore-schedule-pipeline.md) |
| N5 | Gateway BatchPut 并行度；Storage 级 batch propose | Gateway 并行 + Batch CQ fan-in 见 [28](28-compute-net-opt.md)；跨请求 batch propose **待做** |
| N8 | 二进制 Get / TSO async / stub 缓存 | 已做；见 [28](28-compute-net-opt.md) |
| N7 | `propose` 对 `kOnePc` 检查 `region_splitting` | 已做 |
| N9 | **海量数据 / 大快照迁移**（InstallSnapshot 逻辑 dump 过大） | 问题已记；见 [42](42-massive-snapshot-migration.md) |

---

## N1b：本地 FSM 快照多份保留（待重新设计）

**现状：** `save_local_fsm_snapshot_locked` 只写一份 `fsm_<gid>.snap`（`.tmp` → rename 覆盖）。每 32 条 apply / compact 前更新。重启只认这一份。

**问题：** 最新快照文件损坏（写一半、磁盘坏块、逻辑写出坏 blob）时：
- 无上一份可试；
- 若日志已被 compact 砍掉，重放也做不了 → 状态机可能停在 bootstrap（路由/TS 像刚装机）。

**期望方向（后边重做，本轮不实现）：**
1. 保留最近 **N=3** 份（按 index 或世代轮转）；
2. 启动按**新→旧**试装，带校验（magic / 长度 / `snap_applier` 成败）；
3. 某一份装成功且落后于 `last_applied` 时，用**仍在的日志**追平；
4. 三份都坏且日志不够 → 明确失败/告警，勿默默 bootstrap。

**代码锚点：** `raft/src/core/apply.cpp`：`save_local_fsm_snapshot_locked` / `restore_local_fsm_snapshot`。

---

## N9：海量数据迁移 / 大 InstallSnapshot（问题记录）

**问题：** 追赶路径用 `create_snapshot` 把 Region KV 扫进大 blob 过网；Region 一大则内存/带宽/apply 顶穿。本机 `create_checkpoint`（SST 硬链）更便宜，但**未**接到 InstallSnapshot。

**详情与方向：** [42-massive-snapshot-migration.md](42-massive-snapshot-migration.md)（流式导出 / SST ingest / 调度先 split 等，待排期）。

---

## 验收（回归）

| ID | 命令 / 期望 |
|----|-------------|
| O1 | `ctest -R 'MetaHa\|MetaFsm\|FsmSnap\|OnePc\|ApproxAndPickSplit\|IndexWaiter\|LatchAsync\|LatchHold'` PASS |
| O2 | Meta：split → compact → 重启 → `GET_ROUTE` regions 仍在 |
| O3 | 集群 put bench `fail=0`；无长期 Deadline Exceeded |
