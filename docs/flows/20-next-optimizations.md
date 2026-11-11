# 后续优化项（Meta snap · WriteBatch · BatchSystem）

**状态：** 部分已落地，其余待做  
**相关：** [19-architecture-hardening](19-architecture-hardening.md) · [23-write-path-batching](23-write-path-batching.md)

本页跟踪**下一刀**工程项；已完成的写路径批处理见 23，勿在此重复当待办。

---

## 项

| ID | 内容 | 状态 |
|----|------|------|
| N1 | Meta compact 前写 `fsm_<gid>.snap`；`load_from_disk` 后 `snap_applier` 恢复 ClusterState | 已做 / 回归 `MetaFsm`·`FsmSnap` |
| N3 | ONEPC apply 经 `KvEngine::write_batch` 一次写 data+write（含 expire） | 与 23 apply 批一并推进 |
| N4 | BatchSystem：mailbox `recv_batch`+CV；`SKV_RAFT_WORKERS` / `SKV_APPLY_WORKERS` | 部分；空转 sleep 已收敛 |
| N5 | Gateway BatchPut 并行度；Storage 级 batch propose | Gateway 并行已抬；Storage batch propose **待做** |
| N7 | `propose` 对 `kOnePc` 检查 `region_splitting` | 已做 |

---

## 验收（回归）

| ID | 命令 / 期望 |
|----|-------------|
| O1 | `ctest -R 'MetaHa\|MetaFsm\|FsmSnap\|OnePc\|ApproxAndPickSplit\|ProposalQueue\|LatchHold'` PASS |
| O2 | Meta：split → compact → 重启 → `GET_ROUTE` regions 仍在 |
| O3 | 集群 put bench `fail=0`；无长期 Deadline Exceeded |
