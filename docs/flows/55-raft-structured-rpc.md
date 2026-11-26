# 55 · Raft 交互结构化载荷（无 pipe|hex）

**问题：** 出站 AE/Vote/Snap 曾把字段拼成 `term|…|hex(data)` 字符串，入站再 `split` + `from_hex`，再填 gRPC proto——多一次分配与编解码，热路径浪费明显。

**方案：** `Ready::Message` / `RaftMessage` 直接扛 `VoteReq` / `AppendEntriesReq` / `InstallSnapshotReq`（`entries[].data` 为二进制）。`GrpcRaftTransport` 与 Storage 入站按结构体填/读 proto；`Store::apply_inbound_raft` 回 `RaftReply`，不再拼 `OK AE …` 行。

**边界：**

- 线协议仍是 gRPC `RaftService`（字段未改）；改的是**进程内**组装与回包。
- Meta 嵌入路径走同一 `TransportSend(Message)`；Meta TCP 调试口若仍要文本，可本地格式化，生产路径不走。
- external_io 快照壳：`InstallSnapshotReq.data` 空 → SnapWorker 填 blob 后再发。

**失败 / 回滚：** 行为与原先一致；若回包 `RaftReply::kError`，gRPC 侧对应 `success/ok=false`。回滚即恢复字符串拼装（不推荐）。

**组件关系：** Replication/Election `enqueue` → ReadyPump → Store `transport_fn_` / 嵌入 `flush_transport` → `GrpcRaftTransport::bind` → proto；入站 `RaftService` → `handle_raft_*` 组 `RaftMessage` → `step_raft_message` → `RaftReply` → 填 Response。

## 验收

| ID | 命令 / 期望 |
|----|-------------|
| S1 | `./build/tests/openkv_unit_tests --gtest_filter='RaftRpcStructured.*'` PASS |
| S2 | 相关 Raft gtest（Ready / FaultScenario / Persist）PASS |
| S3 | 本机 `make cluster-up` 后 `./.run/kv-example 127.0.0.1:19000` 成功；全 API：`(cd client/go && go build -o ../../.run/kv-bench ./examples/bench) && ./.run/kv-bench -addr 127.0.0.1:19000 -mode all …`，对话贴 SUMMARY |

附带：

- 大括号整理曾误在 `Latches::unlock` 多写一次 `++it`，erase 到 `end()` 后 SEGV；本机 become leader → SetEpoch 必现。已修，见 `SchedulerTest.UnlockSingleHeldDoesNotSegfault`。
- 本机 `cluster-up` 后 Put/TSO `DeadlineExceeded`：多层问题——(1) AE 应答误清 `snap_inflight_` + InstallSnapshot 双 `Finish`；(2) `maybe_compact` 曾把日志窗砍空；(3) Meta `fill_ts_response` 把 `count` 写死成 1，Gateway TSO 批货失效，每次 Put 再打 `AllocTs(5000)` 把 Meta Raft 打满。已修 inflight/双 Finish/compact 滞回与 trailing window，并恢复 `count` 回传；GetTs 失败不再 `Finish(OK)`。见 `RaftRpcStructured.StaleInstallSnapshotReplyDoesNotRegressMatch` · [53-raft-log-window](53-raft-log-window.md)。

相关：`raft_rpc.hpp` · [06-raft-election-commit](06-raft-election-commit.md) §3 · [54-storage-raft-boundary](54-storage-raft-boundary.md)
