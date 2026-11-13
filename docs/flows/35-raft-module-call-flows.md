# Raft 模块调用流程（协议核 + raftstore）

**状态：** 与源码对齐（`raft/` · `openkv_raft`）  
**源码地图：** [raft/README.md](../../raft/README.md) · **分层契约：** [raft/ARCHITECTURE.md](../../raft/ARCHITECTURE.md)  
**相关：** [06-raft-election-commit](06-raft-election-commit.md) · [24-persist-raft-vs-kv](24-persist-raft-vs-kv.md) · [26-raftstore-schedule-pipeline](26-raftstore-schedule-pipeline.md) · [MULTI_RAFT_PRODUCTION](../production/MULTI_RAFT_PRODUCTION.md)

## 问题

`raft/` 下同时有 `openkv::raft`（协议）与 `openkv::raftstore`（调度）。需要**调用关系图 + 执行时序图**对照源码，避免把内存 `RaftLog` 误当成落盘层。

---

## 1. 组件调用关系（静态）

谁依赖谁、消息怎么走：

```mermaid
flowchart TB
  subgraph storage_layer["storage/"]
    SN[StorageNode]
    RKV[RaftKV]
    RRE[RocksRaftEngine]
    RS[RaftService gRPC 入站]
  end

  subgraph raft_facade["raft/ 门面"]
    MR[MultiRaft]
    GT[GrpcRaftTransport 出站]
  end

  subgraph raftstore["openkv::raftstore"]
    ST[Store]
    RT[Router]
    MB[(Mailbox)]
    SQ[ScheduleQueue]
    BS[BatchSystem]
    PE[Peer]
    SW[SnapWorker]
    SM[StoreMeta]
  end

  subgraph protocol["openkv::raft 协议核"]
    RN[RaftNode / RawNode]
    RL[RaftLog 内存]
    RD[Ready]
    FE[FileRaftEngine]
  end

  SN --> MR
  SN --> RKV
  SN --> RS
  MR --> ST
  RKV -->|propose / request_ready| RN
  RKV -->|request_ready| ST
  RS -->|apply_inbound_raft| ST

  ST --> RT
  ST --> BS
  ST --> PE
  ST --> SW
  ST --> SM
  RT --> MB
  RT -->|notify| SQ
  BS --> SQ
  BS -->|on_raft_batch / on_apply_batch| ST
  PE --> RN

  RN --> RL
  RN -->|take_ready| RD
  ST -->|persist_fn| RN
  RN -->|persist_ready → consume| RRE
  RN -.->|UT/Meta| FE
  ST -->|transport_fn| GT
  RN -->|messages| GT
```

---

## 2. 短写执行流程（Propose → 落盘 → Apply）

客户端 Put/RawPut → Gateway → Storage `StoreService` → `RaftKV::async_write`。

```mermaid
sequenceDiagram
  autonumber
  participant KV as RaftKV
  participant RN as RaftNode
  participant Log as RaftLog内存
  participant St as Store
  participant RT as Router/Mailbox
  participant BS as BatchSystem
  participant Eng as RocksRaftEngine
  participant FSM as KV Apply

  KV->>RN: propose(data)
  RN->>Log: append（仅内存）
  KV->>St: request_ready(region)
  Note over St: ready_coalesce_n / ms 攒批
  St->>St: drive_ready → kWake
  St->>RT: send_raft + schedule
  RT->>BS: ScheduleQueue.push
  BS->>St: on_raft_batch（同 Region 串行）
  St->>RN: process_ready / take_ready
  St->>RN: persist_ready(Ready)
  RN->>Eng: build_engine_batch + consume(sync)
  Note over Eng: persist_sync=data → Raft WAL fsync
  St->>BS: send_apply(ApplyMsg)
  BS->>FSM: ApplyBatchFn / ApplyFn
  FSM->>RN: report_applied(index)
  RN->>RN: maybe_compact
```

**关键代码**

| 步骤 | 位置 |
|------|------|
| propose + request_ready | `storage/src/raftkv.cpp` · `RaftKV::async_write` |
| 攒批 | `raft/src/raftstore/store.cpp` · `request_ready` / `coalesce_loop` |
| 批末 Ready | `Store::on_raft_batch` → `process_ready_legacy` |
| 落盘 | `raft/src/raft_node.cpp` · `persist_ready` |
| Apply 回告 | `RaftNode::report_applied` |

---

## 3. 入站 Raft RPC

```mermaid
flowchart LR
  A[RaftService gRPC] --> B[Store::apply_inbound_raft]
  B --> C[step_raft_message<br/>RaftNode::on_*]
  C --> D{批处理?}
  D -->|同步路径| E[立刻 process_ready]
  D -->|邮箱路径| F[on_raft_batch 合并 Ready]
  E --> G[persist_fn]
  F --> G
  G --> H[transport_fn 出站]
  G --> I[ApplyMsg]
```

禁止：`RaftService` 直调 `RaftNode::on_*`。  
出站：`GrpcRaftTransport` 或 Store `transport_fn`。

---

## 4. Tick / 选举 / 心跳

```mermaid
sequenceDiagram
  participant T as Store::schedule_tick
  participant MB as Peer Mailbox
  participant BS as BatchSystem raft worker
  participant St as Store::on_raft_batch
  participant RN as RaftNode::tick
  participant Net as Transport

  T->>MB: TickMsg × N Peers
  MB->>BS: schedule
  BS->>St: drain mailbox
  St->>RN: handle_tick → tick
  Note over RN: PreVote / heartbeat / check_quorum
  St->>RN: 批末 process_ready
  RN->>Net: AE / RV / PreVote 等
```

Hibernate：Follower 空闲后多数 tick 跳过，仍低频 tick 保选举。

---

## 5. 内存日志 vs 落盘

```mermaid
flowchart TB
  A[RaftLog 内存] --> B[build_engine_batch_locked]
  B -->|相对 stable_* append/cut/state| C[RaftLogBatch]
  C --> D[RaftEngine::consume]
  D --> E[FileRaftEngine<br/>raft/ Meta·UT]
  D --> F[RocksRaftEngine<br/>storage/ 生产]
```

`RaftLog` **没有**落盘 API；`raft_persist.cpp` 仅旧文本迁移。

File 引擎布局与追加契约见 [ARCHITECTURE.md §5](../../raft/ARCHITECTURE.md)：

- 容器编解码：`raft_log_file`（RLG2）
- 单条记录：`encode_log_entry`（RLE2）
- 纯追加只写 RLE；cut / 冲突 index → 整文件 rewrite

---

## 边界与失败

- 单 Region 热点：固定落在同一 raft/apply worker + 一条 fsync 流水线。
- `persist_sync=data`：Raft Engine sync；业务 KV apply 异步（[24](24-persist-raft-vs-kv.md)）。
- `external_io` 下 propose 不等 apply；可见性靠 Apply / ProposalQueue。

## 验收

| ID | 标准 |
|----|------|
| F1 | 本节图与 `raft/README.md` 文件表一致 |
| F2 | 从 `RaftKV::async_write` 能跳到 `persist_ready` / `consume` |
| F3 | 热路径源码有行内注释 |

未跑 `-mode all`：纯文档。
