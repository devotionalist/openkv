# raft 模块（`openkv_raft`）

OpenKV 的 **Multi-Raft 协议 + 本机调度运行时**，编成静态库 `openkv_raft`。

| 命名空间 | 目录 | 职责 |
|----------|------|------|
| `openkv::raft` | `include/openkv/raft/` · `src/*.cpp` | 协议状态机、内存日志、Ready、Engine、出站 Transport |
| `openkv::raftstore` | `include/openkv/raftstore/` · `src/raftstore/` | Store / BatchSystem / Peer / Router |

**分层契约、所有权、File 布局与耐久语义：** → [ARCHITECTURE.md](ARCHITECTURE.md)

**调用 / 时序图：** → [docs/flows/35-raft-module-call-flows.md](../docs/flows/35-raft-module-call-flows.md)

生产落盘：`storage::RocksRaftEngine`。本库提供接口 `RaftEngine` 与 `FileRaftEngine`（Meta / 单测）。

```text
RaftKV::async_write → propose → Store::request_ready
  → BatchSystem → persist_ready → Engine::consume
  → Apply → report_applied
```

## 文件索引

各文件顶部有 `@file`（职责 / 架构 / 协作 / 注意）。

### 协议核 `openkv::raft`

| 文件 | 作用 |
|------|------|
| `types.hpp` | Role / Peer / LogEntry / SoftState / ConfChange |
| `raft_log.*` | **内存**日志；落盘不在此 |
| `ready.hpp` | Ready / Message / HardState |
| `raw_node.*` | 纯协议（`external_io`） |
| `raft_node.*` | 单 Group 完整节点 + persist_ready |
| `raft_engine.*` | Engine / LogBatch 接口；RLE/RLS 编解码 |
| `raft_log_file.*` | RLG 文件容器编解码（与单条 RLE 分离） |
| `file_raft_engine.*` | 文件版 Engine（GroupPaths + 追加/rewrite） |
| `raft_persist.*` | SyncMode + 旧文本 persist（仅迁移） |
| `grpc_transport.*` | 出站 gRPC |
| `multi_raft.*` | 门面 → Store |
| `snap_stream.hpp` | 入站快照分片缓冲 |
| `raft_frame.hpp` | → `openkv::net::skv_frame` |

### 运行时 `openkv::raftstore`

| 文件 | 作用 |
|------|------|
| `store.*` | 入口、Ready 攒批、入站 RPC |
| `batch_system.*` | Raft/Apply 双线程池 |
| `router.*` / `mailbox.hpp` / `schedule_queue.hpp` | 投递与唤醒 |
| `msg.hpp` | 邮箱消息 |
| `peer.*` | 每 Region 运行时 |
| `store_meta.*` | Region 元数据 |
| `snap_worker.*` | 出站快照通道 |
| `write_batch_merge.hpp` | Apply 展平辅助 |
