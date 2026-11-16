# raft 模块（`openkv_raft`）

OpenKV 的 **Multi-Raft 协议 + 本机调度运行时**，编成静态库 `openkv_raft`。按职责分包，可作通用共识库使用。

| 命名空间 | 目录 | 职责 |
|----------|------|------|
| `openkv::raft::`（core/log/engine/net/util） | `include/openkv/raft/{core,log,engine,net,util}/` | 协议门面、日志、WAL 引擎、出站、工具 |
| `openkv::raftstore` | `include/openkv/raftstore/` · `src/raftstore/` | Store / BatchSystem / Peer / Router / PersistWorker |

**分层契约：** → [ARCHITECTURE.md](ARCHITECTURE.md)  
**时序：** → [docs/flows/35-raft-module-call-flows.md](../docs/flows/35-raft-module-call-flows.md)

生产落盘：`DbRaftEngine`（当前后端 RocksDB）。Meta / 单测可用 `FileRaftEngine`。Store **默认**调用 `RaftNode::persist_ready`，消费者不必再 `set_persist_fn`。

旧路径头文件（如 `openkv/raft/raft_node.hpp`）仍可用，内部转发到分包路径。

```text
Propose → Store::request_ready → BatchSystem
  → persist_ready → Engine::consume → 发网 → Apply → report_applied
```

## 分包索引

### `core/` — 协议门面

| 类型 | 作用 |
|------|------|
| `RaftNode` | 单 Group 门面（编排复制/Ready/落盘） |
| `Election` | 选主：角色切换、PreVote/Vote、CheckQuorum |
| `Membership` | 成员表 / Joint / ConfChange / 多数派 |
| `Replication` | 心跳/AE、抬 commit、InstallSnapshot |
| `LogPersist` | 组 batch、刷 RaftEngine、Ready persist |
| `Apply` | 已提交条目推进状态机、compact、本地 FSM 快照、report/wait |
| `LinearRead` | Leader lease + ReadIndex 多数派 ACK |
| `ReadyPump` | 出站攒批/flush、has/take Ready、唤醒 Store |
| `ProgressTracker` | Leader 复制进度 next/match/inflight |
| `ReadyIo` | Ready 缓冲字段 |
| `MultiRaft` / `Ready` / `types` | 多组门面与类型 |

### `log/` — 内存日志与文件编解码

### `engine/` — 持久化

| 类型 | 作用 |
|------|------|
| `RaftEngine` | WAL 抽象 |
| `FileRaftEngine` | 目录文件实现 |
| `DbRaftEngine` | 嵌入式 KV 多 Group 实现（当前 RocksDB） |
| `SyncMode` / `PersistedHardState` | 耐久级别与必须落盘的元状态（`sync_mode.hpp`） |

### `net/` — 出站传输（`GrpcRaftTransport`，仍绑 OpenKV storage.v1；后续可再拆）

### `raftstore/` — Multi-Raft 调度（结构保持）
