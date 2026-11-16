# RaftNode 协作类

**状态：** 已落地  
**相关：** [raft/ARCHITECTURE.md](../../raft/ARCHITECTURE.md) · [raft/README.md](../../raft/README.md)

## 问题

`raft_node.cpp` 曾堆选举、成员、复制、落盘于一身，难读难改。

## 方案

对外仍只暴露 `RaftNode`。内部协作类（均持 `RaftNode&`、`friend`、**不含锁**，由 `RaftNode::mu_` 保护）：

| 协作类 | 管什么 |
|--------|--------|
| `Election` | 角色切换、PreVote/Vote、CheckQuorum |
| `Membership` | Joint、ConfChange、多数派、加删 peer |
| `Replication` | 心跳/AE、抬 commit、InstallSnapshot |
| `LogPersist` | 组 batch、刷引擎、Ready persist |
| `Apply` | 已提交条目推进状态机、compact、本地 FSM 快照、report/wait applied |
| `LinearRead` | Leader lease + ReadIndex 多数派 ACK |
| `ReadyPump` | 出站攒批/flush、has/take Ready、唤醒 Store |
| `ProgressTracker` / `ReadyIo` | 复制水位；Ready 缓冲字段 |

门面保留：propose / tick 编排、对外 API 转发。

## 边界

- 对外 API / ConfChange 线协议不变。
- 不加测试专用双轨路径。

## 验收

| ID | 标准 |
|----|------|
| C1 | `core/` 有 election…apply / linear_read / ready_pump |
| C2 | `--gtest_filter='Raft*:DbRaft*'` 全绿 |
| C3 | 全量 `openkv_unit_tests` 全绿 |

未跑 `-mode all`：热路径语义未改，仅拆协作类。
