# Raft 模块架构契约

本文定义 `openkv_raft` 的**分层边界、所有权与耐久语义**。实现与评审以本文为准；调用时序见 [docs/flows/35-raft-module-call-flows.md](../docs/flows/35-raft-module-call-flows.md)。

## 1. 目标与非目标

**目标**

- 单库同时提供协议状态机与本机 Multi-Raft 调度，避免「协议库 / store 库」分裂导致双份依赖。
- 持久化通过 `RaftEngine` 抽象；生产 Storage 使用 `storage::RocksRaftEngine`，Meta / 单测使用 `FileRaftEngine`。
- Ready → persist → apply → report 的职责可追踪到具体类型与线程池。

**非目标**

- 不在本模块实现业务 KV 的 MVCC / TTL / Compact。
- 不把内存 `RaftLog` 伪装成 WAL；落盘只发生在 `RaftEngine::consume`。
- 不在热路径用「整库扫描」代替按 group / 按 index 的读写。

## 2. 分层（自上而下）

```text
┌─────────────────────────────────────────────────────────────┐
│  调用方：storage::RaftKV / StorageNode / Meta                 │
├─────────────────────────────────────────────────────────────┤
│  门面：MultiRaft · GrpcRaftTransport（出站）                   │
├─────────────────────────────────────────────────────────────┤
│  调度：raftstore::Store · Router · BatchSystem · Peer         │
│        （按 Region mailbox + Raft/Apply 双池）                 │
├─────────────────────────────────────────────────────────────┤
│  协议：RaftNode / RawNode · Ready · RaftLog（内存）            │
├─────────────────────────────────────────────────────────────┤
│  持久化：RaftEngine（接口）                                    │
│         ├─ RocksRaftEngine（生产，在 storage/）                 │
│         └─ FileRaftEngine（Meta / UT，本库）                   │
└─────────────────────────────────────────────────────────────┘
```

| 层 | 命名空间 | 允许依赖 | 禁止 |
|----|----------|----------|------|
| 门面 | `openkv::raft` | raftstore、Transport | 直接写盘 |
| 调度 | `openkv::raftstore` | 协议核、Engine 指针 | 解析 RLG/Rocks key |
| 协议 | `openkv::raft` | types / log / ready | 知悉 Region 路由细节 |
| 引擎 | `RaftEngine` 实现 | 编解码、OS/DB | 持有 Peer / BatchSystem |

物理目录：协议与 Transport 在 `include/openkv/raft` + `src/*.cpp`；调度在 `include/openkv/raftstore` + `src/raftstore/`。同属 `openkv_raft` 目标。

## 3. 所有权与生命周期

| 对象 | 所有者 | 说明 |
|------|--------|------|
| `Store` | `MultiRaft` / StorageNode | 进程内单例级 |
| `Peer` + `RaftNode` | Store（经 Router） | 随 Region 创建/销毁 |
| `RaftEngine` | StorageNode 注入；或 RaftNode 自建 File 引擎 | 多 Group 共享同一 Engine |
| `RaftLog`（内存） | RaftNode | 与 Engine 窗口对齐；崩溃后由 Engine 恢复 |
| Ready 批 | BatchSystem Raft 线程 | `persist_ready` 内消费 |

`RaftNode::persist_ready` 是协议侧唯一允许触发 `Engine::consume` 的热路径入口（另有显式 flush / 迁移）。

## 4. 写路径契约

1. **Propose**：只改内存 log / unstable，并 `request_ready`。
2. **Ready 攒批**：Store 合并同 Region 的 ready 信号，BatchSystem 拉起。
3. **persist_ready**：把 HardState + 新增/截断日志编入 `RaftLogBatch`，一次 `consume(sync)`。
4. **Apply**：仅在 commit 推进后，由 Apply 池执行业务写入；`persist_sync=data` 时 KV 不强制 sync。
5. **report_applied**：回写 `last_applied`，可再进 Engine。

失败语义：`consume` 返回 0 或抛错视为本批未落盘；调用方不得推进「已持久化」游标。File 引擎对 IO 失败写日志并返回 0。

## 5. FileRaftEngine 布局与语义

```text
{root}/
  g{group_id}/
    raft_state      # RLS1（原子：tmp → sync → rename）
    raft_log        # RLG2 头 + 连续 RLE2 记录
```

| 操作 | 行为 |
|------|------|
| 纯追加（无 cut、index 严格衔接） | 文件尾 append 记录；魔数只写一次 |
| cut / 覆盖同 index / 非 RLG2 | 内存合并后整文件 rewrite（tmp → rename） |
| put_raft_state | 与本批 log 变更同一 `consume` 临界区提交 |
| SyncMode::kData | 脏文件 FULLFSYNC / fdatasync |
| SyncMode::kAll | 文件 sync + root 目录 sync |
| SyncMode::kNone | 不 sync（仅测速 / 单测） |

编解码独立在 `raft_log_file`（文件容器）与 `raft_engine`（单条 RLE / RLS）。**禁止**在 append 路径再次写入带魔数的「整文件 blob」。

## 6. 遗留路径

- `RaftPersist`：旧文本 `hard.state` / `log.entries`，仅迁移与兼容单测。
- `FileRaftEngine::migrate_legacy_persist_dir`：导入后一律走 Engine。

新代码不得再写 `RaftPersist::save` 作为主路径。

## 7. 验收

- 单元：`RaftEngineTest.*`（含纯追加、cut/gc、CRC）。
- 集成：现有 `raftstore` / `RaftNode` 用例不回归。
- 架构：本文件与 `README.md` 文件索引一致；热路径注释不引用外部产品名对照。
