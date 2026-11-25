# Storage ↔ Raft 职责边界（内聚约定）

**状态：** 设计约定 · 出站改包在 Store · 出站 Transport 在 MultiRaft  
**相关：** [raft/ARCHITECTURE.md](../../raft/ARCHITECTURE.md) · [35](35-raft-module-call-flows.md) · [COMPONENT_MAP](../architecture/COMPONENT_MAP.md)

---

## 1. 一句话

**Storage 只管真实数据读写**（引擎、MVCC、Region 范围、客户端 Store API）。  
**共识（选举 / 复制 / 提交 / 快照 / 出站入站）在 `openkv_raft` 内聚**；Storage 只做组合根，注入 peer / apply / snap，不实现传输与协议。

心智模型：

```text
没有共识时：  Storage ≈ KvEngine 读写 + 对外 API
有了共识后：  同一套读写，外面多包一层 Raft（propose → 复制提交 → apply 再写引擎）
```

共识是耐久与多副本的包装层，不是把选举复制揉进 KvEngine。边界清楚时：摘掉 Raft，Storage 仍能讲清「怎么读写」；Raft 仍能讲清「怎么达成一致」而不依赖 MVCC。

---

## 2. 谁干什么

| 归属 | 管什么 | 不管什么 |
|------|--------|----------|
| **StorageNode / KvEngine** | 用户 KV、MVCC、范围、客户端 RPC、本机数据目录；注入 peer 地址 / apply / snap 字节 | AE/Vote 组包、peer 复制水位、线协议改包、出站队列生命周期 |
| **Raft 协议核** | term、日志、commit、角色 | Rocks 用户表、gRPC listen 端口运维 |
| **raftstore::Store** | Region 邮箱、Ready 顺序（persist→出站→apply） | 业务语义；**理想情况下不解析 gRPC 线格式** |
| **MultiRaft** | 多 Group 门面；**持有 `GrpcRaftTransport`**，构造时接到 Store `transport_fn_` | 用户 Put/Get |
| **raft::GrpcRaftTransport** | 出站队列、stub；入站 demux（目标） | 用户 Put/Get |

依赖方向（只允许箭头向下/向外调用库）：

```text
StorageNode ──注入 peer 地址 / apply / snap 字节──► MultiRaft（内含 Transport + Store）
Store / Transport / RaftNode  ──禁止──►  StorageNode / KvEngine 业务类型
```

Apply 边界：Raft 只回调「这条已提交日志的 bytes」；**怎么解成 KV 写** 是 Storage 的 `apply_fn`。

---

## 3. 目标形态

```text
┌─ Storage ─────────────────────────────────────┐
│  客户端 Store API · KvEngine · Region 元数据     │
│  组合根：peer 地址、listen、apply_fn、snap 字节   │
└──────────────────┬────────────────────────────┘
                   │ 注入，不实现共识 / 不出站
┌─ openkv_raft ────▼────────────────────────────┐
│  MultiRaft：持有 Transport；start 出站→Store，stop 反向 │
│  Transport：出站 +（目标）入站                   │
│  Store：mailbox / Ready / persist 调度          │
│  RaftNode：协议状态机                           │
│  RaftEngine：共识 WAL（非用户数据）              │
└───────────────────────────────────────────────┘
```

Storage **不应**再出现：

- 手写快照线协议改包（补 term 等）  
- `set_transport_fn` / 自持有出站 `GrpcRaftTransport`  
- 除「转给 Store」以外的 Raft RPC 状态机逻辑  
- 复制限流 / next-match / 选举计时等协议细节

---

## 4. 现状与差额

| 点 | 状态 | 说明 |
|----|------|------|
| 出站快照改包 | **已做** | Store SnapWorker 拼 payload 后直接 `InstallSnapshot` |
| 出站接线 | **已做** | MultiRaft 持有 Transport 并接线；Storage 只 `set_peer` / `register_node` / apply |
| propose→apply 等待 | **已做** | 通用 `raftstore::IndexWaiter`；Storage 只挂指标并把 apply 业务 error 塞进 `invoke` |
| 入站 | 待收 | 仍 `RaftService` → `handle_raft_*` → `apply_inbound_raft` |
| Store API | 待收 | 仍为 `transport_fn_` 闭包；理想 `Transport::send` |

---

## 5. 验收

| ID | 期望 | 结果 |
|----|------|------|
| B1 | Storage 源码无 `InstallSnapshotData` 拼 term | 已满足（改包在 `store.cpp`） |
| B2 | Storage 无 `set_transport_fn`；出站属 MultiRaft | 本刀已满足 |
| B3 | 共识单测不链 StorageNode；可用 FakeTransport | 既有能力保留（gtest 不强制起 gRPC） |
| B4 | 写路径仍：persist → 出站 → apply | 语义不变 |

改动后跑：相关 gtest / 编译；全 API bench 见对话（数据面出站路径有动）。
