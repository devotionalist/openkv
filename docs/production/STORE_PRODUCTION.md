# 生产级 Store（存储层）要求与路线图

本文定义 OpsKV **Store** 达到工业级/生产级的能力要求。  
Raft 运行时见 [MULTI_RAFT_PRODUCTION.md](MULTI_RAFT_PRODUCTION.md)；本文聚焦 **数据面、Region 元数据、事务、引擎与运维**。

对标：工业界 Region Store / Cockroach Store。

---

## 1. 目标画像

| 维度 | 生产目标 |
|------|----------|
| 正确 | 崩溃/杀进程后 Region 边界与 epoch 不丢、不乱；无静默丢写 |
| 事务 | 单 Region Percolator 硬化；跨 Region 2PC 可演进 |
| 引擎 | RocksDB WAL/sync 与 Raft 提交语义对齐；大 Region 可快照 |
| 安全 | Client / Raft / Admin 隔离；鉴权 |
| 可运维 | 指标、备份、分裂/迁移可恢复 |

---

## 2. 能力总表

### 2.1 正确性（P0）

| ID | 要求 | 状态 |
|----|------|------|
| S1 | **Admin 入 Raft**（split/merge/prepare/epoch）多数派提交后再改盘 | **已实现** |
| S1b | **Region epoch 持久化**（引擎 meta key，重启可恢复） | **已实现** |
| S1c | **txn_get 线性读**（ReadIndex，与 GET 对齐） | **已实现** |
| S2 | 原子 Range 迁移（两侧 WriteBatch；禁逐 key 半成品） | **已实现** |
| S2b | 右 Region 成员与左一致（ENSURE + Meta 路由校验） | **已实现** |
| S3 | Apply 冲突对客户端可见（禁止静默 `break`） | **已实现** |
| S4 | 跨 Region 2PC / ResolveLock / primary 租约 | **暂缓**（见 §6） |

### 2.2 耐久与引擎（P1）

| ID | 要求 | 状态 |
|----|------|------|
| E1 | RocksDB sync/WAL 与 Raft `SyncMode` 对齐 | **已实现** |
| E2 | Column Family（data/lock/write） | **已实现** |
| E3 | Checkpoint / 增量快照 | **已实现**（Checkpoint；线式快照仍全量） |
| E4 | Propose 幂等（request_id） | **已实现** |

### 2.3 安全与运维（P1/P2）

| ID | 要求 | 状态 |
|----|------|------|
| O1 | TLS + Admin ACL | **已实现**（ACL）；TLS 走 sidecar |
| O2 | Prometheus / 每 Region 延迟直方图 | **已实现** |
| O3 | Watch 持久化与 revision=Raft index | **已实现** |
| O4 | Client / Raft / Admin 队列隔离 | **已实现** |

---

## 3. 分阶段

### Store Phase S1（当前）

- [x] 文档  
- [x] `CmdOp`：`kAdminPrepareSplit` / `kAdminSplit` / `kAdminMerge` / `kAdminSetEpoch`  
- [x] `on_apply` 执行迁移与 epoch；Leader `admin_*` 改为 propose + 等 apply  
- [x] epoch 写入引擎 meta key（`\xff\xffskv/epoch`），启动/ensure 时加载；migrate 跳过 meta  
- [x] `txn_get(linearizable)` 走 `read_index`  

**验收**：`StoreAdminRaftTest.*`；杀进程后 epoch 恢复；Admin 经 Raft apply。

### Store Phase S2

- [x] `migrate_ge`：dst/src 各一次 `WriteBatch`（先写后删）  
- [x] `on_apply` 发布结果；`propose` / `propose_encoded` 回传 `locked` / `write_conflict` / …  
- [x] 右 Region 成员与 Meta 一体：`seed_right_region`（继承左成员 ENSURE）+ Split 仅 Raft propose；Meta 缺失 right 告警  

**验收**：`ApplyConflictVisibleToClient` / `MigrateGeAtomicBatches` / `SplitSeedsRightWithLeftPeers`。

### Store Phase S3

- [x] E4 Propose 幂等：`Command.request_id` 尾部编码；Leader 缓存短路；apply 跳过重复  
- [x] E1：`persist_sync` / `SKV_PERSIST_SYNC` → Raft `persist_sync` + RocksDB `WriteOptions.sync` / `use_fsync`  
- [x] E2：RocksDB CF `default|lock|data|write`；按 `user\0{L|D|W}` 路由；启动迁移 default 遗留  
- [x] E3：`create_checkpoint` / `restore_from_checkpoint`；`ADMIN_CHECKPOINT`；Raft 导出前 Flush  
- [x] O2：每 Region propose/get 延迟直方图；`METRICS`（Prometheus 文本）；`ADMIN_REGION_STATS` 含 p50/p99  
- [x] O3：Watch 日志落盘（`\xff\xffskv/watch/`）；revision 统一 Raft index；`WATCH_HEAD`  
- [x] O4：入站 Client/Raft/Admin inflight 隔离（`ERR busy`）；STATS 暴露 reject/inflight；Raft 出站优先级队列沿用  
- [x] O1：Admin ACL（`SKV_ADMIN_TOKEN`；`AUTH` 后放行 `ADMIN_*`）；TLS 由 stunnel/envoy/nginx 终止（**不含** S4）  

**验收**：`InflightGateTest.*`；`AdminAclTest.*`；Client 打满时 Raft 仍可进入。

---

## 4. Admin 日志编码（S1）

普通 Command：`op(1)|klen|vlen|ttl|key|value`（op &lt; 0xF0）。

| CmdOp | 值 | key | value |
|-------|-----|-----|-------|
| `kAdminPrepareSplit` | 8 | split_key | （空） |
| `kAdminSplit` | 9 | split_key | `right_region` u32 LE |
| `kAdminMerge` | 10 | （空） | `right_region` u32 LE |
| `kAdminSetEpoch` | 11 | （空） | `epoch` u64 LE |

与 Raft ConfChange（首字节 `0xFF`）互不冲突。

Epoch meta key：`\xff\xffskv/epoch`（每 Region 引擎内）。

---

## 5. 变更记录

| 日期 | 内容 |
|------|------|
| 2026-10-21 | 初版 Store 生产级要求；启动 S1 |
| 2026-10-21 | S1 完成（Admin Raft / epoch / txn_get ReadIndex） |
| 2026-10-21 | S2 核心：Apply 结果回传 + migrate WriteBatch |
| 2026-10-21 | S2b：`seed_right_region` + Split 去 TCP 广播 |
| 2026-10-21 | E4：`request_id` 幂等 propose |
| 2026-10-21 | **S4 跨 Region 2PC 暂缓**，记入 backlog |
| 2026-10-21 | E1：RocksDB sync 与 Raft SyncMode 对齐 |
| 2026-10-21 | E2：Column Family lock/data/write |
| 2026-10-21 | E3：RocksDB Checkpoint + ADMIN_CHECKPOINT |
| 2026-10-21 | O2：Region 延迟直方图 + METRICS |
| 2026-10-21 | O3：Watch 持久化 + revision=Raft index |
| 2026-10-21 | O4：Client/Raft/Admin 入站 inflight 隔离 |
| 2026-10-21 | O1：`SKV_ADMIN_TOKEN` + `AUTH`；TLS 推荐 sidecar |

### O1 运维鉴权（简要）

- 环境变量 `SKV_ADMIN_TOKEN`：非空则启用；空则开发模式（不鉴权）。
- 连接内先发 `AUTH <token>` → `OK AUTH`；随后 `ADMIN_*` 才放行。
- `skv` / Meta→Storage / Store 互调 `tcp_line` 在 token 已设时自动握手。
- **TLS**：进程监听明文 TCP；生产在前面挂 stunnel / Envoy / nginx 做 TLS 终止（避免链 OpenSSL 进 Seastar 演示路径）。

---

## 6. Backlog：S4 跨 Region 2PC（暂缓）

**决定**：先不做。现状：单 Region Percolator + Gateway 已有「按 key 路由 → primary 先 commit → secondary 尽力 / Resolve」雏形；完整跨 Region 2PC 另开迭代。

**待做清单（恢复时按序）**：

1. Primary 锁携带 `secondaries[]`（编码进 `LockInfo` / prewrite value）  
2. `CheckTxnStatus` 回传 secondaries；崩溃恢复按列表 Resolve  
3. Primary 锁 TTL 租约 + `LOCK_HEARTBEAT` 续约  
4. Gateway：按 Region 分组 prewrite；secondary 先、primary 后（带 secondaries）；commit 后 async resolve  
5. 跨 Region 单测 / 故障演练（杀进程后 Resolve 一致性）

**现状可依赖**：同 Region 事务路径、`ResolveLock` / `CheckTxnStatus`、epoch 校验、Gateway `resolveBlockingLock`。
