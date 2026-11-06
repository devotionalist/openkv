# 分布式事务方案调研

面向 OpsKV（Range + Multi-Raft KV）的事务层选型调研。  
当前实现：**单 key 强一致写**（Region Leader → Raft 多数派 → Apply）；**无跨 Region / 多 key 事务**。

相关文档：[ARCHITECTURE.md](ARCHITECTURE.md)、[RANGE_SHARDING.md](RANGE_SHARDING.md)、[SHARDING_STRATEGIES.md](SHARDING_STRATEGIES.md)。

---

## 1. 问题与目标

### 1.1 已有能力

| 能力 | 状态 |
|------|------|
| 单 key Put/Get/Del | ✅ Region 内 Raft |
| Range 路由 / 分裂 / place | ✅ Meta + Storage |
| 跨 Region 原子多 key | ❌ |
| 快照隔离 / 可重复读 | ❌ |
| 全局提交序（TSO / HLC） | ❌ |

### 1.2 事务层要解决什么

1. **原子性**：多 key（可跨多个 Region）要么全成功，要么全失败。  
2. **隔离性**：常见目标为 **快照隔离（SI）**；部分系统再提供悲观锁 / 可串行化变体。  
3. **提交全序**：需要可比较的时间戳或等价机制，支撑 MVCC 读与冲突检测。  
4. **与分片共存**：Region 分裂、迁移、NotLeader 时，事务协议仍正确。

### 1.3 对 OpsKV 的约束

- 分片已是 **Range Range Region + 每 Region Raft**。  
- Compute（Gateway）无状态；状态在 Storage / Meta。  
- 优先选与现有模型 **同构** 的方案，避免先引一整套 SQL/分区表语义。

---

## 2. 主流路线总览

```
                    ┌─ Percolator（工业 SQL+KV）
  Range + Multi-Raft ┤
                    └─ CockroachDB（HLC + 并行提交变体）

  中心化事务管线 ──── FoundationDB（Resolver / Commit Proxy）

  分区库 + 经典 2PC ─ OceanBase、部分 NewSQL

  TrueTime / 原子钟 ─ Spanner（外加 2PC）
```

| 路线 | 代表 | 时间戳 | 跨分片提交 | 与 OpsKV 契合度 |
|------|------|--------|------------|---------------------|
| Percolator | 工业 SQL+KV | 中心 TSO | Prewrite + Commit（primary 先） | **高** |
| FDB 管线 | FoundationDB | 版本号（Proxy） | 中心化 commit path | 中（要改控制面） |
| 分区 2PC | OceanBase | GTS 等 | 协调者 + 分区参与者 | 低（缺分区/SQL 层） |
| Spanner 类 | Spanner、Yugabyte 部分 | TrueTime / HLC | 2PC + 外部时钟 | 中高（时钟成本不同） |
| 无跨分片事务 | Redis Cluster | — | 基本不支持 | 不适用目标 |

---

## 3. Percolator

### 3.1 要点

Google Percolator 模型在 **通用分布式 KV** 上实现多行事务：

1. **TSO**：中心时间戳服务分配 `start_ts` / `commit_ts`（中心时间戳服务）。  
2. **MVCC**：每 key 多版本；读用 `start_ts` 选可见版本。  
3. **锁与写意图**：写在 key 所在 Region 上留下 lock / data；冲突在存储侧检测。  
4. **两阶段**：  
   - **Prewrite**：所有 key 上锁并写入数据（未提交）。  
   - **Commit**：先提交 **primary key**，再异步 resolve **secondary**。  
5. SQL / 协调层是客户端；**锁与数据在 KV Region**。

### 3.2 流程（简化）

```
Client/SQL
  │  get start_ts (TSO)
  │  Prewrite(key1..keyN)  ──► 各 Region Leader（Raft）
  │  get commit_ts (TSO)
  │  Commit(primary)       ──► primary 所在 Region
  │  async Resolve(secondary)
```

### 3.3 优劣

| 优点 | 缺点 |
|------|------|
| 与 Multi-Raft Region **同构** | 需 TSO（Meta/PD 扩展）与 lock 列 |
| 工业验证充分 | 异步 secondary 需清理/恢复逻辑 |
| Gateway 可作轻量协调 | 大事务、热点 key 锁竞争明显 |
| 可渐进：先乐观，再加悲观 | 分裂窗口要与 epoch/锁共存设计 |

### 3.4 对 OpsKV

**首选参考实现路径**：Meta 提供 TSO（或独立 Timestamp 服务）→ Storage 扩展 lock/MVCC → Gateway（或独立 TxnClient）跑 Prewrite/Commit。

---

## 4. FoundationDB（FDB）

### 4.1 要点

- 同样是 **有序 key range** 分片，但事务 **不走「每分片独立 Percolator」**。  
- **Transaction / Commit 管线中心化**：Client → Proxy → Resolver（冲突检测）→ 存储按版本持久化。  
- 存储侧重复制与按版本读；**冲突检测与提交序**在专用角色上完成。  
- 对外提供严格的 **可串行化 / 乐观事务** API（`get` / `set` / `commit`）。

### 4.2 与 Percolator 的本质差别

| | Percolator | FDB |
|--|-------------------|-----|
| 冲突检测位置 | 各 Region（锁 / 写冲突） | Resolver（中心化） |
| 提交路径 | 分散 Prewrite + Commit | 集中 commit pipeline |
| 存储角色 | 持锁 + MVCC + Raft | 版本化存储 + 复制 |
| 运维复杂度 | 锁残留、异步 resolve | Proxy/Resolver 扩展与热点 |

### 4.3 对 OpsKV

分片相似，**控制面差一层**。若选 FDB，需新增 Proxy/Resolver 类角色，与当前「Gateway 无状态 + Storage Multi-Raft」差距更大。可作为长期对照，**不宜作为第一版事务路径**。

---

## 5. OceanBase（分区库 + 2PC）

### 5.1 架构差异（相对 SQL+KV 分离）

| | SQL+KV 分离 | OceanBase |
|--|------|-----------|
| 形态 | 存算分离：SQL + KV + 元数据 | **一体化 OBServer** + RootService |
| 分片 | Region（自动分裂的 key range） | **表分区 / 分区组 / Log Stream** |
| 复制 | 每 Region Raft | 分区/日志流上 **Paxos** |
| 存储 | RocksDB（KV 层） | **自研引擎** |
| 事务模型 | **Percolator** | **MVCC + 全局时间戳 + 跨分区 2PC** |

### 5.2 事务路径

- **单分区**：本地事务，成本低。  
- **跨分区**：经典 **2PC**（协调者 + 各分区参与者），参与者与 clog/副本绑定。  
- 全局时间戳（GTS 一类）支撑快照与提交序，但 **不是**「锁行沉在通用 KV Region」的 Percolator 流水线。  
- SQL、锁、多版本更多在 **OBServer 进程内**完成。

### 5.3 直观对比

```
SQL+KV:    SQL ──RPC──► 多个 Region Raft（锁/数据在 KV）
OceanBase: SQL+引擎同进程；跨分区再 2PC ──► 各分区 Paxos 组
```

### 5.4 对 OpsKV

OceanBase 强依赖 **分区表 + 库内引擎** 叙事；当前 OpsKV 是纯 KV Multi-Raft。要对齐需先补分区/SQL 抽象，**与现有架构不同向**。调研结论：作对照即可，不作为默认事务方案。

---

## 6. 其他常见系统（速览）

### 6.1 CockroachDB

- Range + Multi-Raft。  
- **HLC**（混合逻辑时钟）而非中心 TSO。  
- 事务层在 KV 上，含并行提交等优化；隔离目标偏可串行化。  
- 启示：可用 HLC **降低 TSO 单点压力**，但实现与时钟管理成本不低。

### 6.2 Spanner / YugabyteDB

- Spanner：**TrueTime** + 2PC，外带原子钟/GPS。  
- Yugabyte：DocDB，类 Percolator/Hybrid，偏 Raft 分片。  
- 启示：外部时间可信则可简化 commit wait；普通机房很少上 TrueTime。

### 6.3 Vitess

- MySQL 分片 + VTGate；跨分片事务能力有限或依赖应用/2PC 扩展。  
- 与「通用 KV 事务」目标不同。

### 6.4 Redis Cluster

- Hash 槽；**基本禁止跨槽多 key 原子写**。  
- 不满足分布式数据库事务目标。

### 6.5 单机 / 本地事务优先

- 许多业务通过 **同 Region 共置**（共同前缀、同分区键）把事务降成单 Raft Group 提交。  
- 可作为优化：**尽量同 Region，跨 Region 再走完整协议**。

---

## 7. 横向对比

### 7.1 协议与组件

| 维度 | Percolator 系 | FDB | OceanBase | Cockroach |
|------|-----------|-----|-----------|-----------|
| 分片 | Region | Range | 表分区/LS | Range |
| 共识 | Multi-Raft | 存储复制+中心事务 | 分区 Paxos | Multi-Raft |
| 时间戳 | PD TSO | Commit 版本 | GTS | HLC |
| 跨分片 | Percolator | Resolver 管线 | 2PC | 事务记录+锁/写意图 |
| SQL 耦合 | 低（KV 通用） | 无（纯 KV） | 高（一体化） | 中（SQL 一体） |

### 7.2 工程成本（相对 OpsKV 现状）

| 方案 | 新增组件 | Storage 改动 | 风险 |
|------|----------|--------------|------|
| Percolator | TSO；Txn 协调（可在 Gateway） | lock/MVCC、Prewrite/Commit RPC | 中，路径清晰 |
| FDB 管线 | Proxy、Resolver、版本服务 | 大改写入路径 | 高 |
| OB 式 2PC | 协调者、分区模型 | 大（语义不同） | 高 |
| 仅同 Region 原子 | 无/少 | 多 key 同 Region batch | 低，能力窄 |

---

## 8. 与 OpsKV 的映射建议

### 8.1 推荐路线（第一版）

**采用 Percolator 类模型**，理由：

1. 与现有 **Range + Multi-Raft** 同构。  
2. Gateway 无状态协调、Storage 持锁，符合存算分离。  
3. 可分阶段交付，不必先上 SQL/分区表。

### 8.2 建议分期

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| P0 | 明确「无跨 Region 事务」；同 Region 多 key 可先做 Leader 本地原子 batch（可选） | 文档 + 可选 API |
| P1 | Meta（或独立）**TSO**；Storage **MVCC + lock** 列 | 单 Region 乐观事务 |
| P2 | 跨 Region **Prewrite / Commit primary / Resolve** | 多 Region 乐观事务 |
| P3 | 悲观锁、大事务限流、与 **split/merge/place** 的 epoch 协同 | 生产可用 |
| 可选 | HLC 替代或部分替代 TSO；FDB 管线仅作研究对照 | — |

### 8.3 明确不做（当前）

- 不以 OceanBase 一体化分区 2PC 为主路径。  
- 不以 Redis「禁跨槽」为最终模型。  
- 双写窗口 / 分裂流量暂停与事务并发：单独设计，见 [flows/10-region-placement.md](../flows/10-region-placement.md) 与 RANGE 文档「仍未做」。

### 8.4 最小概念数据模型（Percolator 向）

```
UserKey →  { lock?,  write(commit_ts→start_ts),  data(start_ts→value)... }
TSO     →  单调 start_ts / commit_ts
Txn     →  primary_key + secondary_keys + start_ts
```

读：按 `start_ts` 找最新已提交 `write`。  
写：Prewrite 占 lock；Commit 写 `write` 并清 lock。

---

## 9. 结论

| 结论 | 说明 |
|------|------|
| SQL+KV 与 OceanBase **事务方案不同** | 前者 Percolator on KV；后者分区库 MVCC + 跨分区 2PC |
| FDB 与 Percolator 系 **分片同类、事务栈不同** | FDB 中心化 commit；Percolator 分散锁与 Prewrite |
| OpsKV **应对齐 Percolator** | 成本最低、与 Region/Raft 一致 |
| 现状 | **P1–P3 已落地**：TSO、乐观/悲观锁、CheckTxnStatus/Resolve、Region epoch |

## 10. 实现状态（OpsKV）

| 组件 | 能力 |
|------|------|
| Meta | `GET_TS`；`GET_SAFE_POINT` / `ADVANCE_SAFE_POINT` / `REPORT_MIN_START_TS`；Region `epoch`；路由含 epoch |
| Storage gRPC | `PREWRITE`/`COMMIT`/`ROLLBACK`/`TXN_GET`；`PESSIMISTIC_*`；`CHECK_TXN_STATUS`/`RESOLVE_LOCK`/`GET_LOCK`；可选 epoch 校验 |
| RocksDB | lock（含悲观）/ data / write |
| Gateway | 乐观事务 + `TxnGet` 遇锁自动 Check→Resolve；primary 先提交 + secondary 异步 resolve；epoch 失配刷路由；上报活跃 `min_start_ts`；`Watch`；副本读 |
| Go SDK | `Begin`/`Get`/`GetOpt`/`Put`/`Watch`/`LockKeys`/`Commit`/`Rollback` |

示例：`client/go/examples/txn`、`txn_p3`。

### GC safe point（TSO）

| 步骤 | 说明 |
|------|------|
| 水位 | `gc_safe_ts ≈ min(next_ts - retain, min_start_ts_hint)`，只前进 |
| Meta | Leader ~5s `ADVANCE_SAFE_POINT`；Gateway 周期 `REPORT_MIN_START_TS` |
| Storage | Leader tick `GET_SAFE_POINT` → Raft `kGcBelow`；`commit_ts < safe` 只留最新一版作 floor |

### 运维相关

| 项 | 说明 |
|----|------|
| 分裂写暂停 | `ADMIN_PREPARE_SPLIT`；写返回 `splitting` |
| `SKV_TXN_GC` | 默认开；Leader ~10s 清过期锁 + 按 Meta safe point 做 MVCC GC |
| Gateway Put/Get | 内部走 Prewrite/Commit / TxnGet，与事务可见性一致 |
| `SKV_FAULT_DROP_AE_PCT` / `SKV_FAULT_DELAY_AE_MS` | Storage 注入丢/拖 AppendEntries；`make fault-drill` 覆盖 |
| Watch / 读副本 | `WATCH_POLL` + Gateway 流；`GetOpt(..., false)` |
