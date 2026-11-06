# 分布式 KV 分片策略对比

本文对比业界常见分片策略，并结合主流开源项目说明优缺点。

**当前实现：** 已按方案 4 落地 **Range + 手动/阈值自动分裂 + 手动合并**，详见 [RANGE_SHARDING.md](RANGE_SHARDING.md)。（早期原型曾用取模哈希，已废弃。）

相关：架构总览见 [ARCHITECTURE.md](ARCHITECTURE.md)；扩容流程见 [flows/03-scale-out.md](../flows/03-scale-out.md)。

---

## 1. 总览

| 策略 | 典型开源 | 扩容方式 | 有序扫描 | 热点 | 路由复杂度 |
|------|----------|----------|----------|------|------------|
| 取模哈希 | 早期自研、部分分库中间件 | 改 N，大量重映射 | 差 | 靠哈希打散 | 低 |
| 一致性哈希 | Ketama、部分缓存代理 | 加节点影响邻居 | 差 | 虚拟节点缓解 | 中 |
| 哈希槽 | **Redis Cluster**、Codis | 迁槽，公式不变 | 差 | 仍可能单 key 热 | 中 |
| 范围分片 | **FoundationDB**、HBase、CockroachDB、**OpsKV** | 分裂/合并/搬迁 range | 强 | 需自动分裂 | 高 |
| 目录/查表 | Vitess（拓扑）、云数据库控制面 | Meta 改映射 | 取决于底层 | 可精细调度 | 高（依赖 Meta） |

---

## 2. OpsKV 与 Redis / FDB 的差异（速览）

| 维度 | OpsKV（当前） | Redis Cluster | FoundationDB |
|------|-------------------|---------------|--------------|
| 映射 | 有序 `[start, end)` Region；`locate(key)` | `CRC16(key) % 16384` → hash slot | 有序 key 空间上的连续 `[start, end)` |
| 扩容 | **分裂 / 合并** Region + Raft ConfChange | **迁移 slot**，key→slot 公式不变 | **分裂 / 合并 / 搬迁** range |
| 复制 | 每 Region 一组 Raft（RF=3，多数派 2） | 主从异步（槽在主上） | 存储复制 + 中心化事务管线 |
| 有序扫描 | 强（同 Region 内有序；跨 Region 可串联） | 弱 | 强 |

结论：OpsKV 与 **FDB 等同属 Range + Multi-Raft**；与 Redis 槽位迁移不是同一套策略。尚未具备 FDB 级分布式事务与完整 DD。

---

## 3. 各策略详解

### 3.1 取模哈希 `hash(key) % N`

**代表：** 手工分库分表、早期简单分片（OpsKV 早期原型曾用，已改为 Range）

**优点**

- 实现极简单，路由无状态，客户端/Proxy 可本地计算。
- 均匀性取决于哈希质量，一般场景够用。

**缺点**

- **N 一变，几乎所有 key 换片**（除非二次哈希或批量迁移方案）。
- 与 Multi-Raft 绑定时，扩容 = 改分片数或整组迁移，工程成本高。
- 无序，range / 前缀扫描要打全部 shard。

**适用：** 分片数长期稳定、按固定 Raft Group 划分的强一致 KV。

---

### 3.2 一致性哈希（哈希环 + 虚拟节点）

**代表：** Ketama（Memcached 客户端）、部分 Twemproxy 类缓存代理、部分 CDN 边缘缓存

**优点**

- 加/删节点时，**只搬环上相邻一小段**，比纯取模友好。
- 客户端可本地计算，不一定依赖中心 Meta。

**缺点**

- 虚拟节点数量、负载均衡调参麻烦；节点异构时仍易不均。
- 对**持久化强一致**不友好：副本集合与成员变更需另做，环本身解决不了共识。
- 同样不支持高效全局有序扫描。

**适用：** 可丢可重建的缓存、只读副本；少用于强事务分布式数据库的核心层。

---

### 3.3 哈希槽（固定 Slot 表）

**代表：** **Redis Cluster**（16384 slot）、**Codis**（固定槽表一类设计）

**优点**

- `key → slot` 公式**长期不变**；扩容是 **slot 在节点间搬家**。
- 运维模型清晰（迁槽、槽位图）；生态与工具成熟。
- 可用 hash tag（如 `{user100}:a`）把相关 key 钉在同槽，做局部多 key 操作。

**缺点**

- 槽位数量固定，过粗/过细都会别扭；大 key / 大 value 仍会形成热点。
- **跨槽事务 / 多 key 原子操作**受限（Redis 基本禁止跨槽多 key 原子写）。
- 有序遍历、前缀扫描不自然。
- 复制多为**主从异步**，不是「每槽一套共识」（与每 Region 一套共识不同）。

**适用：** 高 QPS 缓存型 KV、可接受较弱的跨 key 语义；水平扩容以迁槽为主。

---

### 3.4 范围分片（Range / Region）

**代表：** **FoundationDB**、**HBase**、**CockroachDB**；Cassandra 按 token 的 range 语义也接近

**优点**

- **有序 key、前缀扫描、范围查询**天然友好。
- 热点或过大可**自动分裂**；空闲可合并；Data Distribution / PD 在节点间搬 range。
- 与 **Multi-Raft（每 Region 一组）** 同构，CockroachDB 等走这条路。

**缺点**

- 实现最重：分裂、合并、迁移、路由表（PD / Meta）、与事务/时间戳联动。
- **单调写入**（纯自增 ID、时间戳后缀）易打成写热点，需打散设计（hash 前缀、反向时间等）。
- 客户端需缓存 region 路由并处理 stale（`NotLeader`、epoch 变化等）。

**FDB 与典型 Range+Multi-Raft KV**

| | FoundationDB | Range+Multi-Raft KV（典型） |
|--|--------------|------|
| 分片 | 有序 range，自动拆合迁移 | Region（range）+ PD 调度 |
| 共识/事务 | 中心化事务管线 + 存储复制 | 每 Region Raft + Percolator 类模型 |

分片策略同类，一致性与事务栈不同。事务层细节见 [TRANSACTION_SURVEY.md](TRANSACTION_SURVEY.md)。

**适用：** 分布式数据库、需要扫描 / 事务 / SQL 的下层 KV。

**OpsKV：** 已采用本策略（Region + Meta 路由 + 每 Region Raft）；自动 merge / 跨 Region 事务仍待补齐，见 [RANGE_SHARDING.md](RANGE_SHARDING.md)。

---

### 3.5 目录 / 查表分片（Meta 显式映射）

**代表：** **Vitess**（keyspace + shard 拓扑，可选 hash 或 range）；广义上 PD / FDB DD 的「range → 节点」也是目录的一种

**优点**

- **最灵活**：可按租户、表、热点做策略或人工调度。
- 扩容、缩容、副本放置可精细控制。
- 可组合其它策略（Vitess：业务侧选 hash 或 range shard）。

**缺点**

- **强依赖 Meta 的可用性与一致性**（需 Meta HA）。
- 路由多一跳或依赖本地缓存失效协议。
- 自动化不足时易退化为「人肉分片」。

**适用：** 分库中间件、多租户、需要业务级策略的场景；常作为 Range / Slot 之上的控制面。

---

## 4. 结合主流项目的选型经验

```text
只要缓存、加机器简单、跨 key 要求弱
 → Redis Cluster（哈希槽）

只要缓存、客户端一致性哈希即可
 → Ketama 类（一致性哈希）

要 SQL / 扫描 / 分布式事务，开源成熟栈
 → CockroachDB、HBase（Range + 自动分裂）

要严格 KV 事务、有序空间、工业级数据分布
 → FoundationDB（Range）

要有序 Range + 每片 Raft、可分裂合并（当前 OpsKV）
 → Range + Multi-Raft（方案 4 / Range）

按库表 / 租户切开、兼容 MySQL 协议
 → Vitess（目录 + 可选 hash/range）
```

---

## 5. 对照 OpsKV 现状与演进

当前默认：**3 Storage、开机 1 个 Region 0 覆盖全空间、每 Region RF=3**；容量靠 split/merge 增减 Region。

| 维度 | 当前 OpsKV | Redis Cluster | FDB / Range-Raft KV |
|------|----------------|---------------|------------|
| 映射 | 有序 Range Region | 固定 slot + 迁槽 | 有序 range + 分裂 |
| 复制 | 每 Region Raft RF=3 | 主从（槽在主上） | 每 Region Raft 或等价 |
| 扩容 | split/merge + ConfChange | 迁槽工具链 | PD / DD 调度 |
| 扫描 | 同 Region 有序 | 弱 | 强 |
| 缺口 | 双写窗口、跨 Region 事务、自动 merge | — | 已较成熟 |

**没有银弹：**

- 槽位：运维心智简单，偏缓存。
- Range：偏数据库与扫描 / 事务（**当前路线**）。
- 取模：实现最简单，扩容痛。
- 目录：策略最灵活，但绑死 Meta。

后续补齐优先：分裂流量窗口、跨 Region 事务、空闲 Region 自动 merge。成员变更编排见 [flows/03-scale-out.md](../flows/03-scale-out.md)。

---

## 6. 参考阅读（项目文档向）

- Redis Cluster 规范：hash slot、迁槽、hash tag
- FoundationDB：Data Distribution、key range / shard
- 工业界 Range KV：Region、中心调度、Raft Group
- CockroachDB：Range、split / merge
- Vitess：keyspace、shard、VSchema
