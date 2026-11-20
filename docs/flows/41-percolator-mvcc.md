# Percolator 风格 MVCC（事务平面）

**状态：** 与 `storage/mvcc.hpp` · `storage/src/mvcc.cpp` · apply 路径对齐  
**相关：** [37-onepc-vs-2pc](37-onepc-vs-2pc.md) · [27-raw-vs-txn](27-raw-vs-txn.md) · [01-write-put](01-write-put.md) · [02-read-get](02-read-get.md) · [13-version-cas](13-version-cas.md) · [STORE_DATA_TOPOLOGY](../topology/STORE_DATA_TOPOLOGY.md)

---

## 1. 解决什么问题

事务平面要在多客户端并发下同时做到：

| 需求 | 白话 |
|------|------|
| 快照读 | 读「我开始事务那一刻该看见的世界」，不被半截写干扰 |
| 写写冲突可检测 | 两人改同一 key，后来者能明确失败并重试 |
| 多 key 原子提交 | 要么都生效，要么都能根据 primary 判清并收尾 |
| 读写少互堵 | 不靠「写时整表锁死读」 |

**Percolator 风格做法：** 多版本（MVCC）+ 时间戳排序（TSO）+ 以 primary 为中心的两阶段提交（Prewrite → Commit）。

它**不**替代 Raft（复制/选举仍是共识层）；也**不**用于 Raw 平面（Raw 无 L/D/W）。

---

## 2. 一句话

**同一 user key 留多版本；写先占 Lock，再写 Write 表示提交；读按 `start_ts` 找已提交版本。**  
OpenKV 物理上拆成 **L / D / W** 三类 key。

---

## 3. 三张逻辑记录（L / D / W）

物理编码（见 `mvcc.hpp`）：`user + '\0' + kind[+ ts]`

| 种类 | Kind | 白话 | 典型内容 |
|------|------|------|----------|
| **Lock** | `L` | 有人正在写这个 key | `start_ts`、op、primary、TTL、可选暂存 value |
| **Data** | `D` + `start_ts` | 实际值挂在事务开始 ts 上 | value；可选用户 TTL |
| **Write** | `W` + `~commit_ts` | 「这次提交生效了」 | 指向哪个 `start_ts`、op、过期 |

```text
读路径（直觉）：
  在 W 里找 commit_ts ≤ start_ts 的最新合法 Write
    → 取出其 start_ts
    → 去 D 取 data
  若撞上别人的 L → 返回 locked（再 Resolve / 重试）
```

---

## 4. 时间戳

| 符号 | 谁发 | 干什么 |
|------|------|--------|
| `start_ts` | TSO（Gateway `alloc`） | 事务快照点；Data 版本号 |
| `commit_ts` | TSO（须 `> start_ts`） | 提交顺序；Write 版本号 |

隔离直觉：**快照隔离**——读只看见 `commit_ts ≤ start_ts` 的已提交写；写若发现有更新的 Write，则 `write_conflict`。

---

## 5. 写路径：2PC 与 1PC

### 5.1 经典 2PC（多 key / 显式 Txn）

```text
1. 取 start_ts
2. Prewrite 各 key：写 L（+ 通常写 D）
3. 取 commit_ts
4. Commit primary：写 W、清 L
5. Commit secondary：同上；失败可凭 primary 状态 Resolve
```

**Primary**：事务的判决钥。别人撞锁时查 primary，可知 committed / rolledback / 仍 locked（含是否过期）。

### 5.2 1PC / ONEPC（单 key 短 Put/Delete）

一次 Raft Propose 内写完 **D + W**，**不留**长期事务锁。语义上仍是 MVCC 提交，只是把两阶段收成一步。细节见 [37](37-onepc-vs-2pc.md)。

### 5.3 何时走哪条

| 场景 | 路径 |
|------|------|
| 单 key `Put` / `Delete` / CAS | **1PC** |
| 多 key / `TxnBegin`… | **2PC** |
| `RawPut` / `RawDelete` | **不是** Percolator（无 TSO、无 L/D/W） |

---

## 6. 读路径与冲突

| 现象 | 含义 | 常见处理 |
|------|------|----------|
| 正常读到值 / 空 | 快照下可见或未写过 | — |
| `locked` | 他事务持 L | CheckTxnStatus → Resolve → 重试 |
| `write_conflict` | 已有更新的 W（或等价冲突） | **换新 start_ts** 重试 |
| `lock_not_found` | Commit 时锁对不上 | 失败或走 resolve |

错误码表：[ERROR_CODES.md](../architecture/ERROR_CODES.md)。

---

## 7. 源码锚点

| 层 | 位置 |
|----|------|
| 编解码 / 读写 API | `storage/include/openkv/storage/mvcc.hpp` · `storage/src/mvcc.cpp` |
| Apply：OnePc / Prewrite / Commit / Rollback | `storage/src/storage_node.cpp`（`on_apply` 各 `CmdOp`） |
| CF 路由 | Rocks `lock` / `data` / `write`（见 Store 生产文档 E2） |
| 单测 | `tests/storage/mvcc_test.cpp` |
| 流程对照 | [37-onepc-vs-2pc](37-onepc-vs-2pc.md) · [27-raw-vs-txn](27-raw-vs-txn.md) |

---

## 8. 边界与非目标

| 做 | 不做 / 另层 |
|----|-------------|
| 单 Region 上的版本、锁、提交语义 | 复制与选主 → Raft |
| Gateway 编排 Prewrite/Commit/Resolve | 完整跨 Region 2PC 硬化见 Store backlog（可演进） |
| 与用户 TTL 共存（读过滤） | Raw 平面语义 |

---

## 9. 验收（可读性 / 对齐源码）

| ID | 标准 |
|----|------|
| V1 | 能口述 L/D/W 各自干什么，以及读如何选版本 |
| V2 | 能区分 1PC 与 2PC，并指出 Raw 不走本模型 |
| V3 | `tests/storage/mvcc_test.cpp` 相关用例可通过：`./build/tests/openkv_unit_tests --gtest_filter='Mvcc*'` |

未跑 `-mode all`：本文为概念与源码对照文档，无改数据面热路径。

---

## 10. 口述稿

> Percolator 风格就是：TSO 给时间戳，MVCC 留多版本，写用 Prewrite 上锁再 Commit 写 Write，读按 start_ts 看已提交版本。  
> OpenKV 落成 L/D/W；单 key 短写收成 1PC，多 key 走 2PC；Raw 平面不参与这套。
