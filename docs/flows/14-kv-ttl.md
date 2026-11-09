# 流程：KV TTL（TiKV RawKV 风格）

**状态：** 已实现  
**读者：** Client / Gateway / Storage  
**相关：** [01-write-put.md](01-write-put.md) · [02-read-get.md](02-read-get.md) · [13-version-cas.md](13-version-cas.md)

> **易混：** `LockInfo.ttl_ms` 是**事务锁租约**，不是用户 KV TTL。

对齐 [TiKV RawKV TTL](https://github.com/tikv/tikv/issues/9612)：

1. value 侧挂绝对 `expire_at_ms`（0=永不过期）；propose 前 stamp
2. 读路径过滤：过期 ⇒ 不可见（不保证立刻腾盘）
3. **TTL Checker**：Leader 定时扫描删除已过期 KV，再 `CompactRange` 回收  
   （上游 TiKV 用 CompactionFilter；本仓库 RocksDB 以 `-fno-rtti` 构建，无法挂 filter 子类，故用显式删除 + compact，语义等价）

**不做：** etcd Lease；DS 对象 TTL；依赖 compaction 才不可见（读过滤保证正确性）。

---

## 语义

| API | 行为 |
|-----|------|
| `Put` / `PutTTL`，`ttl_ms` | `0` 永久；`>0` 相对寿命，**propose 前**打成 `expire_at_ms = now + ttl` |
| `Get` / `TxnGet` / `Scan` | 过期 ⇒ 不可见（懒过期） |
| `TTL(key)` | 剩余 ms；不存在/过期 `-2`；永久 `-1`（`TXN_TTL`） |
| `Expire(key, ttl_ms)` | 读出 value 后写回并 stamp 新寿命；`ttl_ms<=0` 永久；不存在 `found=false` |
| `Persist(key)` | 等价 `Expire(key, 0)` |

线性一致：带 `linearizable` 的读走 Leader，与 TiKV「Leader 判定过期后它处不可读」同目标（副本落后读仍可能用本地钟，属 `linearizable=false` 语义）。

---

## 确定性

相对 `ttl_ms` → 绝对 `expire_at_ms` 只在 **propose 入口**（Gateway / StoreFacade / ExecLine）计算一次，写入 Command / Prewrite；`on_apply` 只落盘，各副本结果一致。

`Expire`/`Persist` 走与 `Put` 相同的 Prewrite+Commit（新 commit_ts = 新 revision）。

---

## 存储

### plain / 引擎层 `KvRecord`

```
revision(8) | expire_at_ms(8) | user_value     // 新格式：revision 最高位置 1
revision(8) | user_value                       // 旧格式：最高位 0 → expire=0
```

`put(key, value, rev, expire_at_ms=0)`；`get` 若 `expire_at>0 && now>=expire_at` → 空。

### MVCC

- `WriteInfo.expire_at_ms`；`LockInfo.user_expire_at_ms`（prewrite→commit 传递）
- `put_write` / `put_data` 同时把 `expire_at` 写入 `KvRecord` 头
- `txn_get` / `txn_ttl`：选中 write 后若已过期 ⇒ 当不存在
- 行协议：`TXN_TTL rid hex_key start_ts [lin]` → `OK TXN_TTL <ttl_ms>`

### 回收

| 机制 | 作用 |
|------|------|
| 读过滤 | 正确性 |
| `compact_for_ttl`（Leader 定时） | 扫描删过期 + CompactRange |

开关：`SKV_KV_TTL_COMPACT`（默认开）。

---

## 验收

```bash
cmake --build build -j --target openkv_unit_tests
./build/tests/openkv_unit_tests --gtest_filter='*Ttl*'
# 集群起好后：
go run ./client/go/examples/ttl
```

期望：`TTL` 码正确；`Expire`/`Persist` 改寿命；到期后 Get/TTL 不可见。
