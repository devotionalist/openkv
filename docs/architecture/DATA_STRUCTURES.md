# 常用数据结构（非 RESP）

OpsKV 在 **gRPC `kv.v1.DSService`** 上提供 Hash / Set / List / ZSet，语义对齐 Redis 常用命令子集。  
**不做** Redis 线协议（RESP）、PubSub、Stream、Lua、Cluster `MOVED`。

相关流程：[flows/12-data-structures.md](../flows/12-data-structures.md)

---

## 1. 目标与非目标

| 做 | 不做 |
|----|------|
| Gateway gRPC DS API + Go SDK | redis-cli / RESP |
| Hash → Set → List → ZSet（分期） | 完整 Redis 命令面 |
| 与普通 `Get`/`Put` 命名空间隔离 | 跨 Region 单对象（见路由） |
| 短事务写（同 Put） | 一期 TTL（meta 预留字段） |

---

## 2. 物理编码（v1）

### 2.1 Key

```
MetaKey(ukey) = ukey + "\x00\x01ds\x00"
```

- **同一 ukey 仅一种 DS 类型**（类型字节在 blob 内：`h`/`s`/`l`/`z`）；错类型 → `FailedPrecondition` / `wrong_type`
- **ukey 作前缀**，使 `Locate(MetaKey)` 与逻辑名落入同一 Range 前缀带
- 普通用户 `Put(ukey)` 与 `MetaKey(ukey)` 不同物理键，互不覆盖

### 2.2 Value（v1：单对象 blob）

整表存在 MetaKey 的 value 内（自描述二进制，见 `client/go/internal/dsencode`）：

| 类型 | Value 内容 |
|------|------------|
| Hash | field → value 映射 |
| Set | member 集合 |
| List | 有序元素数组 |
| ZSet | member → score；rank 序由 score+member 排序得到 |

> **v2（规划）**：元素子键 `MetaKey + "\x00" + sub` + MVCC 感知 `SCAN_PREFIX`，大对象拆分；split 禁止切开对象前缀。

### 2.3 路由与 Split

- Gateway 对 DS 操作一律 `Locate(MetaKey(ukey,t))`
- **约束**：尽量避免在「某 ukey 前缀与 `ukey\x00\x01ds…` 之间」人工 Split；调度/文档要求大对象慎用
- 一期集成测试默认单 Region 或整 ukey 落在同一 Region

---

## 3. 命令表（已实现子集）

### Hash

| RPC | 语义 |
|-----|------|
| HSet | 设置 field；新建返回 added |
| HGet | 取 field |
| HDel | 删 field |
| HExists | field 是否存在 |
| HLen | field 个数 |
| HGetAll | 全部 field-value |
| HKeys | 全部 field |
| HMSet / HMGet | 批量 |

### Set

| RPC | 语义 |
|-----|------|
| SAdd / SRem | 加/删 member |
| SIsMember | 是否成员 |
| SCard | 基数 |
| SMembers | 全部 member |

### List

| RPC | 语义 |
|-----|------|
| LPush / RPush | 头/尾插入 |
| LPop / RPop | 头/尾弹出 |
| LLen | 长度 |
| LRange | `[start, stop]`（同 Redis 负下标） |
| LIndex | 按下标取（可选） |

### ZSet

| RPC | 语义 |
|-----|------|
| ZAdd / ZRem | 加/删 |
| ZScore | 查 score |
| ZCard | 基数 |
| ZRange | by rank `[start, stop]` |

---

## 4. 错误码

全节点 token 目录：[ERROR_CODES.md](ERROR_CODES.md)（含 Storage / Meta / Gateway 重试策略）。DS 对外：

| 情况 | gRPC status |
|------|-------------|
| ukey / 参数为空 | `InvalidArgument` |
| Meta 存在但类型不符 | `FailedPrecondition`（`wrong_type`） |
| blob 损坏 | `DataLoss`（`corrupt_ds_blob`） |
| 存储/事务失败 | `Internal` / 透传信息 |

---

## 5. 一致性

- **写**：AllocTs → Prewrite(MetaKey) → Commit（与 `Put` 相同短事务）
- **读**：默认线性一致（TxnGet + TSO）；与 `Get` 对齐

---

## 6. 验收

| 期 | 命令 | 自动化 |
|----|------|--------|
| Hash | 见上 | `go test ./internal/dsencode` · `scripts/it-ds-hash.sh` |
| Set | 见上 | `scripts/it-ds-set.sh` |
| List | 见上 | `scripts/it-ds-list.sh` |
| ZSet | 见上 | `scripts/it-ds-zset.sh` |

登记：[TEST_CASES.md](../test/TEST_CASES.md)
