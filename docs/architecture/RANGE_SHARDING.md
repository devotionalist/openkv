# Range 分片（有序区间 / FDB / HBase 类）

OpsKV 使用 **有序 Range + 分裂/合并**（已从早期哈希取模切换）。总览见 [ARCHITECTURE.md](ARCHITECTURE.md)；流程见 [flows/09-range-split-merge.md](../flows/09-range-split-merge.md)、[flows/10-region-placement.md](../flows/10-region-placement.md)；**逐步详解**见 [REGION_OPS.md](../ops/REGION_OPS.md)。

## 模型

| 概念 | 说明 |
|------|------|
| Region | 半开区间 `[start, end)`；`end` 空 = +∞ |
| 路由 | `locate(key)` → `region_id`（Meta / Compute / Gateway） |
| 复制 | 每个 Region 一组 Raft（RF=3） |
| 分裂 | Meta `SPLIT_REGION`；Storage `ADMIN_SPLIT` **只改 meta + 同机共享引擎**（不 `migrate_ge`） |
| 合并 | 共享引擎则只扩区间；否则搬数据；Meta `MERGE_REGIONS` |
| 放置 | `skv place`：跨机迁副本（Learner/Snapshot）；这才是腾盘/打散 |

初始：**1 个 Region 0** 覆盖全 key 空间（覆盖全 key 空间）。

## 协议

```
GET_ROUTE  → OK ROUTE ver n n rid:startHex:endHex:nrep,node@addr*L,...
LOCATE <hex_key> → OK LOCATE <region_id>
SPLIT_REGION <rid> <hex_split_key> → OK SPLIT <left> <right>
MERGE_REGIONS <left> <right> → OK MERGE <left> <right>
SUGGEST_PLACE [avoid=id,id] → OK SUGGEST n id...
FIND_MERGE_RIGHT <left> → OK MERGE_RIGHT <left> <right>
PLACE_REGION <rid> <id,id,...> → OK PLACE ...
ADMIN_SPLIT / ADMIN_MERGE / ADMIN_ENSURE_REGION / ADMIN_ADD_LEARNER /
ADMIN_PEER_PROGRESS / ADMIN_PROMOTE / ADMIN_TRANSFER_LEADER / ADMIN_CAMPAIGN /
ADMIN_REMOVE_PEER / ADMIN_DESTROY_REGION
```

## 自动分裂 / 合并 / 打散提示

由 Storage Leader **tick 线程**周期性检查（不在 apply 路径）：

| 环境变量 | 含义 | 默认 |
|----------|------|------|
| `SKV_SPLIT_KEYS` | key 数 ≥ 阈值则分裂 | `0`（关闭） |
| `SKV_SPLIT_BYTES` | 近似字节数 ≥ 阈值则分裂 | `0`（关闭） |
| `SKV_MERGE_KEYS` | 两侧 key 数都 < 阈值则合并相邻 | `0`（关闭） |

Split **不**自动 place。无感扩容：`join` → `balance`；手动打散：`skv place`。

## 运维（skv）

```bash
./build/tools/skv locate mykey
./build/tools/skv split 0 m              # 仅切区间
./build/tools/skv place 1 storage-3,storage-4,storage-5  # 需要时再迁右片
./build/tools/skv merge 0 1
./build/tools/skv nodes storage
./build/tools/skv route
```

## Region 放置（多 Store）

本地默认 **6 个 Storage**，`replica_factor=3`：开机 Region 0 只在 **前 3 个**；3/4/5 不预建 Region。

`skv place` 流程：

1. 缺失副本：`ENSURE` → `ADD_LEARNER` → **轮询 `PEER_PROGRESS` 至 caught_up** → `PROMOTE`
2. 若当前 Leader 不在目标集：`TRANSFER_LEADER` → 目标 `CAMPAIGN`
3. 多余副本：`REMOVE_PEER` → `DESTROY_REGION`
4. Meta `PLACE_REGION` 发布路由

也可直接：`ADMIN_PLACE_REGION <rid> <id,id,...>`（任一可达 Storage 会跟 Leader 编排）。

## 近期补齐

- 分裂流量暂停：`ADMIN_PREPARE_SPLIT` → `ADMIN_SPLIT`（见 [flows/09-range-split-merge.md](../flows/09-range-split-merge.md)）
- 事务 GC：`SKV_TXN_GC`（默认开）过期锁 rollback + 按 Meta TSO `GET_SAFE_POINT` 做 MVCC GC（floor 保留）
- Gateway `Get/Put/Delete` 已与 Txn MVCC 可见性统一

事务细节见 [TRANSACTION_SURVEY.md](TRANSACTION_SURVEY.md)。
