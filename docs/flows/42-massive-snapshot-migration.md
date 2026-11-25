# 海量数据迁移 / 大快照问题（已知限制）

**状态：** 问题记录 · 方案未落地  
**相关：** [04-fault-recovery](04-fault-recovery.md) · [DATA_PLANE_HARDENING](DATA_PLANE_HARDENING.md) · [20-next-optimizations](20-next-optimizations.md) N9

---

## 1. 问题一句话

落后副本一旦掉出日志窗口，就要走 **InstallSnapshot**：当前实现是把 Region 的 KV **逻辑扫出来打成一个大 blob** 过网再灌库。Region 一大，内存、带宽、对端 apply 都会顶穿——这就是「海量数据迁移」在 OpenKV 里的痛点。

---

## 2. 现状两条「快照」别混

| | Raft 追赶（InstallSnapshot） | 本地 Checkpoint（E3） |
|---|---|---|
| 入口 | `KvEngine::create_snapshot(range)` → SnapWorker → gRPC 分片 | `KvEngine::create_checkpoint` |
| 形态 | 迭代 RocksDB，**整段 key/value 编码进 `string`** | RocksDB Checkpoint，**硬链 SST**，几乎不拷数据 |
| 体积 | ≈ 该 Region 用户数据（再加区间头 / ConfState） | 目录看起来大，盘上多是硬链 |
| 用途 | 过网给落后 peer / 扩容拉齐 | 本机一致点、备份 |
| 接到 InstallSnapshot？ | ✅ 这条 | ❌ **没有** |

打本地 FSM 文件（`fsm_<gid>.snap`）是协议侧另一条线，和 RocksDB 大文件无关；见 [20 N1b](20-next-optimizations.md#n1b本地-fsm-快照多份保留待重新设计)。

---

## 3. 为何会「很大」

```text
Leader: next_index < first_index（日志已 compact）
  → Ready 外提 SnapshotMeta
  → SnapWorker：Provider = create_snapshot(range)   ← 整表扫进内存
  → Sender（Store 内拼 term）→ InstallSnapshot → transport_fn_
  → gRPC SnapshotChunk（默认 16KB 一片）
Follower: SnapStreamBuffer 拼齐 → apply_snapshot → compact 日志
```

痛点拆开看：

1. **生成侧内存峰值**：`create_snapshot` 一次攒满 Region；共享引擎虽可按 `KeyRange` 裁切，单个 Region 仍可到 GB 级。  
2. **发送侧**：blob 进 SnapWorker 队列；同 Region 合并/重试挡的是「任务风暴」，挡不住「单包巨大」。  
3. **网络**：分片只降低单帧大小，总字节量仍是全量。  
4. **接收侧**：`apply_snapshot` 有界 range 先删区间再写回；吞吐与写放大都差，且长时间占 apply/引擎。  
5. **与 Checkpoint 脱节**：本机已有廉价 SST 硬链能力，**追赶路径没用上**，等于自己把「文件级迁移」退化成「逻辑 dump」。

扩容、宕机追赶、learner 加入，只要掉出 AE 窗口，都会撞上同一条路。

---

## 4. 现有缓解（治标）

| 手段 | 作用 | 局限 |
|------|------|------|
| `compact_threshold` / 及时 AE | 少触发 InstallSnapshot | 落后过久仍必走快照 |
| `snapshot_min_interval_ms` | 同 peer 限发频率 | 单次仍可能巨大 |
| SnapWorker 合并 / 重试 / QoS | 队列不爆、失败可再试 | 不缩小单次体积 |
| Region 切分（range 变小） | 单次导出变小 | 运维/调度成本；已很大的 Region 仍要先迁一次 |
| gRPC chunk | 避免单帧撑爆 | 总流量不变 |

---

## 5. 期望方向（后边做，本轮只记问题）

按代价从低到高（讨论用，未排期实现）：

1. **流式导出**：`create_snapshot` 改为边扫边写临时文件 / 边 chunk，避免整 Region 进一个 `string`。  
2. **SST / ingest 迁移**：Leader 对区间做 flush + 导出（或基于 Checkpoint 裁剪），Follower `IngestExternalFile`；协议仍带 `last_index/term` + ConfState。  
3. **把 Checkpoint 接到追赶**：本机一致点与过网载体统一，少走一份逻辑 dump。  
4. **调度侧**：大 Region 先 split 再拉副本；learner 带限速与优先级，避免打满集群带宽。

边界要守住：共享引擎下禁止「空 range 整库清空」；ConfState / epoch 与数据一起对齐；失败可重试且不破坏 Leader 主路径。

---

## 6. 代码锚点

| 环节 | 位置 |
|------|------|
| 触发发快照 | `raft/src/core/replication.cpp` `broadcast_replicate`（`next_index < first_index`） |
| 出站队列 | `raftstore/snap_worker.*` · `Store::dispatch_ready_after_persist` |
| 逻辑导出 / 灌库 | `storage/src/kv_engine.cpp` `create_snapshot` / `apply_snapshot` |
| 本机 Checkpoint | 同文件 `create_checkpoint` / `restore_from_checkpoint` |
| 入站拼包 | `raft/util/snap_stream.hpp` · `StorageNode::handle_raft_snap_chunk` |
| 出站接线 | Store SnapWorker::Sender 拼 `InstallSnapshot`；Storage 只 `transport.bind`（见 [54](54-storage-raft-boundary.md)） |

---

## 7. 验收（落地某方案后补；当前仅登记）

| ID | 期望（方案落地后） |
|----|-------------------|
| M1 | 单 Region 数据量 ≥ 设定阈值时，InstallSnapshot **峰值 RSS** 不明显随全量线性涨（流式或 SST） |
| M2 | 追赶成功：Follower `match` 对齐，读写一致；失败可重试且 Leader 不堵死 AE |
| M3 | 现有门禁：`SnapWorker*` / 快照相关 IT / `api-bench-all` 主路径无回归 |

当前：**未实现优化**；本文只作问题与方向备忘，避免以后再把 Checkpoint 与 InstallSnapshot 混为一谈。
