# 流程：Range 分裂 / 合并

**背景：** 有序 key 空间上的 Region（半开区间），每 Region 一组 Raft。见 [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md)。

**逐步操作与后端细节：** [REGION_OPS.md](../ops/REGION_OPS.md)（Split / Merge / Place / Scatter 全流程）。

## 分裂 Split

```mermaid
flowchart TD
  A[Leader 检测超阈值<br/>keys 或 bytes] --> B[pick_split_key 中位]
  B --> C[Meta SPLIT_REGION<br/>Raft 复制路由]
  C --> D[左 end=split<br/>右 start=split 新 region_id]
  D --> E[广播 ADMIN_PREPARE_SPLIT<br/>left 写暂停]
  E --> F[广播 ADMIN_SPLIT<br/>share engine + 改区间]
  F --> G[解除 pause + bump epoch]
  G --> H[Gateway 拉 GET_ROUTE<br/>遇 splitting 重试]
```

**流量窗口：** `PREPARE`→`SPLIT` 仅覆盖元数据 Raft apply（同机**不搬 KV**，）；Gateway 遇 `splitting` / `epoch_not_match` 刷路由重试。跨机打散见 Place/Scatter。

手动：`skv split <rid> <key>`（内部先 PREPARE 再 SPLIT）。详解 [REGION_OPS.md](../ops/REGION_OPS.md)。

## 合并 Merge

```mermaid
flowchart TD
  A[skv merge left right] --> B[各副本 ADMIN_MERGE<br/>right 全量 → left]
  B --> C[Meta MERGE_REGIONS]
  C --> D{left.end == right.start?}
  D -->|否| E[失败]
  D -->|是| F[删除 right<br/>left.end = right.end]
  F --> G[bump route_ver]
```

要求：**相邻**区间（`left.end_key == right.start_key`）。
