# Storage compact 风暴与 exit 139

**状态：** 已修复  
**相关：** [26-raftstore-schedule-pipeline](26-raftstore-schedule-pipeline.md) · [23-write-path-batching](23-write-path-batching.md)

## 问题

压测 `-mode all` 时 `storage-0` **SIGSEGV（exit 139）** 反复重启。崩溃前日志：

1. `shard=0 compacted to index=…` **每百毫秒刷屏**（空转 compact）
2. `RegionMetrics by_region_ bucket_count=0; rebuilding map…` 后立刻挂

## 根因

| 层 | 原因 |
|----|------|
| Compact | `maybe_compact` 用 `log_.size()`（含未 apply 尾部）作阈值；`report_applied` **每条**调用 → 高写入时阈值永真，空转 `compact_to` + `persist` + INFO |
| Metrics | 堆压力/损坏后 `bucket_count==0`；旧 `ensure_map_usable` **swap 再析构坏 map** → 二次 SEGV |

## 方案

1. Compact 门槛改为 **已 apply 仍在内存的前缀长度** `last_applied - first + 1`；`first` 未推进则不打 INFO。  
2. Apply 批处理：整批只 `report_applied(last)` 一次。  
3. Metrics：检测到坏 map 时 **泄漏旧实例**，换新表，禁止析构踩垃圾。

## 验收

| ID | 标准 |
|----|------|
| C1 | `ctest -R 'LatencyHist\|RaftLogTest.CompactTo'` 相关 PASS |
| C2 | 部署后 `-mode all`：storage **Restart 不增加**；INFO 无 compact 刷屏（成功 compact 改 DEBUG） |
| C3 | 不再出现 `bucket_count=0` 后立刻 exit 139 |

**2026-11-13 实测：** C1 PASS；C2 `Restart=0`，`-mode all` w8/8s exit=0；C3 无 `bucket_count=0`/exit 139。详见 [TEST_CASES](../test/TEST_CASES.md)。
