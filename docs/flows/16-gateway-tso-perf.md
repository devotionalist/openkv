# Gateway TSO / Meta RPC 性能优化

日期：2026-11-10（2026-11-12：区间队列 + 低水位预取）

## 问题

短 Put/PutCAS 路径原为：

`AllocTs → Prewrite → AllocTs → Commit`（4 次串行内部 RPC）

且每次 `GET_TS` / Meta `ExecLine` **新建并关闭 gRPC 连接**，强一致 Get 也要先 `AllocTs`。

## 改动

| 项 | 位置 | 效果 |
|---|---|---|
| TSO 无锁区间环 | `ts_range_queue.hpp` + `tso.*` | 固定环 MPMC：热路径 **CAS try_take**（无 mutex）；补货 **push**；默认 GetTs **5000**；剩余 **&lt;2000** 异步预取；miss 才锁 waiters |
| 环境变量 | `OPENKV_TSO_BATCH` / `OPENKV_TSO_LOW_WATERMARK` | 批 ≤1 关闭缓存 |
| Put/Delete 双 TS | Gateway | `alloc_async(2)` 一次取 start+commit |

```text
          ┌─ Meta GetTs(5000) ─┐
          │                    ▼
热路径 ──►│ 无锁环 TsRangeQueue │──► start_ts / commit_ts
 CAS take │  atomic remaining   │     push（预取 / miss）
          └─ 剩余 < 2000 触发 ─┘
```

热路径无锁；仅硬 miss 的 waiter 列表用 `mutex`。

## 验收

| ID | 命令 / 期望 |
|----|-------------|
| T1 | `ctest --test-dir build -R TsoTest --output-on-failure` → PASS |
| T2 | 默认 batch=5000、watermark=2000；`QueueDrainOrder` 按队序发放 |

## 未做（下一刀）

- Storage propose 后 5ms 轮询 / `apply_timeout` → 回调等待  
