# Gateway TSO / Meta RPC 性能优化

日期：2026-11-10

## 问题

短 Put/PutCAS 路径原为：

`AllocTs → Prewrite → AllocTs → Commit`（4 次串行内部 RPC）

且每次 `GET_TS` / Meta `ExecLine` **新建并关闭 gRPC 连接**，强一致 Get 也要先 `AllocTs`。

## 改动

| 项 | 位置 | 效果 |
|---|---|---|
| Meta 连接池 | `client/go/internal/route/meta_pool.go` | Meta RPC 复用 `ClientConn` |
| TSO 批缓存 | `client/go/internal/route/tso.go` | 默认一次 `GET_TS 512`，本地发放；`OPENKV_TSO_BATCH` 可调（≤1 关闭缓存） |
| Put/Delete 双 TS | `cmd/compute-gateway/main.go` | `allocTs(2)` 一次取 start+commit，`commitWithTs` |
| 统一 alloc | Get / Scan / TTL / Txn* | 走 `s.allocTs` → 命中批缓存 |

热路径形态：

```text
Put/PutCAS:  [本地TS×2] → Prewrite → Commit     （Meta 热路径 0 跳；Storage 2 跳）
Get lin:     [本地TS] → TxnGet                    （Meta 0；Storage 1）
```

## 未做（下一刀）

- Storage propose 后 5ms 轮询 / `apply_timeout` → 回调等待  
- 单 key 非 Percolator 快速 PutCAS（1 次 Raft Propose）
