# Gateway 全接口压测

**状态：** 工具已落地  
**相关：** [01-write-put](01-write-put.md) · [23-write-path-batching](23-write-path-batching.md) · [TEST_CASES](../test/TEST_CASES.md)

## 问题

需要可复跑工具覆盖 Gateway **全部** gRPC 方法（KV / Txn / Watch / DS），输出 ops/s 与 p50/p95/p99，作为性能基线与回归对照。

## 方案

- 工具：`client/go/examples/bench`（`-mode all` 顺序跑各 API；`-list` 列模式）
- 集群内跑（避免本机 `kubectl port-forward` 断连）：交叉编译 `GOOS=linux GOARCH=amd64` → `kubectl cp` 到临时 Pod → `-addr gateway:19000`
- 短 Put = 本地 TSO + **ONEPC**（1 Propose）；seed **禁止**同 key 超时重试（否则 `ERR locked`）

## 怎么跑

```bash
# 本机（需稳定 port-forward 到 gateway:19000）
(cd client/go && go build -o ../../.run/kv-bench ./examples/bench)
./.run/kv-bench -addr 127.0.0.1:19000 -mode all -workers 8 -duration 5s

# 集群内（推荐）
(cd client/go && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ../../.run/kv-bench-linux ./examples/bench)
kubectl -n openkv run okv-bench --restart=Never --image=alpine:3.20 -- sleep 3600
kubectl -n openkv wait --for=condition=Ready pod/okv-bench
kubectl -n openkv cp .run/kv-bench-linux okv-bench:/kv-bench
kubectl -n openkv exec okv-bench -- chmod +x /kv-bench
kubectl -n openkv exec okv-bench -- /kv-bench \
  -addr gateway.openkv.svc.cluster.local:19000 \
  -mode all -workers 8 -duration 5s -warmup 500ms -keys 50
kubectl -n openkv delete pod okv-bench
```

## 验收

| ID | 标准 |
|----|------|
| B1 | `-list` 含 get/put/txn/watch/hset/sadd/lpush/zadd 等 ≥20 模式 |
| B2 | `-mode all` 跑完打印 `SUMMARY` 表；进程 exit 0 |
| B3 | 读路径（get / get_stale）fail=0；写路径允许 fail（`apply_timeout` / `locked`）但 ok>0 |

## 基线结果（2026-11-10）

环境：Colima k3s · meta=1 / storage=1 / RF=1 · workers=8 · duration=5s · keys≈26（seed 中途 `apply_timeout`）· vsize=64

| API | ops/s | p50 | p99 | ok | fail |
|-----|------:|----:|----:|---:|-----:|
| get | 429 | 14ms | 149ms | 2147 | 0 |
| get_stale | 514 | 14ms | 49ms | 2568 | 0 |
| put | 6.4 | 353ms | 574ms | 32 | 138 |
| put_ttl | 19 | 323ms | 1.1s | 97 | 73 |
| batch_put | 0.4 | 937ms | 937ms | 2 | 87 |
| batch_get | 99 | 50ms | 413ms | 496 | 0 |
| scan | 31 | 439ms | 519ms | 156 | 0 |
| txn | 8.8 | 412ms | 858ms | 44 | 30 |
| watch | 18 | 447ms | 463ms | 88 | 0 |
| lpush / zadd | 22 / 25 | ~300ms | ~600ms | 109 / 126 | 0 |
| hset / sadd | 2.2 / 0.2 | — | — | 低 | 高 |

结论：线性读尚可（~400–500 ops/s）；写路径受单 Region Raft apply + 事务写路径限制，高并发下大量 `apply_timeout`/`locked`。与 [16-gateway-tso-perf.md](16-gateway-tso-perf.md)「下一刀：apply 回调等待」一致。

多 Region 压测：先 `bash scripts/bench-multi-region.sh bench-mr 4 200`，再 `examples/bench -prefix bench-mr …`；设计见 [23-write-path-batching](23-write-path-batching.md)。

## 失败与回滚

- seed/`put` 遇 `apply_timeout`：降 workers、拉长 `-op-timeout`，或重启 storage 清 backlog；勿对同 key 盲目重试
- 工具本身无服务端改动；删除临时 Pod 即可回滚运行痕迹
