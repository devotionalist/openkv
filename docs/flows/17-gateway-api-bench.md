# Gateway 全接口压测

**状态：** 工具已落地  
**相关：** [01-write-put](01-write-put.md) · [23-write-path-batching](23-write-path-batching.md) · [TEST_CASES](../test/TEST_CASES.md)

## 问题

需要可复跑工具覆盖 Gateway **全部** gRPC 方法（KV / Txn / Watch / DS），输出 ops/s 与 p50/p95/p99，作为性能基线与回归对照。

## 方案

- 工具：`client/go/examples/bench`（`-mode all` 顺序跑各 API；`-list` 列模式 + **plane**）
- 集群内跑（避免本机 `kubectl port-forward` 断连）：交叉编译 `GOOS=linux GOARCH=amd64` → `kubectl cp` 到临时 Pod → `-addr gateway:19000`
- 短 Put = 本地 TSO + **ONEPC**（1 Propose）；seed **禁止**同 key 超时重试（否则 `ERR locked`）

### API 平面（SUMMARY 按 plane 分组）

| plane | 含义 | 模式 |
|-------|------|------|
| **txn** | MVCC 事务平面（TSO / lock / ONEPC） | `get` `put` `delete` `batch_*` `scan` `txn` `watch` … |
| **raw** | 无事务 Raw 平面（无 TSO/MVCC） | **仅** `rawput` `rawget` `rawdelete` |
| **ds** | 数据结构 RPC；底层仍走 MVCC Put/Get | `hset` `sadd` `lpush` `zadd` … |

无事务接口一律带 `raw` 前缀。`batch_put` / `batch_get` **不是** Raw，也不是「N key 一次 Raft Propose」：Gateway **扇出** ≤16 路单 key Put/Get，bench 的 ops/s 计**批次数**（batch=4 时 keys/s ≈ ops×4）。

## 怎么跑

Agent / 改完收尾：规则 `.cursor/rules/post-change-api-bench.mdc` + skill `.cursor/skills/openkv-api-bench/SKILL.md`。

```bash
# 一键（推荐）
./scripts/api-bench-all.sh

# 本机（需稳定 port-forward 到 gateway:19000）
(cd client/go && go build -o ../../.run/kv-bench ./examples/bench)
./.run/kv-bench -addr 127.0.0.1:19000 -mode all -workers 8 -duration 5s

# 集群内手工
(cd client/go && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ../../.run/kv-bench-linux ./examples/bench)
kubectl -n openkv run okv-bench --restart=Never --image=alpine:3.20 -- sleep 3600
kubectl -n openkv wait --for=condition=Ready pod/okv-bench
kubectl -n openkv cp .run/kv-bench-linux okv-bench:/kv-bench
kubectl -n openkv exec okv-bench -- chmod +x /kv-bench
kubectl -n openkv exec okv-bench -- /kv-bench \
  -addr gateway.openkv.svc.cluster.local:19000 \
  -mode all -workers 8 -duration 5s -warmup 500ms -keys 50
```

## 验收

| ID | 标准 |
|----|------|
| B1 | `-list` 含 get/put/txn/watch/hset/sadd/lpush/zadd 等 ≥20 模式 |
| B2 | `-mode all` 跑完打印 `SUMMARY` 表；进程 exit 0 |
| B3 | 读路径（get / get_stale）fail=0；写路径允许 fail（`apply_timeout` / `locked`）但 ok>0 |

## 基线结果

### 本地 RF=3（2026-11-15，写路径修复后）

环境：本机 daemon · Meta×3 / Storage×6 / Gateway×2 · RF=3 · `SKV_PERSIST_SYNC=none` · workers=8 · duration=5s  
修复：keepalive 10s/3s、GW2=`19002`、`persist_ready` 尊重 `none`、AE 回包 `ready_notify`。

| API | ops/s | p50 | ok | fail |
|-----|------:|----:|---:|-----:|
| get / get_stale | 5606 / 5311 | ~1.4ms | 高 | 0 |
| put / rawput | **416 / 1409** | **18.3ms / 4.9ms** | 2080 / 7046 | 0 |
| rawget / batch_get / ttl | 4627 / 2401 / 4727 | ~1.5–3ms | 高 | 0 |
| txn / txn_rollback | 112 / 747 | 64ms / 5.7ms | 560 / 3734 | 96 / 0 |
| watch | 1469 | 4.9ms | 7345 | 0 |

### 历史 k3s RF=1（2026-11-12）

Colima k3s · meta=1 / storage=1 / RF=1 · workers=8 · duration=5s：put/rawput 隔离 **227 / 344 ops/s · 7.8ms / 4.0ms**。

结论：写路径 Raw 非 CAS 跳过 latch 后 **rawput ≥ put**（见 [27](27-raw-vs-txn.md)）。

多 Region 压测：先 `bash scripts/bench-multi-region.sh bench-mr 4 200`，再 `examples/bench -prefix bench-mr …`；设计见 [23-write-path-batching](23-write-path-batching.md)。

## 失败与回滚

- seed/`put` 遇 `apply_timeout`：降 workers、拉长 `-op-timeout`，或重启 storage 清 backlog；勿对同 key 盲目重试
- 工具本身无服务端改动；删除临时 Pod 即可回滚运行痕迹
