# 流程：写入 Put

**路径：** Client → gRPC Gateway → Range locate → Storage TCP → Region Raft Leader → 多数派提交 → apply → RocksDB

**关键代码：** `client/go/kv` · `compute-gateway` · `internal/route` · `storagetcp` · `ExecLineHandler` · `StoreFacade::propose` · `RaftNode::propose` · `StorageNode::on_apply` · `KvEngine::put`

**多数派：** RF=3 → 需 ≥2 副本 `match_index` 推进 `commit_index`

```mermaid
flowchart TD
  A[Go Client<br/>kv.Client.Put] -->|gRPC KVService.Put| B[compute-gateway<br/>server.Put]
  B --> C[route.Locate key<br/>tryOrder region]
  C --> D[storagetcp.Client.Put<br/>PUT region hexKey hexVal]
  D --> E[ExecLineHandler::handle_line]
  E --> F[StoreFacade::propose]
  F --> G{本机是<br/>该 Region Leader?}
  G -->|否| H[ERR not_leader<br/>redirect=leader_addr]
  H --> R[Gateway 重试<br/>见 08-not-leader-redirect]
  G -->|是| I[encode_command<br/>RaftNode::propose]
  I --> J[log.append + persist_locked<br/>raft_shard_N/]
  J --> K[broadcast_heartbeat<br/>RAFT_AE → Follower]
  K --> L[Follower on_append_entries<br/>写日志 + 回 OK AE]
  L --> M[Leader on_append_entries_reply<br/>更新 match_index]
  M --> N{match_index 多数派<br/>≥ majority=2?}
  N -->|否| K
  N -->|是| O[maybe_commit<br/>推进 commit_index]
  O --> P[apply_committed<br/>StorageNode::on_apply]
  P --> Q[KvEngine::put<br/>RocksDB shard_N]
  Q --> S[轮询 last_applied ≥ idx]
  S --> T[OK PUT revision]
  T --> U[Gateway → Client<br/>gRPC PutResponse]
```

> 磁盘目录名仍可能带 `shard_<id>`（与 `region_id` 同号）；路由语义是 Range Region，见 [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md)。

## 落盘边界

| 层 | 目录 / 内容 |
|----|-------------|
| Raft WAL | `{data_dir}/raft_shard_<id>/hard.state` + `log.entries` |
| KV | `{data_dir}/shard_<id>/` RocksDB |

## 时序要点

1. `propose` 后同步轮询 `soft_state.last_applied`（demo 模型，非异步回调）。
2. 空心跳 / 写触发的 AE 都会推进复制。
3. 仅 **当前 term** 的日志条目可被 `maybe_commit` 提交。
