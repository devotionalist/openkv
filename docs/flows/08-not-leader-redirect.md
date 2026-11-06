# 流程：not_leader 重定向

**目的：** 写/线性读打到 Follower 时，引导客户端（Gateway）改打当前 Leader，避免错写或脏读。

**错误码总目录：** [ERROR_CODES.md](../architecture/ERROR_CODES.md)。

**关键代码：** `StoreFacade::{propose,get}` · `storagetcp.NotLeaderError` · Gateway `rememberLeader` / `tryOrder` ·（可选）C++ `KvProxy`

```mermaid
flowchart TD
  A[Gateway 选副本<br/>tryOrder: hint → shard%N → 全量] --> B[发 PUT 或 GET]
  B --> C[Storage StoreFacade]
  C --> D{本机该 shard<br/>是 Leader?}
  D -->|是| E[正常 propose / get]
  D -->|否| F[拼 redirect_to<br/>leader_address 或 peer_address]
  F --> G[ERR not_leader redirect=host:port]
  G --> H[Gateway 解析 NotLeaderError]
  H --> I[rememberLeader shard → redirect]
  I --> J{redirect 在已知<br/>storages 列表?}
  J -->|是| K[立即改打该地址重试]
  J -->|否| L[换 tryOrder 下一副本]
  K --> B
  L --> B
  E --> M[OK 回客户端]
```

## 各层错误形态

| 层 | 形态 |
|----|------|
| Storage TCP | `ERR not_leader redirect=127.0.0.1:9100` |
| Meta TCP | `ERR not_leader redirect=<meta_leader>` |
| Go `storagetcp` | `NotLeaderError{Redirect}` |
| Gateway | 更新 `leaderHint[shard]` 后重试 |

## 与读路径交叉

```mermaid
flowchart LR
  W[写 PUT] --> NL[非 Leader → redirect]
  R[线性读 GET] --> NL
  NL --> GW[Gateway 缓存 Leader]
  GW --> L[打到正确 Leader]
```

Follower 本地 RocksDB **不会**在线性读路径被返回给 Gateway（`linearizable=true`）。
