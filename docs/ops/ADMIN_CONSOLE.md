# OpsKV Web 管控模块

独立进程 `web/`（非 Gateway、非 Client SDK），可视化运维。

## 目录

```
web/                     # 独立 Go module
  main.go
  frontend/              # TypeScript（esbuild → static/）
  static/                # 构建产物，Go embed
  internal/api/          # REST
  internal/cluster/      # Meta/Storage gRPC ExecLine
```

## 启动

```bash
make cluster-up
make admin-run                 # npm build 前端 + go build → http://127.0.0.1:19100
# 或随集群拉起
SKV_ADMIN=1 make cluster-up
```

前端开发：`cd web/frontend && npm run watch`。

## 页面

| Tab | 内容 |
|-----|------|
| 总览 | **拓扑图**（Meta / Gateway / Storage）+ 节点上线/下线/重启 + TSO/GC |
| 运维 | Region 主表（行内 Split/Merge/Place，展开看副本）+ 待加入 Store + Balance |
| Console | Gateway Put/Get、Meta Locate，以及 Hash / Set / List / ZSet（`POST /api/ds`） |

节点上下线需 `-run-dir` + `-root`（`make admin-run` 已带）。API：`POST /api/node/{start,stop,restart}` body `{"name":"storage-1"}`。

Store 生命周期：`REGISTER` → **Joining（待加入）** → `join`/`JOIN_NODE` → **Active** → `scale-in` → Draining → Tombstone → `remove-tombstone`；已 Active 用 `balance` 做 RF 恒定 Replace。

## API

| 方法 | 路径 |
|------|------|
| GET | `/api/overview` `/api/route` `/api/nodes` `/api/ping` `/api/safe-point` |
| GET | `/api/scale-in-status?node_id=` |
| POST | `/api/split`（仅切区间）`/api/merge` `/api/place` `/api/add-replica` `/api/advance-safe` `/api/kv/put` `/api/ds` |
| POST | `/api/join-node` `/api/scale-in` `/api/remove-tombstone` `/api/balance` |
| GET | `/api/kv/get?key=` `/api/locate?key=` `/api/suggest-place` `/api/find-merge-right` `/api/processes` |
