# OpsKV Web 管控模块

独立于 Meta / Storage / Gateway / Client 的 **Web 运维进程**，默认监听 `:19100`。

```
web/                    ← 本模块（独立 go.mod）
  main.go               进程入口（embed static/）
  frontend/             TypeScript 源码（esbuild → static/）
    src/*.ts
    index.html / style.css
  static/               构建产物（Go embed）
  internal/
    api/                HTTP JSON API
    cluster/            Meta / Storage **gRPC** 客户端（含 ExecLine）
```

KV 调试经 Gateway gRPC，依赖同仓库 `client/go`（`replace`）。网络总览见 [docs/NETWORKING.md](../docs/NETWORKING.md)。

## 前端（TypeScript）

```bash
cd web/frontend
npm install
npm run build          # → ../static/
npm run watch          # 开发时热编
npm run typecheck
```

## 启动

```bash
# 先起集群
make cluster-up

# 编前端 + Go 二进制并运行
make admin-run
# 或
cd web/frontend && npm run build
cd web && go run . -run-dir ../.run -root ..
```

浏览器：**http://127.0.0.1:19100**（Meta/Storage 均为 gRPC ExecLine）。

```bash
SKV_ADMIN=1 make cluster-up   # 集群启动时一并拉起
```

## 标志

| 标志 | 默认 | 说明 |
|------|------|------|
| `-listen` | `0.0.0.0:19100` | HTTP |
| `-metas` | `SKV_META` / 8500–8502 | Meta TCP |
| `-storages` | `SKV_STORAGES` / 9100–9102 | Storage TCP |
| `-gateway` | `SKV_GATEWAY` / 19000 | Gateway gRPC |
| `-run-dir` | `SKV_RUN` | `.run` 进程面板 |

详见 [docs/USER_GUIDE.md](../docs/USER_GUIDE.md)（管控台与运维说明）。
