# OpsKV 用户使用手册

OpsKV 是基于 **Multi-Raft + RocksDB + gRPC** 的存算分离分布式 KV：Meta / Storage（C++）+ Gateway / 客户端 / Web 管控台（Go）。

本文说明如何**安装、访问、读写与扩容**。架构 / 流程图 / SOP / 测试见 [README.md](./README.md) 分类索引；网络协议见 [NETWORKING.md](./NETWORKING.md)。

---

## 1. 组件与端口

| 组件 | 作用 | 默认端口 | 协议 |
|------|------|----------|------|
| Meta ×3 | 路由、拓扑、调度 | 8500 | gRPC `MetaService` |
| Storage ×N | 数据 + Multi-Raft | 9100（集群内同端口、不同 Pod） | gRPC Store / Raft / Admin |
| Gateway | 无状态计算入口（客户端连这里） | 19000 | gRPC `KVService` / `DSService` |
| Web 管控台 | 拓扑 / 运维 / Console | 19100 | HTTP（后端再调 gRPC） |

```
Client / skv / Web Console
        │ gRPC
        ▼
   Gateway :19000  ──gRPC──▶  Meta ×3
        │ gRPC
        ▼
   Storage ×N（RF=3；节点间 Raft 亦为 gRPC）
```

编译默认**不需要** Seastar；**必须**能编过 gRPC C++（系统库或 CMake FetchContent）。

---

## 2. 本地快速体验

### 2.1 编译与一键集群

```bash
make                 # 配置 + 编译 + 单测
make cluster-up      # 拉起本地集群（Gateway 默认 :19000）
make admin-run       # 管控台 http://127.0.0.1:19100
# 或：SKV_ADMIN=1 make cluster-up
```

停集群：`make cluster-down`。

### 2.2 读写

```bash
./.run/kv-example 127.0.0.1:19000

# 或 skv
cmake --build build -j --target skv
./build/tools/skv put hello world
./build/tools/skv get hello
./build/tools/skv locate hello
./build/tools/skv route
```

Go SDK：

```bash
cd client/go && go mod tidy
go run ./examples/basic 127.0.0.1:19000
```

### 2.3 管控台（本地）

浏览器打开 **http://127.0.0.1:19100/**

![管控台总览（拓扑）](images/console-overview.png)

| 页签 | 用途 |
|------|------|
| **总览** | 集群拓扑、节点在线状态、流量 |
| **运维** | Region 分裂/合并、**待加入节点**、Balance |
| **Console** | 经 Gateway 的 Put/Get/Locate 与 Hash/Set/List/ZSet |

本地进程启停需要 Web 带 `-run-dir`（`make admin-run` 已配置）。Kubernetes 部署一般**不**配 `-run-dir`，进程由集群管理。

---

## 3. Kubernetes / Helm 部署

Chart：[`deploy/helm/opskv`](../deploy/helm/opskv)。默认 **Meta×3 + Storage×3 + Gateway×1 + Web**。

### 3.1 构建并导入镜像（Colima / k3s）

k3s 使用 containerd，需把 Docker 镜像导入后再装：

```bash
docker build -f deploy/Dockerfile -t opskv:dev .
docker build -f deploy/Dockerfile.gateway -t opskv-gateway:dev .
docker build -f deploy/Dockerfile.web -t opskv-web:dev .

docker save opskv:dev         | colima ssh -- sudo k3s ctr images import -
docker save opskv-gateway:dev | colima ssh -- sudo k3s ctr images import -
docker save opskv-web:dev     | colima ssh -- sudo k3s ctr images import -
```

### 3.2 安装

```bash
helm upgrade --install opskv deploy/helm/opskv -n opskv --create-namespace \
  --set image.pullPolicy=Never \
  --set gatewayImage.pullPolicy=Never \
  --set web.image.pullPolicy=Never

kubectl -n opskv get pods
```

期望 Meta / Storage / Gateway / Web 均为 Ready。

### 3.3 访问 Web 与 Gateway

- **Web**：默认 `service.web.type=LoadBalancer`。Colima 下通常可直接打开  
  **http://127.0.0.1:19100/**  
  （若不通：`kubectl -n opskv port-forward svc/web 19100:19100`）
- **Gateway（客户端）**：

```bash
kubectl -n opskv port-forward svc/gateway 19000:19000
./.run/kv-example 127.0.0.1:19000
```

### 3.4 卸载

```bash
helm uninstall opskv -n opskv
# PVC 默认保留；彻底清盘需手动删除 PVC
```

---

## 4. 扩容：加节点 ≠ 加入集群

设计总览：[DATA_SCALE.md](./architecture/DATA_SCALE.md)。

**务必分清两步：**

| 阶段 | 做什么 | 谁做 | 结果 |
|------|--------|------|------|
| **A. 增加节点** | 拉起新 Pod | Helm | Store 为 **Joining（待加入）**，尚未调度、无副本 |
| **B. 加入并打散** | 准入 + 迁 Region | **运维页人工** | **Active** → **Balance**（RF 不变） |

Helm 把 `replicaCount.storage` 加大，**不会**自动 join / balance。

### 4.1 加 Gateway（无状态）

```bash
helm upgrade opskv deploy/helm/opskv -n opskv --reuse-values \
  --set replicaCount.gateway=2 \
  --set image.pullPolicy=Never \
  --set gatewayImage.pullPolicy=Never \
  --set web.image.pullPolicy=Never
```

无需「加入」操作；`svc/gateway` 负载到多个 Pod。

### 4.2 加 Storage（示例：3 → 6）

**A — 只加节点**

```bash
helm upgrade opskv deploy/helm/opskv -n opskv --reuse-values \
  --set replicaCount.storage=6 \
  --set image.pullPolicy=Never \
  --set gatewayImage.pullPolicy=Never \
  --set web.image.pullPolicy=Never

kubectl -n opskv get pods -l app.kubernetes.io/component=storage
# 期望出现 storage-3 / storage-4 / storage-5，且为 Joining
```

请使用 `helm upgrade`，不要只用 `kubectl scale sts/storage`（否则 Gateway/Web 地址列表会对不齐）。

**B — 运维页人工加入**

1. 打开 http://127.0.0.1:19100/ → **运维**
2. **待加入节点**中对 `storage-3` / `4` / `5` 分别点 **上线 / 加入**
3. 全部变为 Active 后，点 **Balance**（可多点几次）

可选 CLI（本机能连 Meta 时）：`./build/tools/skv join storage-3`，再 `skv balance`。

### 4.3 缩容 Storage（摘要）

设计：[DATA_SCALE_IN.md](./architecture/DATA_SCALE_IN.md)。

1. 运维页下线 / `skv scale-in` → 等待迁空 → Tombstone  
2. `remove-tombstone`  
3. 再 `helm upgrade --set replicaCount.storage=<更小>`  

Draining 期间不要强杀 Pod。

---

## 5. 常见问题

| 现象 | 建议 |
|------|------|
| Web 打不开 | 确认 LoadBalancer / `port-forward`；ClusterIP 不能直接用本机 IP 访问 |
| 拓扑全「离线」但顶部计数正常 | 使用集群 DNS 版 Web 镜像；K8s 勿依赖本机 `127.0.0.1` 探测 |
| 加了 Storage 但运维页没有待加入 | 查 Pod Ready 与 `REGISTER` 日志；conf 应为「待加入」形态（空 bootstrap、peers 仅自己） |
| `JOIN` 失败 `not_pending` / `heartbeat_stale` | 等心跳正常后再加入；升级到含 JOIN 修复的镜像后滚动 Meta/Storage |
| Balance 返回 0 | Region 很少或负载已均匀时正常；可先 Split 再 Balance |

---

## 6. 许可证

Apache License 2.0 · 见仓库 [LICENSE](../LICENSE) 与 [NOTICE](../NOTICE)。
