# Kubernetes / Helm 部署（HA）

## 问题

本机 `conf/*.conf` 与 `scripts/start-cluster.sh` 把 peer 写成 `127.0.0.1`，不能直接搬进集群。Compose（[deploy/docker-compose.yml](../../deploy/docker-compose.yml)）同样依赖本机网络与固定端口。上 k8s 需要：

- 稳定 DNS 地址（StatefulSet + Headless Service）
- 持久卷（Meta Raft / Storage RocksDB）
- 可重复安装（Helm chart）

## 方案

Chart：[`deploy/helm/opskv`](../../deploy/helm/opskv)

| 组件 | 工作负载 | 说明 |
|------|----------|------|
| Meta ×3 | StatefulSet + headless | `meta-N.meta-headless.<ns>.svc`，PVC `/data` |
| Storage ×3 | StatefulSet + headless | 同上；`raft_peers` / `meta_addr` 用 DNS |
| Gateway | Deployment + Service | `-metas` / `-storages` 指向 headless；`-stats-listen=0.0.0.0:19001` + Service `stats` 口，供 Web 拉 `/stats` 画网关线 |
| Web | Deployment（默认 `web.enabled=true`） | 管控台 :19100；无 `-run-dir`，拓扑用 `-metas/-storages/-gateway` DNS 探测在线态（勿再画本机 127.0.0.1） |

配置：入口脚本（[deploy/docker-entrypoint.sh](../../deploy/docker-entrypoint.sh)）按 `POD_NAME` ordinal 写 `/etc/opskv/node.conf`，生成 DNS 级 `meta_peers` / `raft_peers`，并写 `advertise_addr`（Storage `REGISTER` 可达地址；见 `NodeConfig::advertise_addr`）。

镜像：

- `deploy/Dockerfile` → `opskv:<tag>`（`meta_server` / `storage_node`）
- `deploy/Dockerfile.gateway` → `opskv-gateway:<tag>`（compute-gateway）
- `deploy/Dockerfile.web` → `opskv-web:<tag>`（管控台）

## 边界

- 不做 Operator / 自动扩缩容；加副本仍走 Scaler / Web / skv
- 依赖集群默认或 values 指定的 `storageClassName`
- 首版无 Ingress；对外用 `Service` type 或 `kubectl port-forward`
- 本地 Colima/k3s：`service.web.type=LoadBalancer`（默认）时，lima 会把 guest `:19100` 映到本机 `http://127.0.0.1:19100`，**不必**再挂 `port-forward`（ClusterIP 时 port-forward 一断就打不开）
- `replicaCount.meta` / `storage` 默认 3；改副本数须同步改 peer 列表（chart 用 `range` 生成）

## 扩容操作（Gateway / Storage）

见 [K8S_SCALE.md](K8S_SCALE.md)：

- **加节点（Helm）**：只拉起 Store → `REGISTER` → **Joining**，**不**自动进集群  
- **加入 / 打散（运维页人工）**：待加入 → 上线/加入 → Balance  

本地进程版：[SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md)。

## 失败与回滚

```bash
helm upgrade --install opskv deploy/helm/opskv -n opskv --create-namespace
helm rollback opskv -n opskv
helm uninstall opskv -n opskv
# PVC 默认保留；彻底清盘需手动删 PVC
```

## 与现有组件关系

- 本地开发仍用 `start-cluster.sh` / Compose
- 协议与二进制不变；仅部署拓扑与地址发现变化
- 见 [GRPC_MANDATORY.md](GRPC_MANDATORY.md)：集群内仅 gRPC

## 本地 Colima / k3s

Docker 镜像在 docker 侧，k3s 用 containerd，需导入后再 `pullPolicy=Never`：

```bash
docker save opskv:dev | colima ssh -- sudo k3s ctr images import -
docker save opskv-gateway:dev | colima ssh -- sudo k3s ctr images import -
docker save opskv-web:dev | colima ssh -- sudo k3s ctr images import -
```

## 验收

```bash
# 无集群：模板门禁
bash scripts/it-helm-template.sh

# 有 kind + docker + helm：
bash scripts/k8s-kind-smoke.sh
# 期望：helm status deployed；gateway Ready；port-forward 后 kv-example PASS
```

本机 Colima（2026-10-26）：`STATUS: deployed`；7 Pod Ready；`kv-example` → `get found=true value=opskv`。
