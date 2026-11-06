# Kubernetes / Helm：加 Gateway / Storage

面向 Colima/k3s + Helm。部署总览：[K8S_HELM.md](K8S_HELM.md)；本地进程版：[SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md)；设计：[DATA_SCALE.md](../architecture/DATA_SCALE.md) · [扩容缩容设计.md](扩容缩容设计.md)。

Web 管控台：http://127.0.0.1:19100/

```bash
HELM_IMG=(
  --set image.pullPolicy=Never
  --set gatewayImage.pullPolicy=Never
  --set web.image.pullPolicy=Never
)
```

---

## 核心原则（务必分清）

| 阶段 | 做什么 | 谁操作 | 结果 |
|------|--------|--------|------|
| **A. 增加节点** | 只拉起 Pod / 进程，登记到 Meta | Helm / 基础设施 | Store **`Joining`（待加入）**，**未进调度、未持副本** |
| **B. 加入集群并扩容** | 准入 + 迁 Region（RF 不变） | **运维页人工**（或等效 `skv`） | `JOIN` → **Active** → **Balance** |

Helm 的 `replicaCount.storage+1` **不等于** 集群扩容完成。  
**不要**在文档或自动化里把「加节点」写成自动 `join` / `balance`。

```mermaid
flowchart LR
  A["A Helm：加节点"] --> B["REGISTER → Joining\n待加入表可见"]
  B -.->|人工| C["B 运维页：上线/加入"]
  C --> D["Active"]
  D -.->|人工| E["B 运维页：Balance"]
  E --> F["Region 迁入 · RF 不变"]
```

---

## 1. 加 Gateway（无状态）

Gateway 无 Joining 态，加 Deployment 副本即可（无 join）：

```bash
helm upgrade opskv deploy/helm/opskv -n opskv \
  --reuse-values --set replicaCount.gateway=2 "${HELM_IMG[@]}"
kubectl -n opskv get pods -l app.kubernetes.io/component=gateway
```

---

## 2. Storage：A — 只增加节点

### 2.1 配置形态（入口脚本）

对齐 `conf/storage.conf` vs `conf/storage-3.conf`；[docker-entrypoint.sh](../../deploy/docker-entrypoint.sh) 按 ordinal：

| 角色 | ordinal | `bootstrap_regions` | `raft_peers` |
|------|---------|---------------------|--------------|
| 初始投票组 | `< replicaFactor`（默认 0/1/2） | `0` | 仅前 RF 个 |
| **待加入（仅加节点）** | `≥ replicaFactor` | **空** | **仅自己** |

禁止给新节点写 `bootstrap_regions=0` + 全员 `raft_peers`（会抢选主）。

### 2.2 Helm 拉起（示例 3 → 4）

```bash
helm upgrade opskv deploy/helm/opskv -n opskv \
  --reuse-values --set replicaCount.storage=4 "${HELM_IMG[@]}"

kubectl -n opskv rollout status sts/storage --timeout=180s
kubectl -n opskv get pods -l app.kubernetes.io/component=storage
```

须用 `helm upgrade`，不要只 `kubectl scale sts/storage`（Gateway/Web `-storages` 与 conf 会对不齐）。

**本阶段结束标准：**

- `storage-3` Ready  
- conf：`bootstrap_regions` 空、`raft_peers` 仅自己  
- Meta / Web「待加入」可见，`state=Joining`  
- **尚未** Active，路由上**无**该 Store 副本（或 regions=0）

```bash
kubectl -n opskv exec storage-3 -- cat /etc/opskv/node.conf
curl -sS http://127.0.0.1:19100/api/nodes \
  | jq '.nodes[] | select(.id=="storage-3") | {id,state,up,regions}'
```

---

## 3. Storage：B — 运维页加入集群并扩容（人工）

在 Web **运维**页完成（推荐），对应本地 `skv join` / `skv balance`：

| 步骤 | 运维页 | 含义 |
|------|--------|------|
| 1 | **待加入节点** → **上线 / 加入** | `JOIN_NODE`：Joining → **Active**（准入调度） |
| 2 | **Balance** | RF 不变 Replace，把部分 Region 迁到新 Store |

可选 CLI（能连 Meta 时）：

```bash
./build/tools/skv join storage-3
./build/tools/skv balance
```

**本阶段结束标准：** 节点 Active；`balance` 后部分 Region 落在新 Store；读写仍通。

> 默认路径**不是** Meta `SCALE_OUT`（那会抬 RF）。见 [扩容缩容设计.md](扩容缩容设计.md)。

### 3.1 JOIN 失败排查

| 现象 | 原因 / 处理 |
|------|-------------|
| `ERR not_pending` 且 UI 显示 Offline | 旧版：Offline 再 REGISTER 只本地升 Active。已修为 Raft → Joining；升级 meta/storage 镜像后重启 STS |
| `ERR heartbeat_stale` / `no_heartbeat` | Store 未连上 Meta；查日志是否误连 `127.0.0.1:8500`（DNS 部署勿用本机 fallback） |
| `ERR unreachable` | PING 失败；等 Pod Ready 再点加入 |
| 待加入不出现 | 确认 conf 为空 `bootstrap_regions` + 仅自己 peers；看 `REGISTER ok` |

---

## 4. 缩容 Storage（摘要）

设计：[DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md)。

1. **运维页**下线 / `skv scale-in` → Draining → Tombstone（进程保持存活）  
2. `remove-tombstone`  
3. 再 Helm 降 `replicaCount.storage`  

详见 [SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md) SOP D。

---

## 5. 不要做

| 项 | 原因 |
|----|------|
| 把 Helm 加副本当成「已扩容进集群」 | 只到 Joining；须运维页 join + balance |
| 自动化里 Helm 后立刻 join/balance | 产品约定：**加入/扩容由运维页人工确认** |
| 只 `kubectl scale sts/storage` | 配置列表不对齐 |
| 新节点带 `bootstrap_regions=0` + 全员 peers | 脑裂 / Offline |
| 用 `SCALE_OUT` 当默认扩容 | 抬 RF |

---

## 6. 验收

```bash
bash scripts/it-entrypoint-storage-conf.sh   # A 阶段 conf 形态
bash scripts/it-helm-template.sh

# A：加节点后
#   storage-N Joining；运维页「待加入」有条目

# B：运维页加入 + Balance 后
curl -sS http://127.0.0.1:19100/api/nodes | jq '.nodes[] | {id,state,regions}'
kubectl -n opskv port-forward svc/gateway 19000:19000 &
./.run/kv-example 127.0.0.1:19000    # get found=true
```
