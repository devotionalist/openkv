# Compute 数据拓扑结构

无状态计算层：缓存 Meta 路由 + 代理 Storage RPC。  
图例：`字段 — 含义 / 作用`  
代码：`compute/include/opskv/compute/*.hpp`  
（生产 Gateway 多为 Go；本模块为 C++ 对照实现。）

---

## 1. 总图（字段 + 含义）

```mermaid
classDiagram
  direction TB

  class ComputeNode {
    +cfg_ : NodeConfig — meta_addr / 本机参数
    +router_ : 路由缓存 — Key→Region→Leader
    +proxy_ : KvProxy — get/put 重定向
    +backend_storage_addr_ : demo 默认 Storage
    +watch_thread_ : 拉 Meta 路由
  }

  class Router {
    +table_ : RoutingTable — 版本化缓存
    +sorted_ : RegionMeta 有序 — 二分 locate
  }

  class RoutingTable {
    +version : 与 Meta 对齐 — 缓存失效
    +shard_count : regions 数量
    +regions : map RegionId→RegionRoute
  }

  class RegionRoute {
    +region_id : 分片号
    +range : KeyRange — 负责区间
    +replicas : 副本端点列表
  }

  class ReplicaEndpoint {
    +node_id : Storage ID
    +address : 拨号地址
    +is_leader : 是否打写到此
  }

  class KvProxy {
    +router_ : 查 Leader
    +get_fn_ / put_fn_ : Storage RPC 回调
    +max_redirects_ : not_leader 重试次数
  }

  ComputeNode *-- Router
  ComputeNode *-- KvProxy
  Router *-- RoutingTable : table_
  RoutingTable *-- RegionRoute : regions
  RegionRoute *-- ReplicaEndpoint : replicas
  KvProxy --> Router
```

---

## 2. 包含关系展开

```mermaid
flowchart TB
  CN["ComputeNode<br/>────────────────────────<br/>cfg_ — 连哪个 Meta<br/>router_ — 本地路由镜像<br/>proxy_ — 业务入口<br/>watch_thread_ — 周期性/长轮询刷路由"]

  CN --> RT["Router.table_<br/>────────────────────────<br/>version — 落后则更新<br/>regions — RegionRoute 字典<br/>sorted_ — start_key 有序便于二分"]

  RT --> RR["RegionRoute<br/>────────────────────────<br/>region_id — 与 Storage shard 一致<br/>range — locate(key)<br/>replicas — 可试的地址列表"]

  RR --> RE["ReplicaEndpoint<br/>────────────────────────<br/>node_id / address — 连谁<br/>is_leader — put 优先 Leader"]

  CN --> KP["KvProxy<br/>────────────────────────<br/>get/put/del<br/>leader 失败→换副本重试<br/>max_redirects_=3"]
```

---

## 3. 字段词典

### `ReplicaEndpoint` / `RegionRoute` / `RoutingTable`

| 字段 | 含义 | 作用 |
|------|------|------|
| `node_id` | Storage 节点 ID | 日志/调试 |
| `address` | `host:port` | RPC 目标 |
| `is_leader` | Leader 标记 | 写路径优先 |
| `region_id` | Region 号 | 请求带 shard |
| `range` | key 区间 | `region_of(key)` |
| `replicas` | 副本列表 | 重试候选 |
| `version` | 路由版本 | 与 Meta `route_ver` 对齐 |
| `shard_count` | Region 个数 | 兼容字段 |
| `regions` | 全表 | 缓存主体 |

### `Router`

| 字段 | 含义 | 作用 |
|------|------|------|
| `table_` | 路由表 | update 后对外查询 |
| `sorted_` | 有序 RegionMeta | 二分定位 key |
| `mu_` | 锁 | 并发 update/读 |

### `KvProxy` / 结果结构

| 字段 | 含义 | 作用 |
|------|------|------|
| `get_fn_` / `put_fn_` | 传输回调 | 注入 TCP/gRPC |
| `max_redirects_` | 最大重定向 | 防环 |
| `ProxyGetResult.ok/found/value/revision/error` | 读结果 | 上层业务 |
| `ProxyPutResult.ok/revision/error` | 写结果 | 上层业务 |

### `ComputeNode`

| 字段 | 含义 | 作用 |
|------|------|------|
| `cfg_` | 节点配置 | meta 地址等 |
| `router_` / `proxy_` | 核心组件 | 对外 API |
| `running_` / `watch_thread_` | 生命周期 | 后台刷路由 |
| `backend_storage_addr_` | 默认后端 | 单机 demo |

---

## 4. 请求路径与字段流

```mermaid
flowchart LR
  K["key"] --> R["Router.region_of<br/>用 range"]
  R --> L["leader_addr<br/>用 is_leader/address"]
  L --> P["KvProxy → Storage<br/>带 region_id"]
  P -->|not_leader| RETRY["换 replicas 重试<br/>max_redirects_"]
```

---

## 相关

- [META_DATA_TOPOLOGY.md](META_DATA_TOPOLOGY.md) — 权威路由在 Meta  
- [DATA_TOPOLOGY_INDEX.md](DATA_TOPOLOGY_INDEX.md)  
