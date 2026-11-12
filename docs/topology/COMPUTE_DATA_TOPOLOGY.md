# Compute 数据拓扑结构

无状态计算层：缓存 Meta 路由 + 代理 Storage RPC。  
图例：`字段 — 含义 / 作用`  
代码：`compute/include/openkv/compute/*.hpp`  
生产入口：C++ `gateway_node` → `GatewayServer`（见 [flows/18-cpp-gateway.md](../flows/18-cpp-gateway.md) · [flows/29-compute-code-map.md](../flows/29-compute-code-map.md)）。

---

## 1. 总图（字段 + 含义）

```mermaid
classDiagram
  direction TB

  class GatewayServer {
    +meta_addrs_ : Meta 端点
    +storage_addrs_ : boot Storage 地址
    +ts_ : TSOracle — 批缓存 TSO
    +routes_ : Router — Key→Region→Leader
    +active_ts_ : 活跃 start_ts
    +pool_ : WorkPool — 本地短任务
    +leader_hint_ : not_leader 记忆
    +bg_thread_ : route_watch_loop
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

  class store_rpc {
    +cached stubs : Store/Admin
    +Propose/Get/Scan/... : StoreService
    +Ping/SetEpoch : AdminService
  }

  class meta_client {
    +GetTs / GetRoutingTable
    +WatchRoutingTable / ReportMinStartTs
  }

  GatewayServer *-- Router : routes_
  GatewayServer --> store_rpc : 出站 Storage
  GatewayServer --> meta_client : 出站 Meta
  GatewayServer *-- TSOracle : ts_
  Router *-- RoutingTable : table_
  RoutingTable *-- RegionRoute : regions
  RegionRoute *-- ReplicaEndpoint : replicas
```

---

## 2. 包含关系展开

```mermaid
flowchart TB
  GW["GatewayServer<br/>────────────────────────<br/>routes_ — 本地路由镜像<br/>ts_ — TSO 批缓存<br/>storage_addrs_ — boot 地址<br/>pool_ — WorkPool（本地）<br/>bg_thread_ — 刷路由 + report_min_start_ts"]

  GW --> RT["Router.table_<br/>────────────────────────<br/>version — 落后则更新<br/>regions — RegionRoute 字典<br/>sorted_ — start_key 有序便于二分"]

  RT --> RR["RegionRoute<br/>────────────────────────<br/>region_id — 与 Storage shard 一致<br/>range — locate(key)<br/>replicas — 可试的地址列表"]

  RR --> RE["ReplicaEndpoint<br/>────────────────────────<br/>node_id / address — 连谁<br/>is_leader — put 优先 Leader"]

  GW --> SR["store_rpc / meta_client<br/>────────────────────────<br/>StoreService / AdminService / MetaService<br/>进程级 Channel+Stub 缓存"]
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

### `GatewayServer`（核心成员）

| 字段 | 含义 | 作用 |
|------|------|------|
| `routes_` | Router | key → region / leader |
| `ts_` | TSOracle | 事务时间戳 |
| `storage_addrs_` | boot 地址串 | try_order 回退 |
| `pool_` | WorkPool | Watch 间隔等本地任务 |
| `leader_hint_` | region→addr | not_leader 后优先打 |
| `bg_thread_` | 后台 | WatchRoutingTable + active_ts 上报 |

---

## 4. 请求路径与字段流

```mermaid
flowchart LR
  K["key"] --> R["Router.region_of<br/>用 range"]
  R --> L["leader_addr / leader_hint_<br/>用 is_leader/address"]
  L --> P["store_rpc<br/>带 region_id"]
  P -->|not_leader| RETRY["换 replicas / 刷路由"]
```

出站一律类型化 gRPC（StoreService / AdminService / MetaService）。详见 [29-compute-code-map](../flows/29-compute-code-map.md)。

---

## 相关

- [META_DATA_TOPOLOGY.md](META_DATA_TOPOLOGY.md) — 权威路由在 Meta  
- [DATA_TOPOLOGY_INDEX.md](DATA_TOPOLOGY_INDEX.md)  
