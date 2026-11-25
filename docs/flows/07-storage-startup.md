# 流程：Storage 节点启动

**入口：** `storage/src/main.cpp` → `StorageNode::start` → `StorageGrpcServer::start`

**关键代码：** `storage_node.cpp` · `exec_line_handler.cpp` · `ShardEngineManager` · `MultiRaft` factory

```mermaid
flowchart TD
  A[main 加载 NodeConfig<br/>node_id listen data_dir<br/>shard_count raft_peers] --> B[构造 StorageNode<br/>ShardEngineManager]
  B --> C[StorageNode::start]
  C --> D[解析 raft_peers 到 bootstrap_peers_]
  D --> E[创建 MultiRaft<br/>内持 GrpcRaftTransport<br/>构造时接到 Store]
  E --> F[transport.set_peer<br/>bootstrap get_or_create / register_node]
  F --> G[MultiRaft::start<br/>出站 worker + BatchSystem]
  G --> H[注入 apply_fn / snap_provider]
  H --> I{磁盘有 WAL?}
  I -->|是| J[load_from_disk]
  I -->|否| K[空日志 Follower]
  J --> L[recover_apply]
  K --> L
  L --> M[启动 tick / split / hb 线程]
  M --> N[StorageGrpcServer::start<br/>listen_addr 对外]
  N --> O[节点就绪<br/>可 PUT/GET/ADMIN_*/RAFT_*]
```

## 工厂内依赖关系

```mermaid
flowchart LR
  MR[MultiRaft] -->|持有| TT[GrpcRaftTransport]
  MR -->|lazy create| RN[RaftNode]
  ST[Store Ready] -->|transport_fn| TT
  RN -->|经 Store apply| OA[StorageNode::on_apply]
  OA --> KE[KvEngine put/del]
  RN -->|SnapshotProvider| CS[create_snapshot]
  RN -->|SnapshotApplier| AS[apply_snapshot]
  TT -->|RAFT_* gRPC| Peer[其它 Storage]
```

## 启动顺序约束

1. **先** `MultiRaft::start`（出站 Transport + Store）+ 注册 Raft，**再**对外 gRPC（否则对等节点连不上 RaftService）。
2. RocksDB 在首次 `get_or_create(shard)` / apply 时打开，不必全部预热。
3. `recover_apply` 必须在对外服务前执行，避免已提交未应用的缺口。
4. Storage **不再** `set_transport_fn`；出站内聚在 MultiRaft（见 [54](54-storage-raft-boundary.md)）。
