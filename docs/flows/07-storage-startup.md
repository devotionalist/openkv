# 流程：Storage 节点启动

**入口：** `storage/src/main.cpp` → `StorageNode::start` → `StorageGrpcServer::start`

**关键代码：** `storage_node.cpp` · `exec_line_handler.cpp` · `ShardEngineManager` · `MultiRaft` factory

```mermaid
flowchart TD
  A[main 加载 NodeConfig<br/>node_id listen data_dir<br/>shard_count raft_peers] --> B[构造 StorageNode<br/>ShardEngineManager]
  B --> C[StorageNode::start]
  C --> D[解析 raft_peers<br/>GrpcRaftTransport::set_peer]
  D --> E[transport_.start<br/>异步 RPC worker]
  E --> F[创建 MultiRaft 工厂]
  F --> G[预热 0..shard_count-1<br/>+ 运行中按 region get_or_create]
  G --> H[RaftNode Options<br/>persist_dir=raft_shard_id]
  H --> I[绑定 on_apply<br/>snap_provider / applier<br/>register_node]
  I --> J{磁盘有 WAL?}
  J -->|是| K[load_from_disk]
  J -->|否| L[空日志 Follower]
  K --> M[recover_apply]
  L --> M
  M --> N[启动 tick_thread 50ms<br/>含自动 split 检查]
  N --> O[启动 hb_thread stub]
  O --> P[StorageGrpcServer::start<br/>listen_addr 对外]
  P --> Q[等待 Region 0 等<br/>出现 leader_id]
  Q --> R[节点就绪<br/>可 PUT/GET/ADMIN_*/RAFT_*]
```

## 工厂内依赖关系

```mermaid
flowchart LR
  MR[MultiRaft] -->|lazy create| RN[RaftNode]
  RN -->|ApplyCallback| OA[StorageNode::on_apply]
  OA --> KE[KvEngine put/del]
  RN -->|TransportSend| TT[GrpcRaftTransport]
  RN -->|SnapshotProvider| CS[create_snapshot]
  RN -->|SnapshotApplier| AS[apply_snapshot]
  TT -->|RAFT_* TCP| Peer[其它 Storage]
```

## 启动顺序约束

1. **先** `transport_.start` + 注册 Raft，**再**对外 gRPC（否则对等节点连不上 RaftService）。
2. RocksDB 在首次 `get_or_create(shard)` / apply 时打开，不必全部预热。
3. `recover_apply` 必须在对外服务前执行，避免已提交未应用的缺口。
