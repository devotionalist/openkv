# 测试用例清单与执行记录

本文档汇总仓库内**已落地**的自动化用例、集成/演练脚本，以及近期执行情况。  
发版门禁场景编号（I*/B*/F*）的设计目标见 [INTEGRATION_TEST.md](INTEGRATION_TEST.md)、[FAULT_DRILL.md](../ops/FAULT_DRILL.md)；本页侧重「有什么、怎么跑、最近跑没跑过」。

> 状态约定：`PASS` 已通过 · `FAIL` 失败 · `SKIP` 未跑 · `XFAIL` 已知失败已登记 · `MANUAL` 需人工

---

## 1. 怎么跑

| 入口 | 覆盖 | 命令 |
|------|------|------|
| C++ 单元 | gtest → ctest（约 128 条） | `make test` / `make gtest` |
| Go 单测 | web 生命周期 / stats | `(cd web && go test ./internal/...)` |
| E2E 冒烟 | 启集群 + Put/Get 等 | `make e2e` → `scripts/run-e2e.sh` |
| 持续负载集成 | e2e/txn + 杀 Store | `make integ-workload` |
| 扩缩容无感 | join/balance + scale-in；设计见 [DATA_SCALE.md](../architecture/DATA_SCALE.md) | `bash scripts/it-scale-live.sh` |
| 缩容无感 | scale-in only；设计见 [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md) | `bash scripts/it-scale-in-live.sh` |
| Region 副本 | split / 加副本 / 下线 | `bash scripts/it-region-replica.sh` |
| 副本 churn | 快速加/下线 + 并发排队 | `bash scripts/it-replica-churn.sh` |
| 数据结构 | Hash/Set/List/ZSet | `bash scripts/it-ds-{hash,set,list,zset}.sh` |
| Web 全链路 | I50–I54 + `/api/ds` | `make it-web` / `bash scripts/it-web.sh` |
| Region scatter | I30–I36 硬断言 | `make it-region` |
| Txn 硬门禁 | I20–I22 | `make it-txn` |
| 磁盘 / 分区 | F14、F3/F11 近似 | `make integ-extra` |
| 故障演练 | F1/F2/F4/F5/F8/F10 子集；设计见 [DATA_HA.md](../architecture/DATA_HA.md) | `make fault-drill` |
| 发版门禁 | unit + e2e + it-web/region/txn + fault-drill | `make release-gate` |
| Sanitizer | ASan/UBSan | `make memcheck` |

产物建议：`./.run/release-gate.log`、各 `scripts/*.sh` 终端输出。

---

## 2. 执行总览（最近一次）

记录日期：**2026-10-26**（本机；数据面硬化后抽测 SnapWorker* + Coprocessor* + Raftstore* + StoreAdmin* + RawNode* + Scheduler* **18/18 PASS**；见 [flows/DATA_PLANE_HARDENING.md](../flows/DATA_PLANE_HARDENING.md)）。  
上一完整总览：**2026-10-25**。

| 套件 | 规模 | 结果 | 备注 |
|------|------|------|------|
| C++ `opskv_unit_tests` | **128** `TEST*` | **PASS（Scaler 新例）** | `ScalerTest.ConcurrentScaleOutQueues` / `CancelInflight` 新增并通过 |
| Go `web/internal/lifecycle` | 3 | **PASS** | `go test` ~1s |
| Go `web/internal/cluster` | 3 | **PASS** | `go test` ~1s |
| Go `client/go` | — | SKIP | 无 `*_test.go`（`gwput_test.go` 为手工 main） |
| `make e2e` / `run-e2e.sh` | 冒烟 | SKIP | 本次未跑全量 |
| `make integ-workload` | 集成 | SKIP | 本次未跑 |
| `it-scale-live.sh` | I 扩缩容 | SKIP | 本次未跑脚本；手工扩容见 §5 |
| `it-scale-in-live.sh` | I 缩容 | SKIP | |
| `it-region-replica.sh` | I37 | SKIP | |
| `it-replica-churn.sh` | I39 | **PASS** | 快速加→立刻下线；并发排队；重复加拒绝 |
| `it-ds-*.sh` / `ds-smoke` | I40–I43 | **PASS** | Hash/Set/List/ZSet 经 Gateway；`wrong_type`；与裸 Put 隔离 |
| `it-web.sh` | I50–I54 | **PASS** | overview/KV/split/DS/启停 |
| `it-region.sh` | I30–I36 | **PASS** | scatter→3/4/5 双侧可读；align 后 merge |
| `it-txn.sh` | I20–I22 | **PASS** | txn_gate |
| `it-partition.sh` | F3/F11 | **PASS** | STOP 1 可写；STOP 2/杀 2 降级后恢复 |
| `it-disk-fault.sh` | F14 | 周跑 | 只读目录；需稳态基线（见 IT_COVERAGE_EXPANSION） |
| `make fault-drill` | F1/F2/… | SKIP | 含 F2 |
| `make release-gate` | 门禁 | SKIP | |
| 手工：KV Put/Get | — | **PASS** | 清盘后 `kv-example` |
| 手工：Split/Merge 数据 | I30/I33 | **FAIL→已修** | 见 §5 |
| 手工：SCALE_OUT storage-5 | — | **FAIL→已修** | 见 §5 |

---

## 3. C++ 单元测试（gtest）

源码：`tests/{common,raft,storage,meta,compute}/*_test.cpp`  
二进制：`build/tests/opskv_unit_tests`（`gtest_discover_tests`，label=`unit`）

### 3.1 按模块汇总

| 模块 | 文件数 | 用例数 | 关注点 |
|------|--------|--------|--------|
| common | 4 | 14 | 哈希、Range、配置、Admin ACL |
| raft | 11 | 34 | 日志、选主、Joint、快照流、RawNode、raftstore |
| storage | 12 | 45 | KvEngine/快照区间、MVCC、StoreFacade、Admin Raft、Watch |
| meta | 4 | 27 | ClusterState、Scaler（含排队/取消）、Scheduler、Meta HA FSM |
| compute | 2 | 8 | Router Locate、Proxy 重试 |
| **合计** | **33** | **128** | |

### 3.2 套件 → 用例名

<details>
<summary>common（14）</summary>

| Suite.Case | 文件 |
|------------|------|
| AdminAclTest.{ClassifyOps,SessionAuth,MetaHandleLineRequiresAuth} | `common/admin_acl_test.cpp` |
| ConfigTest.{LoadNodeConfig,LoadBootstrapPeers,MissingFileThrows} | `common/config_test.cpp` |
| HashTest.{Fnv1aStable,EmptyKey,ShardOfRange,ShardOfZeroCount,ShardDistributionNotCollapsed} | `common/hash_test.cpp` |
| RegionTest.{ContainsAndInf,LocateSorted,CanSplitAt} | `common/region_test.cpp` |

</details>

<details>
<summary>raft（34）</summary>

| Suite | Cases | 文件 |
|-------|-------|------|
| RaftLogTest | AppendAndQuery, AppendEntriesConflictTruncate, AppendEntriesRejectBadPrev, CompactTo, EntriesFromLimit | `raft_log_test.cpp` |
| RaftNodeTest | SingleNodeElectionAndPropose, ProposeRejectedWhenFollower, ConfChangeAddLearner, PreVoteDoesNotBumpTerm, CheckQuorumStepsDownIsolatedLeader, VoteAndAppendBetweenPeers | `raft_node_test.cpp` |
| RaftJointTest / RaftAsyncApplyTest | PromoteUsesEnterThenLeaveJoint, RejectConfWhileJointPending, ProposeAppliesViaWorker | `raft_joint_test.cpp` |
| RaftPersistTest | SaveAndRecoverApplied, InstallSnapshotCompactsAndApplies | `raft_persist_test.cpp` |
| RaftEngineTest | FileLogBatchRoundTrip, EntryCrcRoundTrip, FilePureAppend, CutAndGc | `raft_engine_test.cpp` |
| SnapStreamTest | HexRoundTrip, AssembleChunks, EmptySnapshot, RejectOutOfOrder | `snap_stream_test.cpp` |
| RaftReadIndexTest / RaftFrameTest | LeaseReadOnSingleNode, SoftStateMetrics, EncodeDecodeRoundTrip | `raft_read_index_test.cpp` |
| RawNodeTest | SingleNodeElectAndPropose, BootstrapThenPropose, ThreeNodeVoteAndProposeReady | `raw_node_test.cpp` |
| MultiRaftTest | GetOrCreateAndLeaders | `multi_raft_test.cpp` |
| RaftstoreTest | StoreTickElectsLeader, HibernateSkipsTickUntilWake, ApplyQosFlag | `raftstore_test.cpp` |

</details>

<details>
<summary>storage（45）</summary>

| Suite | 要点 |
|-------|------|
| KvEngineTest | Put/Get/Delete、Snapshot、**SnapshotRangePreservesSibling**（兄弟区间不被空 range 抹掉）、Checkpoint、CF、SyncMode |
| MvccTest | Lock、可见性、WriteConflict、悲观锁、GC |
| StoreFacadeTest / StoreCodecTest / StoreAdminRaftTest | 端到端 Put、编解码、Epoch、Split seed、幂等 Propose |
| WatchHubTest / WatchLogTest | Watch 过滤与持久化 |
| InflightGateTest / LatencyHistTest / SchedulerTest / CoprocessorTest | 限流、延迟、Latch、前缀扫描 |
| RocksRaftEngineTest / ShardEngineManagerTest | Raft 引擎、单 Store 共享引擎 |

</details>

<details>
<summary>meta（27）</summary>

| Suite | 要点 |
|-------|------|
| ClusterStateTest | TSO、GC safe point、Split/Merge、Joining→Active、Drain、Learner Promote、Place、Snapshot |
| ScalerTest | ScaleOut 完成、未知节点失败、CatchUp 门闩、**ConcurrentScaleOutQueues**、**CancelInflight** |
| RegionSchedulerTest | 提交/恢复任务、扫描欠副本 |
| MetaFsmTest / MetaHaTest | FSM 编解码、单副本 Leader Register |

</details>

<details>
<summary>compute（8）</summary>

| Suite | Cases |
|-------|-------|
| RouterTest | BootstrapSingle, UpdateIgnoresStaleVersion, RangeLocateAfterSplit, ReplicaAddrsAndFallback, MissingShard |
| KvProxyTest | PutGetViaTransport, NoRoute, RedirectRetry |

</details>

单独跑某个套件：

```bash
ctest --test-dir build -R KvEngineTest --output-on-failure
./build/tests/opskv_unit_tests --gtest_filter='KvEngineTest.*'
```

---

## 4. Go / 脚本集成用例

### 4.1 Go

| 包 | 用例 | 命令 | 最近结果 |
|----|------|------|----------|
| `web/internal/lifecycle` | TestProbeDisk, TestDataDirFor*, TestComponentsFromAddrsShortNames, TestListStatusRemoteTCP | `go test ./internal/lifecycle` | PASS (2026-10-26) |
| `web/internal/cluster` | TestParseStatsLine, TestRateTracker, TestFetchGatewayStats | `go test ./internal/cluster` | PASS (2026-10-25) |
| `client/go` | — | — | 无正式单测 |

### 4.2 集成 / 演练脚本（对照门禁 ID）

| 脚本 | 对应 ID（约） | 内容 | 自动化状态 |
|------|---------------|------|------------|
| `scripts/run-e2e.sh` | I1–I2, I10 | 编译→启停集群→客户端冒烟 | 有（`make e2e`） |
| `scripts/test-client.sh` | I10 | 等 Gateway + kv-example | 有 |
| `scripts/integ-with-workload.sh` | I10–I11 + 杀 Store | 后台 chaos + e2e/txn | 有（`make integ-workload`） |
| `scripts/it-scale-live.sh` | 扩容+缩容无感 | join→balance→scale-in | 有 |
| `scripts/it-scale-in-live.sh` | I38 | 持续流量下缩容 | 有 |
| `scripts/it-region-replica.sh` | I37 | Split→加副本→下线自洽 | 有 |
| `scripts/it-replica-churn.sh` | I39 | 快速加/立刻下线 + 并发 SCALE_OUT 排队 | 有（PASS 2026-10-25） |
| `scripts/it-ds-hash.sh` 等 | I40–I43 | DS Hash/Set/List/ZSet | 有（PASS 2026-10-25）；设计见 [DATA_STRUCTURES.md](../architecture/DATA_STRUCTURES.md) |
| `go test ./internal/dsencode` | — | blob 编解码 / wrong_type | 有 |
| `scripts/it-web.sh` | I50–I54 + Web DS | overview/KV/split/merge/启停/ds | 有（`make it-web`） |
| `scripts/it-region.sh` | I30–I36 | Split→place scatter→双侧读→merge | 有（`make it-region`） |
| `scripts/it-txn.sh` / `examples/txn_gate` | I20–I22 | 提交可见/冲突/Rollback | 有（`make it-txn`） |
| `scripts/it-disk-fault.sh` | F14 (+轻 F15) | 数据目录只读 + 追赶 | 有（`make it-disk-fault`） |
| `scripts/it-partition.sh` | F3 / F11 近似 | STOP/杀 2 副本须降级 | 有（`make it-partition`） |
| `scripts/fault-drill.sh` | F1,F2,F4,F5,F8,F10 | 杀 Store/Meta/GW、**F2 Leader**、滚动、AE | 有（`make fault-drill`） |
| `scripts/release-gate.sh` | 门禁串联 | ctest + e2e + it-web/region/txn + fault-drill | 有（`make release-gate`） |

故障全表 F1–F16 的手册步骤与结果模板：[FAULT_DRILL.md](../ops/FAULT_DRILL.md)。

---

## 5. 近期手工回归（开发联调）

| 日期 | 场景 | 结果 | 根因 / 处置 |
|------|------|------|-------------|
| 2026-10-25 | Gateway Put/Get | FAIL→PASS | Meta 硬重启 TSO 回退 + epoch 错位；TSO 墙钟下界 + epoch 修复 |
| 2026-10-25 | SCALE_OUT `storage-5→R1` Promote | FAIL→PASS | 快照未带 ConfState，新 Leader 丢 Learner；并发 scale 撞 Joint。已补 ConfState / busy / 串行扩容；现网补 ADD_LEARNER+PROMOTE |
| 2026-10-25 | Split 后 Merge，`a=11` 丢失、`t=22` 仍在 | FAIL→PASS（代码） | DESTROY/空 range 快照清空共享引擎兄弟区间。已禁空 range 整库清、快照带 SKVR1；**历史 key 需重 put** |
| 2026-10-25 | `start-cluster` 失败提示 RocksDB 损坏 | 误报 | 实为 LOCK 被旧进程占用；提示已区分 LOCK vs Corruption；`SKV_FRESH=1` 清盘后集群 PASS |
| 2026-10-25 | `start-cluster` gateway `storage not ready`（GRPC=OFF） | 已废止 | 根因：曾允许 `OPSKV_USE_GRPC=OFF`。现已强制 gRPC，见 [flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md) |
| 2026-10-25 | Web「部分刷新失败」`http2: frame too large` | 已废止 | 根因：行协议双栈。现已移除 `SKV_LEGACY_TCP`；Web 仅 gRPC |
| 2026-10-25 | gRPC 强制（拆除 OFF/LEGACY） | 代码 PASS / C++ 验收阻塞 | Go `client/go`+`web` build OK；本机 brew 无 bottle 且源码装 grpc/cmake 卡住。需系统 gRPC 或 FetchContent 后 `FORCE_CMAKE=1 ./scripts/start-cluster.sh` |
| 2026-10-26 | 清理废 TCP listen/发包路径 | 代码 | 收窄并改名 `ExecLineHandler`；删除 LineServer 与 `tcp_line_auth`；去掉 `!GRPC` 桩。见 [flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md) |
| 2026-10-26 | 删除 `TcpTransport`，并入 `GrpcRaftTransport` | **PASS**（编译） | 删除 `tcp_transport.*`；peer/计数并入 `GrpcRaftTransport`；`cmake --build … opskv_raft meta_server storage_node` 绿；补 `tcp_line_client.hpp` include。集群冒烟可随下次 `start-cluster` |
| 2026-10-26 | `TcpServer` → `ExecLineHandler` | **PASS**（编译） | 去 TCP 名；`storage_node` 链接绿。逻辑仍是 ExecLine 行协议，不并入 `grpc_server` |
| 2026-10-26 | `MetaServer` → `MetaNode` | **PASS**（编译） | 对齐 `StorageNode`；文件 `meta_node.*`；二进制仍名 `meta_server`；`cmake --build … --target meta_server` 绿 |
| 2026-10-26 | Helm / k8s HA 部署 | **PASS** | template 门禁绿；Colima k3s：3 meta + 3 storage + gateway Ready；`kv-example` PASS。见 [flows/K8S_HELM.md](../flows/K8S_HELM.md) |
| 2026-10-26 | Web 拓扑在 K8s 显示 127.0.0.1「离线」 | **PASS** | 无 `-run-dir` 时拓扑改用 `-metas/-storages/-gateway` TCP 探测；`go test ./internal/lifecycle`；Colima 下 7 节点 `up=true` + DNS 地址 |
| 2026-10-26 | K8s 拓扑缺网关线 | **PASS** | Gateway 默认 stats 绑 `127.0.0.1`；Helm 加 `-stats-listen=0.0.0.0:19001` + Service `stats`；`/stats` 可达后出现 `client→gateway` / `gateway→storage-*` links |
| 2026-10-26 | K8s 加 Storage 节点 conf | **PASS** | 入口：`ordinal≥RF` → 空 `bootstrap_regions` + 仅自己 peers（仅加节点=Joining）；`scripts/it-entrypoint-storage-conf.sh`；加入/Balance 须运维页人工，见 [K8S_SCALE.md](../flows/K8S_SCALE.md) |
| 2026-10-26 | Helm 加节点后 JOIN not_pending | **PASS** | Offline 再 REGISTER 误 touch→Active；Joining 被 sweep 成 Offline。修：Offline REGISTER 走 Raft→Joining；sweep 跳过 Joining；DNS meta 不回落 127.0.0.1 |
| 2026-10-25 | 快速加副本→立刻下线 / 并发扩容 | FAIL→PASS | 并发 SCALE_OUT 假成功(`scale_busy`/`ok:true`)；下线时 Learner 未进路由。Scaler FIFO 排队+`CANCEL_SCALE`；Web 等路由/强制 REMOVE；`ok=false` on Failed |
| 2026-10-25 | DS Hash/Set/List/ZSet（非 RESP） | **PASS** | gRPC `DSService` + blob 编码；`ds-smoke` / `it-ds-*.sh` 全绿；见 [DATA_STRUCTURES.md](../architecture/DATA_STRUCTURES.md) |

---

## 6. 执行记录表（可追加）

复制一行填写即可：

```text
日期:
执行人:
环境: macOS / commit=
命令:
结果: PASS | FAIL | 部分
摘要:
日志: .run/...
```

### 2026-10-26 (Helm)

```text
日期: 2026-10-26
执行人: agent
环境: darwin Colima k3s；helm+docker
命令:
  bash scripts/it-helm-template.sh                               → PASS
  docker build -f deploy/Dockerfile -t opskv:dev .               → PASS
  docker build -f deploy/Dockerfile.gateway -t opskv-gateway:dev → PASS
  docker save … | colima ssh -- sudo k3s ctr images import -     → PASS
  helm upgrade --install opskv deploy/helm/opskv -n opskv …      → PASS（--wait 曾因 ImageNeverPull 超时；导入镜像后 Pod Ready）
  kubectl port-forward svc/gateway 19000 + kv-example            → PASS（value=opskv）
结果: PASS
摘要: 本地 k8s HA 拓扑可用
```

### 2026-10-26 (Web K8s topology)

```text
日期: 2026-10-26
执行人: agent
环境: darwin Colima k3s
命令:
  (cd web && go test ./internal/lifecycle ./internal/cluster)     → PASS
  docker build -f deploy/Dockerfile.web -t opskv-web:dev .       → PASS
  k3s ctr import + rollout restart deploy/web                    → PASS
  curl /api/topology → 7 nodes up=true (DNS addrs)               → PASS
  kv-example via gateway                                         → PASS
结果: PASS
摘要: 拓扑不再画本机 127.0.0.1 离线假节点；Meta/Storage/Gateway 均在线
```

### 2026-10-26

```text
日期: 2026-10-26
执行人: agent
环境: darwin, opskv（FetchContent gRPC v1.67.1）
命令:
  test ! -f raft/src/tcp_transport.cpp                           → PASS
  cmake -S . -B build                                            → PASS（刷新源列表）
  cmake --build build -j4 --target opskv_raft                    → PASS
  cmake --build build -j4 --target meta_server storage_node      → PASS
  nm build/raft/libopskv_raft.a | rg TcpTransport                → 无
结果: PASS（编译/链接）；集群冒烟未重跑
摘要: TcpTransport 删除并入 GrpcRaftTransport；meta 补 net::parse_host_port include
```

### 2026-10-25

```text
日期: 2026-10-25
执行人: agent
环境: darwin, 工作区 opskv（含 gRPC / 共享引擎修复）
命令:
  (cd web && go test ./internal/lifecycle ./internal/cluster)     → PASS
  ./.run/kv-example 127.0.0.1:19000                              → PASS（SKV_FRESH 后）
  ctest --test-dir build -L unit                                 → 初跑 124/126 FAIL(TSO)
  gtest_filter=ClusterStateTest.AllocTs*:…GcSafePoint*           → PASS（断言已改）
  make e2e / integ-workload / fault-drill / it-scale-live        → SKIP（未跑）
  ./build/tests/opskv_unit_tests --gtest_filter='ScalerTest.*' → PASS（5）
  bash scripts/it-replica-churn.sh                               → PASS（I39）
摘要: Go web 单测绿；清盘后 KV 冒烟绿；C++ TSO 相关 2 例已修；副本 churn 排队/取消已修并自动化。
```

---

## 7. 维护说明

1. **新增 gtest**：放进 `tests/<module>/`，并加入 `tests/CMakeLists.txt` 的 `OPSKV_TEST_SOURCES`；本页 §3 用例数同步 +1。  
2. **新增集成脚本**：在 §4.2 登记，并尽量挂到 `Makefile` / `release-gate`。  
3. **发版前**：至少 `make release-gate`；Region scatter/Merge 数据完整性按 [INTEGRATION_TEST.md](INTEGRATION_TEST.md) I30–I33 人工或脚本卡死。  
4. 覆盖扩展设计见 [flows/IT_COVERAGE_EXPANSION.md](../flows/IT_COVERAGE_EXPANSION.md)；周跑磁盘/分区用 `make integ-extra`。
