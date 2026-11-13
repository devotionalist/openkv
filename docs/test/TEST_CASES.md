# 测试用例清单

本文档汇总仓库内**已落地**的自动化用例与集成/演练脚本（**有什么、怎么跑**）。
发版门禁场景编号（I*/B*/F*）见 [INTEGRATION_TEST.md](INTEGRATION_TEST.md)、[FAULT_DRILL.md](../ops/FAULT_DRILL.md)。
每次压测 / 手工跑分**不必**记入本页；结果写对话或对应 `docs/flows/*` 验收节即可。

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
| Scan / ListPrefix | Gateway MVCC 前缀扫 | `bash scripts/it-scan.sh` |
| Web 全链路 | I50–I54 + `/api/ds` | `make it-web` / `bash scripts/it-web.sh` |
| Region scatter | I30–I36 硬断言 | `make it-region` |
| Txn 硬门禁 | I20–I22 | `make it-txn` |
| 磁盘 / 分区 | F14、F3/F11 近似 | `make integ-extra` |
| 故障演练 | F1/F2/F4/F5/F8/F10 子集；设计见 [DATA_HA.md](../architecture/DATA_HA.md) | `make fault-drill` |
| 发版门禁 | unit + e2e + it-web/region/txn + fault-drill | `make release-gate` |
| Sanitizer | ASan/UBSan | `make memcheck` |
| Gateway 全接口压测 | KV/Txn/Watch/DS 吞吐与分位 | `client/go/examples/bench`（`-mode all`）；见 [17-gateway-api-bench.md](../flows/17-gateway-api-bench.md) |
| 写路径批处理 | Ready/apply/多 Region 验收 W1–W4 | gtest + bench；见 [23-write-path-batching.md](../flows/23-write-path-batching.md) |
| 写路径三刀 | Ready 攒批 / 二进制 ONEPC / workers | gtest + Helm env + bench；见 [25-write-path-knives.md](../flows/25-write-path-knives.md) |
| Raftstore schedule | schedule 唤醒 + drain | gtest + bench；见 [26-raftstore-schedule-pipeline.md](../flows/26-raftstore-schedule-pipeline.md) |
| Raw vs Txn | RawPut/Get/Delete 无 TSO | gtest + bench rawput；见 [27-raw-vs-txn.md](../flows/27-raw-vs-txn.md) |
| C++ Gateway 切流 | `gateway_node` 替换 Go | 构建/镜像/SDK 冒烟；见 [18-cpp-gateway.md](../flows/18-cpp-gateway.md) |

产物建议：`./.run/release-gate.log`、各 `scripts/*.sh` 终端输出。

---

## 2. 执行总览（最近一次）

记录日期：**2026-10-26**（本机；数据面硬化后抽测 SnapWorker* + Coprocessor* + Raftstore* + StoreAdmin* + RawNode* + Scheduler* **18/18 PASS**；见 [flows/DATA_PLANE_HARDENING.md](../flows/DATA_PLANE_HARDENING.md)）。  
上一完整总览：**2026-10-25**。

| 套件 | 规模 | 结果 | 备注 |
|------|------|------|------|
| C++ `openkv_unit_tests` | **128** `TEST*` | **PASS（Scaler 新例）** | `ScalerTest.ConcurrentScaleOutQueues` / `CancelInflight` 新增并通过 |
| Go `web/internal/lifecycle` | 3 | **PASS** | `go test` ~1s |
| Go `web/internal/cluster` | 3 | **PASS** | `go test` ~1s |
| Go `client/go` | — | SKIP | 无 `*_test.go`（`gwput_test.go` 为手工 main） |
| `make e2e` / `run-e2e.sh` | 冒烟 | SKIP | 本次未跑全量 |
| `make integ-workload` | 集成 | SKIP | 本次未跑 |
| `it-scale-live.sh` | I 扩缩容 | SKIP | 本次未跑脚本 |
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
| 手工：Split/Merge 数据 | I30/I33 | **FAIL→已修** | 空 range 快照清空兄弟区间；已禁 |
| 手工：SCALE_OUT storage-5 | — | **FAIL→已修** | 快照 ConfState / 串行扩容 |

---

## 3. C++ 单元测试（gtest）

源码：`tests/{common,raft,storage,meta,compute}/*_test.cpp`  
二进制：`build/tests/openkv_unit_tests`（`gtest_discover_tests`，label=`unit`）

### 3.1 按模块汇总

| 模块 | 文件数 | 用例数 | 关注点 |
|------|--------|--------|--------|
| common | 6 | 19 | 哈希、Range、配置、Admin ACL、meta_rpc、grpc 关机宽限 |
| raft | 11 | 34 | 日志、选主、Joint、快照流、RawNode、raftstore |
| storage | 12 | 47 | KvEngine/快照区间、MVCC（TxnScan/TxnTtl）、StoreFacade、Admin Raft、Watch |
| meta | 4 | 29 | ClusterState、Scaler（含排队/取消）、Scheduler、Meta HA FSM |
| compute | 2 | 8 | Router Locate、Proxy 重试 |
| **合计** | **35** | **137** | |

### 3.2 套件 → 用例名

<details>
<summary>common（19）</summary>

| Suite.Case | 文件 |
|------------|------|
| AdminAclTest.{ClassifyOps,SessionAuth,MetaHandleLineRequiresAuth} | `common/admin_acl_test.cpp` |
| ConfigTest.{LoadNodeConfig,LoadBootstrapPeers,MissingFileThrows} | `common/config_test.cpp` |
| HashTest.{Fnv1aStable,EmptyKey,ShardOfRange,ShardOfZeroCount,ShardDistributionNotCollapsed} | `common/hash_test.cpp` |
| RegionTest.{ContainsAndInf,LocateSorted,CanSplitAt} | `common/region_test.cpp` |
| MetaRpcTest.FormatRoutingTableLineIncludesNrepComma | `common/meta_rpc_test.cpp` |
| GrpcShutdownTest.{GraceMsDefaultWhenUnset,GraceMsFromEnv,GraceMsRejectsGarbage,NullServerNoop} | `common/grpc_shutdown_test.cpp` |

</details>

<details>
<summary>raft（34）</summary>

| Suite | Cases | 文件 |
|-------|-------|------|
| RaftLogTest | AppendAndQuery, AppendEntriesConflictTruncate, AppendEntriesRejectBadPrev, CompactTo, EntriesFromLimit | `raft_log_test.cpp` |
| RaftNodeTest | SingleNodeElectionAndPropose, ProposeRejectedWhenFollower, ConfChangeAddLearner, PreVoteDoesNotBumpTerm, CheckQuorumStepsDownIsolatedLeader, VoteAndAppendBetweenPeers | `raft_node_test.cpp` |
| RaftJointTest / RaftAsyncApplyTest | PromoteUsesEnterThenLeaveJoint, RejectConfWhileJointPending, ProposeAppliesViaWorker | `raft_joint_test.cpp` |
| RaftPersistTest | SaveAndRecoverApplied, InstallSnapshotCompactsAndApplies | `raft_persist_test.cpp` |
| RaftEngineTest | FileLogBatchRoundTrip, EntryCrcRoundTrip, FilePureAppend, FileConflictRewrite, CutAndGc | `raft_engine_test.cpp` |
| SnapStreamTest | HexRoundTrip, AssembleChunks, EmptySnapshot, RejectOutOfOrder | `snap_stream_test.cpp` |
| RaftReadIndexTest / RaftFrameTest | LeaseReadOnSingleNode, SoftStateMetrics, EncodeDecodeRoundTrip | `raft_read_index_test.cpp` |
| RawNodeTest | SingleNodeElectAndPropose, BootstrapThenPropose, ThreeNodeVoteAndProposeReady | `raw_node_test.cpp` |
| MultiRaftTest | GetOrCreateAndLeaders | `multi_raft_test.cpp` |
| RaftstoreTest | StoreTickElectsLeader, HibernateSkipsTickUntilWake, DriveReadyCoalescesPersist, RequestReadyCoalescesByCount, ApplyQosFlag | `raftstore_test.cpp` |
| BatchSystemTest / ScheduleQueueTest | ScheduleDrainsWithoutScan, DedupAndPop | `batch_system_test.cpp` |

</details>

<details>
<summary>storage（45）</summary>

| Suite | 要点 |
|-------|------|
| KvEngineTest | Put/Get/Delete、Snapshot、**SnapshotRangePreservesSibling**（兄弟区间不被空 range 抹掉）、Checkpoint、CF、SyncMode |
| MvccTest | Lock、可见性、WriteConflict、悲观锁、GC、TxnScan、TxnTtl |
| StoreFacadeTest / StoreCodecTest / StoreAdminRaftTest | 端到端 Put、编解码、**PlainCas*** / **MvccCasViaPrewrite** / **TxnOpsAsyncPrewriteCommitResolve**、Epoch、Split seed、幂等 Propose |
| WatchHubTest / WatchLogTest | Watch 过滤与持久化 |
| InflightGateTest / LatencyHistTest / SchedulerTest / CoprocessorTest | 限流、延迟、`lock_async` Latch、前缀扫描 |
| RocksRaftEngineTest / ShardEngineManagerTest | Raft 引擎、单 Store 共享引擎 |

</details>

<details>
<summary>meta（27）</summary>

| Suite | 要点 |
|-------|------|
| ClusterStateTest | TSO、GC safe point、Split/Merge、Joining→Active、Drain、Learner Promote、Place、Snapshot |
| ScalerTest | ScaleOut 完成、未知节点失败、CatchUp 门闩、**ConcurrentScaleOutQueues**、**CancelInflight** |
| RegionSchedulerTest | 提交/恢复任务、扫描欠副本 |
| MetaFsmTest / MetaHaTest | FSM 编解码、单副本 Leader Register、`RegisterNodeAsyncFinishOnApply`、`WatchRouteAsyncAndActivatePrecheck` |

</details>

<details>
<summary>compute（Router + Gateway 出站客户端）</summary>

| Suite | Cases | 最近结果 |
|-------|-------|----------|
| RouterTest | BootstrapSingle, UpdateIgnoresStaleVersion, RangeLocateAfterSplit, ReplicaAddrsAndFallback（含 epoch/leader 优先）, MissingShard | **PASS** 2026-11-12 |
| DsEncodeTest | MetaKeyPrefix, HashRoundTrip, WrongType, SetListZSet | **PASS** 2026-11-12 |
| MetaClientTest / GrpcUtilTest / TsoTest / TsRangeQueueTest / ActiveTsTest | ParseRedirect, ParseRoute*, Alloc*, QueueDrainOrder, TsRangeQueue*, ConcurrentUnique, MinAndExpire | **PASS** 2026-11-12 |
| ~~KvProxyTest~~ | 已随遗留 `KvProxy`/`ComputeNode` 删除 | — |

```bash
# 验收 C1（出站库）：docs/flows/18-cpp-gateway.md
ctest --test-dir build -R 'DsEncodeTest|MetaClientTest|TsoTest|ActiveTsTest|GrpcUtilTest|ParseRedirect|RouterTest' --output-on-failure

# 验收 G1（gateway_node）：docs/flows/18-cpp-gateway.md
cmake --build build -j8 --target gateway_node
# 验收 L1（遗留清理）：无 compute_node / KvProxy 源与目标
```

</details>

单独跑某个套件：

```bash
ctest --test-dir build -R KvEngineTest --output-on-failure
./build/tests/openkv_unit_tests --gtest_filter='KvEngineTest.*'
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

## 5. 维护说明

1. **新增 gtest**：放进 `tests/<module>/`，并加入 `tests/CMakeLists.txt` 的 `OPENKV_TEST_SOURCES`；本页 §3 用例数同步 +1。  
2. **新增集成脚本**：在 §4.2 登记，并尽量挂到 `Makefile` / `release-gate`。  
3. **发版前**：至少 `make release-gate`；Region scatter/Merge 数据完整性按 [INTEGRATION_TEST.md](INTEGRATION_TEST.md) I30–I33 人工或脚本卡死。  
4. 覆盖扩展设计见 [flows/IT_COVERAGE_EXPANSION.md](../flows/IT_COVERAGE_EXPANSION.md)；周跑磁盘/分区用 `make integ-extra`。
5. **不要**把每次手工/压测跑分追加进本页；验收结果写在对话或对应 `docs/flows/*` 验收节即可。
