# 各节点错误码梳理

**状态：** 与实现对齐的目录（字符串 token，**无**统一数值 ErrorCode 枚举）  
**读者：** 客户端 / Gateway / 运维 / 联调  
**协议前缀：** `OK …` / `ERR <token> [k=v…]`（见 `common/include/opskv/protocol.hpp`）  
**相关：** [NETWORKING.md](../NETWORKING.md) · [08-not-leader-redirect.md](../flows/08-not-leader-redirect.md) · [DATA_STRUCTURES.md](DATA_STRUCTURES.md) §4

---

## 1. 模型

| 层 | 错误形态 |
|----|----------|
| Storage / Meta 行协议 & 多数 Admin/KV RPC | `Status::OK` + body `ERR <token>…`，或 proto 字段 `error=` / `ok=false` |
| gRPC 状态码 | 主要用于 **鉴权**、**DS**、传输；KV/Txn **不以**数值码区分业务错 |
| Gateway → Client | 解析 token；DS 再映射为 gRPC `InvalidArgument` / `FailedPrecondition` / `DataLoss` / `Internal` |

**客户端策略速查：**

| 类 | Token 例 | 动作 |
|----|----------|------|
| **Redirect** | `not_leader`（可带 `redirect=`） | 跟 hint；空则刷路由 |
| **软重试** | `busy class=*`、`splitting`、`epoch_not_match`、`apply_timeout`、`target_not_caught_up` | 退避 + 刷路由 |
| **换 TS 重试** | `write_conflict` | 新 TSO 重写 |
| **锁** | `locked`（可带 `primary=` / `lock_ts=`） | CheckTxnStatus → Resolve |
| **鉴权** | `unauthorized` / gRPC `UNAUTHENTICATED` | 修 token，勿盲重试 |
| **应用失败** | `wrong_type`、参数错、`key_not_in_region`（刷路由后仍错） | 回调用方 |
| **运维拓扑** | `unknown_node`、`still_has_regions`、`unreachable`、`*_failed` | 按 SOP；条件重试 |
| **Peer 内部** | `destroyed`、`snapc`、`ae_*`、`step` | Raft 对端逻辑，非业务客户端 |

---

## 2. Storage — 数据面

来源：`store_service` → `exec_line_handler` / gRPC KV·Txn。

| Token | 典型全文 | 何时 | 客户端 |
|-------|----------|------|--------|
| `no_region` | `ERR no_region` | 本机无该 Region peer | 刷路由 / 换 Store |
| `not_leader` | `ERR not_leader redirect=<addr>` | 非 Leader | **跟 redirect** |
| `epoch_not_match` | `ERR epoch_not_match expected=<n>` | 客户端 epoch 过期 | 刷路由 / bump epoch / 重试 |
| `splitting` | `ERR splitting` | Split 写暂停窗 | 短睡 + 刷路由重试 |
| `key_not_in_region` | `ERR key_not_in_region` | key 不在本区间 | locate / 刷路由 |
| `propose_failed` | `ERR propose_failed` | Raft propose 拒 | 条件重试 |
| `apply_timeout` | `ERR apply_timeout` | 等 apply 超时 | 重试 |
| `no_engine` | `ERR no_engine` | 缺引擎 | ENSURE 后重试 |
| `locked` | `ERR locked` 或 `ERR locked primary=<hex> lock_ts=<n>` | 他事务持锁 | Resolve 路径 |
| `write_conflict` | `ERR write_conflict` | MVCC 冲突 | **新 start_ts** 重试 |
| `lock_not_found` | `ERR lock_not_found` | Commit 无匹配锁 | 失败 / resolve |
| `pessimistic_not_prewritten` | `ERR pessimistic_not_prewritten` | 仅悲观锁未 prewrite | 失败 |
| `read_index_failed` | （常表现为 not_leader） | ReadIndex 失败 | 重试 / redirect |
| `prewrite_failed` 等 | 行层空 `r.error` 回退名 | Txn 步失败 | 按上表细分 |

Apply 解码失败等：`bad_command` / `bad_prewrite` / `bad_commit` / `bad_admin_split` / `unknown_op`（经 propose waiter 以同名 `ERR` 返回）。

**CHECK_TXN_STATUS 成功态（非 ERR）：** `locked` \| `expired` \| `committed` \| `rolledback`。

锚点：`storage/src/store_service.cpp` · `exec_line_handler.cpp` · `storage_node.cpp`（`on_apply`）。

---

## 3. Storage — 行协议门禁 / 解析

| Token | 何时 | 客户端 |
|-------|------|--------|
| `empty` | 空行 | 失败 |
| `usage: AUTH <token>` | AUTH 参数错 | 失败 |
| `unauthorized` | 缺/错 admin token | 鉴权 |
| `busy class=client\|raft\|admin\|bypass` | Inflight 闸满 | **退避重试** |
| `bad_hex` / `bad_peer` / `bad_request` | 解析 / 未知 op | 失败 |
| `ae_trunc` / `ae_bad_hex` / `snap_bad_hex` | Raft 入站解析 | Peer 重发 |

Inflight 类名：`storage/include/opskv/storage/inflight_gate.hpp`。

---

## 4. Storage — Admin / Place / 转主

| Token | 何时 | 客户端 / 编排 |
|-------|------|----------------|
| `not_leader` (+ redirect) | Admin 打到 Follower | 跟 Leader |
| `no_region` | 无 Group | ENSURE |
| `busy class=joint` | Joint ConfChange 未退出 | **短等重试** |
| `add_learner_failed` / `promote_failed` / `remove_failed` | ConfChange 失败 | 条件重试 |
| `unknown_peer` / `unknown_target` | 目标不在成员表 | 修成员 / 重加 Learner |
| `target_is_learner` | 转主目标仍是 Learner | 先 Promote |
| `lost_leadership` | 转主中丢主 | 重试 |
| `target_not_caught_up` | 目标 lag | 等追赶 / 重试 |
| `abdicate_failed` / `campaign_failed` | 让位 / 拉票失败 | 重试 |
| `destroyed` | DESTROY 后迟到 RAFT_* | 丢弃（防脑裂） |
| `snapc <term>` | Snap chunk 拒 | Peer 重发 |
| `local_ensure` / `remote_ensure …` | Split seed ENSURE | 查对端 |
| `checkpoint_failed path=…` | Rocks checkpoint | 失败 |
| `place_failed` | Place 编排失败 | 查日志重试 |

见 [10-region-placement.md](../flows/10-region-placement.md)、[DATA_PLANE_HARDENING.md](../flows/DATA_PLANE_HARDENING.md)（`target_not_caught_up`）。

---

## 5. Meta

### 5.1 管控行 / Admin

| Token | 何时 | 动作 |
|-------|------|------|
| `not_leader` (+ redirect) | 写打到 Follower | **跟 Meta Leader** |
| `propose_failed` / `apply_timeout` / `raft_not_ready` | Meta Raft | 重试 |
| `empty` / `unauthorized` / AUTH usage | ACL | 鉴权 |
| `bad_heartbeat` / `bad_hex` / `bad_role` / `bad_request` | 参数 | 失败 |
| `not_found` | LOCATE 无 Region | 失败 |
| `unknown_node` / `no_address` | 节点表 | 修 id |
| `not_pending` | JOIN 时非 Joining/Offline | 状态不对 |
| `unreachable` / `unreachable ping=…` | JOIN 前 PING 失败 | 等 Ready / 网络 |
| `no_heartbeat` / `heartbeat_stale age_ms=…` | JOIN 心跳未就绪 | 等 HB |
| `still_active` | 踢出时仍 Active | 先 scale-in |
| `still_has_regions` | 踢出时仍有副本 | 迁光 |
| `split_apply_missing` | Split 后缺右片 | 失败 / 重试 |
| `merge_lost_left` / `merge_right_still_present` | Merge 后校验 | 失败 |
| `no_adjacent` | Merge 无右邻 | 失败 |
| `empty_peers` | PLACE 空成员 | 失败 |
| `no_scheduler` / `no_scaler` / `no_task` | 组件 / 任务缺失 | 运维 |
| `cancel` | 取消迁移失败 | 失败 |
| Raft 入站 | `ae_*` / `snap_bad_hex` / `snapc` | Peer |

锚点：`meta/src/meta_node.cpp`。K8s JOIN 常见表：[K8S_SCALE.md](../flows/K8S_SCALE.md)。

### 5.2 Scaler / RegionScheduler（任务 `error=` 字段）

| Token | 组件 | 何时 |
|-------|------|------|
| `unknown_node` / `node_not_active` / `node_gone` | Scaler | 目标非法 / 非 Active / 消失 |
| `cancelled` | Scaler | 已取消 |
| `add_learner_failed` / `promote_failed` / `bump_route_failed` | Scaler | Storage / 路由步失败 |
| `duplicate_inflight` | Scaler | 历史并发；现多改排队 |
| `no_placement` / `already_migrating` / `place_exhausted` / `commit_place_failed` | Scheduler | 无方案 / 重复 / 重试耗尽 |

`ScaleTaskState` 是任务状态枚举，**不是**错误码；失败原因仍在 `error` 字符串。

---

## 6. Raft peer（`raftstore/store`）

| Token | 何时 |
|-------|------|
| `no_region` | 无 peer |
| `bad_vote` / `bad_ae` / `bad_snap` | 帧 arity |
| `ae_trunc` / `ae_bad_hex` / `snap_bad_hex` | 载荷 |
| `step` | `RawNode::Step` 异常 |
| `unknown_raft_type` | 类型非法 |

业务客户端一般看不到；属 Store↔Store。

---

## 7. Gateway / Go 客户端映射

| 线协议 / 模式 | 映射 | 行为 |
|---------------|------|------|
| `not_leader` | `*NotLeaderError` | `withLeader` 跟 redirect |
| `locked primary=…` | `*LockedError` | Txn Resolve |
| 其它 `ERR …` | `fmt.Errorf` 透传 | 按 token 策略 |
| `no_region` / dial / Unavailable | skip peer | 试下一副本 |
| `splitting` / `epoch_not_match` | — | 刷路由 / bump epoch |
| `write_conflict` | — | Put 换 TSO 重试 |

锚点：`client/go/internal/storagetcp/client.go` · `compute-gateway/{main,txn,ds}.go` · `route/range.go`。

C++ 遗留 stub：`no_route`、`stub_transport`（`compute/`）。

---

## 8. DS（Gateway `DSService`）

| 情况 | gRPC | 消息 |
|------|------|------|
| 空 key / 参数 | `InvalidArgument` | `key required` 等 |
| Meta 类型不符 | `FailedPrecondition` | 含 `wrong_type`（`have=… want=…`） |
| blob 损坏 | `DataLoss` | `corrupt_ds_blob` |
| 底层存储/事务失败 | `Internal` / 透传 | 含上表 Storage token |

详见 [DATA_STRUCTURES.md](DATA_STRUCTURES.md) §4。

---

## 9. gRPC Status（运维向）

| Status | Message | 节点 |
|--------|---------|------|
| `UNAUTHENTICATED` | `unauthorized` | Storage / Meta Admin |
| `UNIMPLEMENTED` | `ExecLine: no line handler` | Storage 未挂行处理 |

---

## 10. 文档与验收

| 用途 | 文档 / 命令 |
|------|-------------|
| not_leader 流程 | [08-not-leader-redirect.md](../flows/08-not-leader-redirect.md) |
| Place 日志里的 ERR | [10-region-placement.md](../flows/10-region-placement.md) |
| JOIN 失败 | [K8S_SCALE.md](../flows/K8S_SCALE.md) · [DATA_SCALE.md](DATA_SCALE.md) |
| 集成 | I14 not_leader · I36 splitting · `it-ds-*` wrong_type |

**验收（目录完整性）：** 本文 token 表与 `store_service` / `meta_node` / `exec_line_handler` 主要 `ERR` 出口一致；新增对外错误须同步改本页并注明客户端动作。

登记可选：在 [TEST_CASES.md](../test/TEST_CASES.md) 联调说明中链到本页。
