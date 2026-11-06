# 故障演练计划

验证 OpsKV 在上下线、进程崩溃、网络抖动、磁盘类故障下能否**自动恢复服务**（Raft 选主 / 追赶 / Gateway 重试），以及运维侧如何拉起进程。

- **设计总览：** [DATA_HA.md](../architecture/DATA_HA.md)（数据面 RF=3 + Meta×3、边界与验收 ID）
- 实现流程：[flows/04-fault-recovery.md](../flows/04-fault-recovery.md)（WAL / AE / Snapshot）· [05-meta-ha.md](../flows/05-meta-ha.md)
- 脚本入口：`make fault-drill` → [`scripts/fault-drill.sh`](../../scripts/fault-drill.sh)
- 运维 CLI：[SKV_CLI.md](SKV_CLI.md)
- 发版集成门禁：[INTEGRATION_TEST.md](../test/INTEGRATION_TEST.md)

## 假设拓扑

| 角色 | 数量 | 端口 | 说明 |
|------|------|------|------|
| Meta | 3 | 8500–8502 | 管控面 / TSO / 路由 |
| Storage | 6 | 9100–9105 | Region RF=3；可 place 到不同节点组 |
| Gateway | 1 | 19000 | 客户端入口 |
| Web | 可选 | 19100 | 运维台；演练可用 `skv` / 脚本代替 |

数据目录：`./data/storage-N`（含 RocksDB + `raft_shard_*`）。  
后台负载：`make fault-drill` 启动时自动拉起全程 `kv-chaos`（`-duration 0`，多 worker Put/Get，直到演练结束）。  
也可单独：`./.run/kv-chaos -addr 127.0.0.1:19000 -duration 0 -workers 4 -qps 40`。  
判定：分步成功率、全程 `overall_success`、路由可达、日志 `recovered term=` / `installed snapshot`。

> **注意**：当前 `fault-drill.sh` 默认按 **Storage×3（9100–9102）** 启停；6 Store 场景请先 `python3 scripts/daemon_cluster.py`（或等价）再按本手册手工跑 F2/F3/F9 等。

## 「自动恢复」边界

| 能力 | 是否自动 | 说明 |
|------|----------|------|
| Raft 选主、Follower AE/Snapshot 追赶 | 是 | 进程仍在多数派内时 |
| Gateway `not_leader` 刷路由重试 | 是 | [flows/08-not-leader-redirect.md](../flows/08-not-leader-redirect.md) |
| 进程被 kill 后自动拉起 | **否** | `skv restart` / 演练脚本 / 运维台 |
| 丢 2/3 副本后仍可写 | **否** | 预期不可用，恢复多数派后恢复 |

## 场景总表

| ID | 场景 | 故障中期望 | 恢复后期望 | 自动化 |
|----|------|------------|------------|--------|
| F1 | 杀 1 个 Follower Store | success ≥70% | ≥90%，有追赶日志 | `make fault-drill` |
| F2 | 杀 Region Leader Store | 短暂选主后可写 | 新 `*L`，数据完整 | `make fault-drill` |
| F3 | 杀同 Region 2 副本 | **不可用**（预期） | 恢复多数派后 ≥90% | 手册 |
| F4 | 滚动重启全部 Store | 部分可用 ≥35% | ≥90% | `make fault-drill`（现 0–2） |
| F5 | 杀 1 个 Meta | 数据面 ≥90%；`GET_ROUTE` 可达 | Meta 重加入 | `make fault-drill` |
| F6 | 杀 Meta Leader | 选主后 TSO/路由恢复 | 正常 | 手册 |
| F7 | 杀 2 个 Meta | 管控面不可用；已缓存路由可读 | 恢复多数派 | 手册 |
| F8 | 杀 Gateway | 客户端全失败 | ≥90% | `make fault-drill` |
| F9 | Web 上下线节点 | 拓扑/探测正确 | 节点回集群 | 手册 |
| F10 | AE 丢包/延迟 | success ≥20% 且非永久 0 | ≥85% | `make fault-drill` |
| F11 | Store↔Store 分区 | 多数派侧仍服务 | 愈合后对齐 | 手册 |
| F12 | Gateway↔Meta 断 | 短 Put 可能失败；已有路由可读 | 恢复 | 手册 |
| F13 | Gateway↔Store 断部分 | 走存活副本/重试 | 恢复 | 手册 |
| F14 | 磁盘满 / 数据目录只读 | 该节点写失败；多数派仍可写 | 恢复后追赶 | 手册 |
| F15 | 损坏 Raft/Rocks 后重启 | snapshot 或 DESTROY | 集群仍服务 | 手册 |
| F16 | `kill -9` 强杀 | 同对应角色场景 | WAL 装载恢复 | `skv kill` / drill |

## 执行节奏

| 频率 | 范围 | 命令 |
|------|------|------|
| 发版前 | F1、**F2**、F4、F5、F8、F10 | `make fault-drill` |
| 周跑 | + F3/F11 近似、F14、F6、F16 | `make integ-extra` + 手册 |
| 人工专项 | F7、F9、F12–F13、F15 强毁 | 按手册操作，填「结果记录」 |

---

## 通用前置

```bash
cd "$SKV_ROOT"   # 仓库根
cmake --build build -j --target meta_server storage_node skv
(cd client/go && go build -o .run/kv-chaos ./examples/chaos \
  && go build -o .run/kv-example ./examples/basic)

# 推荐 6 Store 拓扑（手工场景）
python3 scripts/daemon_cluster.py
./build/tools/skv status
./build/tools/skv route          # 记 Region peers 与 *L
./build/tools/skv who-leader     # Meta Leader
```

辅助：

| 目的 | 命令 |
|------|------|
| 找 Region Leader | `skv route` → 副本名带 `*L` |
| 找 Meta Leader | `skv who-leader` |
| 强杀 | `skv kill storage-N` / `meta-N` / `gateway` |
| 拉起 | `skv restart <name>` |
| 短冒烟 | `./.run/kv-chaos -addr 127.0.0.1:19000 -duration 3s -qps 40` |
| 看恢复日志 | `grep -E 'recovered term=|installed snapshot|compacted to' .run/storage-N.log` |

---

## 已脚本覆盖（F1 / **F2** / F4 / F5 / F8 / F10 / F16；另见 `it-disk-fault` / `it-partition`）

`scripts/fault-drill.sh` 顺序：

1. 基线 ≥95%
2. **F1**：杀 `storage-1`（Follower 假设）→ 期间 ≥70% → restart → ≥90%
3. **F5**：杀 `meta-1` → 数据面 ≥90% + `GET_ROUTE` → restart meta-1
4. **F8**：杀 gateway → 客户端失败 → restart → ≥90%
5. **F4**：滚动杀/启 storage-0..2 → 滚动期间 ≥35% → 最终 ≥90%
6. **F10**：（暂缓）原 `SKV_FAULT_*_AE_*` 随 TcpTransport 删除；待 gRPC 出站注入恢复
7. Watch + stale_read 冒烟

`skv kill` 使用 SIGKILL，覆盖 **F16** 进程侧。

日志：`./.run/fault-drill.log`。

---

## F2 — 杀 Region Leader Store

**目的**：验证选主 + Gateway `not_leader` 重试，数据不丢。

### 步骤

1. `skv route`，记下目标 Region（通常 `0`）的 Leader，例如 `storage-0@…*L`。
2. 种子：`skv put f2key f2v`；`skv get f2key` 确认。
3. 启动负载：`kv-chaos … -duration 25s -qps 40`（后台）。
4. `skv kill storage-0`（换成实际 Leader 名）。
5. 等待 8–15s（选主窗口）。
6. `skv route`：原节点不再是 `*L`，另有副本带 `*L`。
7. `skv get f2key` 成功且值为 `f2v`；chaos 后半段成功率回升。
8. `skv restart storage-0`；检查追赶日志；最终 smoke ≥90%。

### 通过标准

| 项 | 标准 |
|----|------|
| 选主 | 故障后 ≤15s 出现新 `*L` |
| 读 | `f2key` 可读且值正确 |
| 写 | 选主后 Put 成功（允许短暂 not_leader） |
| 追赶 | 旧 Leader 重启后有 recover/AE/snapshot 痕迹 |

### 失败常见原因

- Gateway 未重试 / 路由缓存过期不刷新 → 见 flows/08。
- 杀的是 Follower 而非 Leader → 改杀 `*L` 重跑。

---

## F3 — 杀同 Region 2 副本（预期不可用）

**目的**：确认多数派丢失时正确降级，**不得**误报 PASS。

### 步骤

1. `skv route` 取 Region 0 的三个 peers，记为 A/B/C（含 Leader）。
2. 基线 smoke ≥95%。
3. `skv kill A`；`skv kill B`（保留 C）。
4. 立即 smoke / `skv put f3k v`：**期望失败或 success≈0**（记录「不可用窗口」起止时间）。
5. `skv restart A`（恢复多数派 2/3）；等待追赶 10–20s。
6. smoke ≥90%；`skv put/get f3k` 成功。
7. `skv restart B`；三副本对齐。

### 通过标准

| 阶段 | 标准 |
|------|------|
| 2 副本挂掉 | **必须**写失败 / success 接近 0（PASS = 正确降级） |
| 恢复 1 副本后 | 恢复可写；数据与故障前一致或仅丢失未提交 |
| 禁止 | 2 副本挂着仍「全绿」却标 PASS |

---

## F6 — 杀 Meta Leader

### 步骤

1. `skv who-leader` → 例如 `meta-0` / 地址 `127.0.0.1:8500`。
2. 数据面负载后台跑。
3. `skv kill meta-0`（实际 Leader）。
4. 5–10s 内：`skv who-leader` 指向另一节点；`skv route` / `GET_TS`（或 `skv put` 需 TSO 的路径）恢复。
5. 数据面 smoke 应仍 ≥90%（存算分离）。
6. `skv restart meta-0`。

### 通过标准

- 新 Meta Leader 选出；`GET_ROUTE` / `who-leader` 成功。
- 数据面不因 Meta 换主而长期中断。

---

## F7 — 杀 2 个 Meta（管控面不可用）

### 步骤

1. `skv who-leader`；记下三个 meta。
2. `skv kill` 其中两个（含原 Leader）。
3. **管控面**：`skv route` / `who-leader` / 新 Locate **期望失败**。
4. **数据面**：若 Gateway 仍有 Region 路由缓存，已有 key 的 Get/Put 可能仍成功；无缓存的新 Locate 失败。分别记录。
5. `skv restart` 被杀的两个 meta；等多数派；`skv route` 恢复。
6. smoke ≥90%。

### 通过标准

| 阶段 | 标准 |
|------|------|
| 仅 1 Meta | 管控命令失败 = PASS（预期） |
| 恢复后 | route / TSO / 写读正常 |

---

## F9 — Web 运维台上下线

前置：Web 监听 `19100`，`-run-dir .run`。

### 步骤

1. `GET /api/overview` → storage_up 齐全。
2. 经 UI 或 `POST` 等价 API stop 一个 Store（或 `skv kill storage-2` + 刷新）。
3. `/api/topology` / `/api/nodes`：该节点 DOWN；筛选/软刷新不丢状态。
4. start / `skv restart storage-2`。
5. overview 中该节点回 UP；Region 副本探测正常。

### 通过标准

- 探测状态与真实端口一致；无空白页 / 筛选被重置（软刷新）。

---

## F10 — AE 故障注入（补充说明）

> **状态（2026-10-26）**：随 `TcpTransport` 删除，`SKV_FAULT_DROP_AE_PCT` / `SKV_FAULT_DELAY_AE_MS` 已不再生效。F10 待在 `GrpcRaftTransport` 上重新注入后再启用本小节。

---

## F11 — Store↔Store 网络分区

**目的**：一侧仍具多数派时可服务；愈合后日志对齐。

macOS 本机可用 `pf`（需 root）或临时用 **进程暂停** 近似：`kill -STOP <pid>` / `kill -CONT <pid>`（非真实分区，但可验证「少一票」行为）。

### 推荐步骤（STOP 近似）

1. Region peers = S0,S1,S2；Leader=S0。
2. `kill -STOP $(cat .run/storage-2.pid)`（冻结一个 Follower）。
3. smoke：仍应可用（2/3）。
4. `kill -STOP $(cat .run/storage-1.pid)`（再冻一个 → 仅 Leader）：**期望写失败**（同 F3）。
5. `kill -CONT` 两者；追赶后 smoke ≥90%。

### 真实分区（Linux / 有 pf 时）

- 阻断 `9100↔9102` 双向 TCP（保留 Gateway↔Leader）。
- 断言同 STOP 方案；愈合后 `recovered` / AE 追赶。

### 通过标准

- 仅冻/隔 1 副本：可写。
- 冻/隔 2 副本：不可写。
- CONT/通网后三副本 commit 对齐。

---

## F12 — Gateway↔Meta 网络中断

### 步骤

1. 确认 Gateway 已拉取过 route（先成功 `put/get`）。
2. 阻断 Gateway 到 8500–8502（或 `kill -STOP` 全部 meta 进程作近似——注意与 F7 重叠；更净：仅 STOP meta，不杀）。
3. **已缓存 Region**：Put/Get 可能仍成功。
4. **强制刷新路由**：重启 Gateway 或清缓存后再写 → 期望失败。
5. 恢复 Meta；Gateway 重试 / 重启后 route 恢复；smoke ≥90%。

### 通过标准

- 无 Meta 时新路由不可得；恢复后管控面+数据面正常。

---

## F13 — Gateway↔部分 Store 中断

### 步骤

1. `skv route` 记 Leader 与 Follower 端口。
2. 阻断 Gateway→某个 **Follower** 端口（或 STOP 该 storage）：线性读/写应仍走 Leader 成功。
3. 阻断 Gateway→**Leader** 端口：期望 not_leader / 重试到新 Leader 或短暂失败后恢复（若仅断网未杀进程，选主可能不发生——此时应改杀 Leader 或同时 STOP Leader 以触发选举）。
4. 恢复连通；smoke ≥90%。

### 通过标准

- 断 Follower：业务几乎无感。
- 断 Leader：重试或选主后恢复；无永久挂死。

---

## F14 — 磁盘满 / 数据目录只读

**警告**：在临时副本目录演练；勿对唯一生产数据做不可逆操作。

### 步骤（只读目录）

1. `skv kill storage-1`。
2. `chmod a-w data/storage-1`（或对其父目录）。
3. `skv restart storage-1` → 期望该节点日志报错 / 无法正常 apply；**不要**让它单独组成「假健康」。
4. 对 Region 多数派（0+2）smoke：应仍 ≥70%。
5. `skv kill storage-1`；`chmod u+w data/storage-1`；再 restart；追赶；smoke ≥90%。

### 步骤（磁盘满近似）

```bash
# 在 data/storage-1 下填满一块配额盘或用用户配额；演示可用巨大文件占满测试分区
dd if=/dev/zero of=data/storage-1/FILL bs=1m count=…   # 按测试盘大小调整
```

写失败后删 `FILL`，重启追赶。

### 通过标准

- 坏盘节点退出服务能力；集群靠多数派继续。
- 修复权限/空间后该节点可追赶，不污染已提交数据。

---

## F15 — 损坏 Raft / Rocks 后重启

对齐 [flows/04-fault-recovery.md](../flows/04-fault-recovery.md)。

### 步骤（落后窗口内 — 可 AE 追赶）

1. 负载写入期间 `skv kill storage-1`。
2. 继续写入 5–10s（拉大落后）。
3. `skv restart storage-1` → 日志 `recovered term=` + AE 追赶；smoke ≥90%。

### 步骤（破坏日志，逼 Snapshot）

1. `skv kill storage-1`。
2. 删除或截断 `data/storage-1/raft_shard_*/` 下部分 `log.entries` / 状态文件（**仅演练环境**）。
3. `skv restart storage-1`。
4. 期望：Leader `InstallSnapshot` → Follower `installed snapshot` / `apply_snapshot`；或节点无法恢复需 **DESTROY + 重新加入**（记录实际路径）。
5. 确认多数派始终可服务；修复后三副本可读一致。

### 通过标准

| 结果 | 判定 |
|------|------|
| Snapshot 追赶上且数据一致 | PASS |
| 需 DESTROY 重建且文档/手册有步骤 | PASS（记入手册缺口跟踪） |
| 静默数据错乱 / 无日志 | FAIL |

---

## F4 扩展 — 6 Store 滚动

脚本目前只滚 0–2。手工全量：

```bash
for i in 0 1 2 3 4 5; do
  ./build/tools/skv kill storage-$i
  sleep 2
  ./build/tools/skv restart storage-$i
  sleep 3
done
```

若 Region 经 scatter 落在 3/4/5，滚动必须覆盖这些节点，否则演练无效。

---

## 结果记录模板

每次专项演练复制填写：

```text
日期:
拓扑: meta=3 storage=6/3 gateway=1
场景 ID:
操作摘要:
故障中指标: success=% / 错误样例=
恢复后指标: success=%
日志关键行: (recovered / snapshot / not_leader)
结论: PASS | FAIL | XFAIL(原因)
执行人:
```

建议归档：`./.run/fault-drill-records/YYYYMMDD-Fxx.txt`。

---

## 通过标准（汇总）

1. 脚本场景：以 `fault-drill.sh` 阈值为准；详情进 `./.run/fault-drill.log`。
2. 预期降级场景（F3/F7/F11 双冻结）：**不可用即 PASS**，可用反而要查。
3. Storage 重启：尽量见到 `recovered term=` 或 snapshot 相关行。
4. 发版：至少 `make fault-drill` 全绿；周跑补 F2/F6；硬件/网络类按变更触及与否选跑。

## 脚本与文档对照

| 文档 ID | `fault-drill.sh` | 手册本节 |
|---------|------------------|----------|
| F1 | ✓ 杀 storage-1 | — |
| F2 | ✓（解析 `*L` / Web route） | ✓ |
| F3 | ✗ | ✓ |
| F4 | ✓ 滚 0–2 | ✓ 6 Store 扩展 |
| F5 | ✓ 杀 meta-1 + step9 热重加入 ≥85% | — |
| F6 | ✗ | ✓ |
| F7 | ✗ | ✓ |
| F8 | ✓ | — |
| F9 | ✗ | ✓ |
| F10 | ✓ | ✓ 环境变量 |
| F11–F15 | ✗ | ✓ |
| F16 | ✓（kill -9） | — |

### Meta 从节点重加入（已修复）

**原根因**：compact 开着但无 `ClusterState` 快照 / `RAFT_SNAP` → `bytes=0` 风暴 → TSO `not_leader`。

**已落地**：
1. Meta `data_dir` + `persist_dir=…/raft`（配置见 `conf/bootstrap.conf` / `meta-*.conf`）
2. `ClusterState::serialize` / `load_snapshot` + Meta `snap_provider`/`snap_applier` + `RAFT_SNAP` 处理；`raft_compact_threshold=256`
3. Raft：无 provider 不发/不 compact；有 provider 时 snapshot **限流**（默认 1s）；`snap_applier` 失败则拒绝安装
4. `fault-drill` step9：**要求** meta-1 rejoin 后 success ≥85%

后续若落地 `make fault-drill-full`，应按上表把手册步骤自动化，而不是删减断言。
