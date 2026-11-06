# 全功能自动化集成测试（发版门禁）

每次功能修改后、**发布之前**须全量跑通本清单。失败即停，不得用 skip 蒙混（已知问题用 `XFAIL` 显式登记）。

关联：[TEST_CASES.md](TEST_CASES.md)（用例清单 + 执行记录）· [FAULT_DRILL.md](../ops/FAULT_DRILL.md)（HA / 故障恢复场景）· [`scripts/run-e2e.sh`](../../scripts/run-e2e.sh) · [`scripts/fault-drill.sh`](../../scripts/fault-drill.sh)

## 入口（目标）

```bash
make release-gate          # 单元 + e2e + it-web/region/txn + fault-drill（含 F2）
make test                  # Stage 0 单元
make e2e                   # Stage 1–2 子集
make it-web it-region it-txn
make integ-workload        # 集成：后台多用户 R/W + e2e/txn + 杀 Store
make fault-drill           # Stage 5；含 F2 Leader；全程 bg kv-chaos
make integ-extra           # 周跑：F14 磁盘只读 + F3/F11 分区近似
```

持续负载参数（可选）：`CHAOS_QPS=40 CHAOS_WORKERS=4 CHAOS_WRITE_RATIO=0.5`。

### 持续用户读写说明

| 入口 | 行为 |
|------|------|
| `make integ-workload` | 启集群 → 功能用例 → **后台多 worker Put/Get** → 杀 1 Store → 验收成功率 |
| `make fault-drill` | 演练全程挂 `-duration 0` 的 `kv-chaos` 后台负载，结束输出 `overall_success` |
| `make release-gate` | `ctest` + `run-e2e` + `fault-drill` |

负载工具：`client/go/examples/chaos` → `.run/kv-chaos`  
`-duration 0` 表示直到 SIGTERM；`-workers` 模拟并发用户；`-write-ratio` 控制写占比。

产物建议：`./.run/release-gate.log` + 摘要 `passed/failed`。

默认拓扑：**Meta×3 · Storage×6 · Gateway×1 · Web 可选**（Region RF=3）。

---

## Stage 0 — 构建与静态检查

| ID | 测试点 | 命令 / 内容 | 断言 |
|----|--------|-------------|------|
| B1 | C++ 构建 | `cmake --build build -j`（meta/storage/skv） | 成功 |
| B2 | Go 构建 | gateway、examples、web | 成功 |
| B3 | 前端 | `web/frontend && npm run build && tsc --noEmit` | 成功 |
| B4 | 单元测试 | `make test` / `make gtest` | 全绿 |
| B5 | Sanitizer（夜间可选） | `make memcheck` / asan | 无致命泄漏/UB |

---

## Stage 1 — 集群生命周期

| ID | 测试点 | 断言 |
|----|--------|------|
| I1 | `skv start` / `daemon_cluster`（6 Store） | 端口齐；`GET_ROUTE` OK |
| I2 | `skv status` / Web `/api/overview` | meta=3/3、store=6/6、gw=UP |
| I3 | stop 再 start | 可重复拉起 |
| I4 | 启动后 30s 稳定 | pid/日志无瞬时崩溃 |

---

## Stage 2 — KV / 路由（Gateway）

| ID | 测试点 | 断言 |
|----|--------|------|
| I10 | Put / Get / Del 冒烟 | 读写一致 |
| I11 | 并发 chaos 短跑 | overall_success ≥95% |
| I12 | Locate 多 key | Region 正确 |
| I13 | 线性读 vs 副本读 | 线性走 Leader；stale 可读副本 |
| I14 | not_leader 重试 | 杀 Leader 后 Get 最终成功 |
| I15 | Watch（若启用） | 变更可 poll 到 |

---

## Stage 3 — 事务 / MVCC / TSO

| ID | 测试点 | 断言 |
|----|--------|------|
| I20 | Txn Begin / Get / Prewrite / Commit | 提交可见 |
| I21 | 冲突 Prewrite | 返回 locked / 冲突 |
| I22 | Rollback / Resolve | 锁清除 |
| I23 | 快照隔离读 | 未提交不可见 |
| I24 | TSO 单调 | `next_ts` 递增 |
| I25 | SafePoint / GC（可选） | 水位推进不误删活跃 |

---

## Stage 4 — Region 策略（必卡回归）

| ID | 测试点 | 断言 |
|----|--------|------|
| I30 | Split（无 scatter） | 2 Region；**双侧 key 仍可读** |
| I31 | Split + scatter | 右半到 3/4/5；读写成功 |
| I32 | Place | 路由 peers 与目标一致 |
| I33 | Find-merge-right + Merge | Region 数回落；数据仍在 |
| I34 | Suggest-place | 返回 RF 个节点 |
| I35 | Web `/api/split\|merge\|place` | 与 skv 结果一致 |
| I36 | 分裂窗口写 | `splitting` 可重试成功 |

> **回归重点**：scatter 后 `mmm` / `zzz` 等右半 key 不得读丢失（Gateway 路由 / Leader）。

---

## Stage 5 — Meta / Raft HA

| ID | 测试点 | 断言 |
|----|--------|------|
| I40 | 杀 1 Meta | 数据面 OK；路由仍可 |
| I41 | 杀 1 Store Follower | 数据面 OK；重启追赶 |
| I42 | 滚动重启 Store | 最终 success ≥90% |
| I43 | Gateway 重启 | 恢复后业务 OK |
| I44 | AE 故障注入短跑 | 收敛、不永久失败 |

完整场景表见 [FAULT_DRILL.md](../ops/FAULT_DRILL.md)（F1–F16）。

---

## Stage 6 — 运维台 Web

| ID | 测试点 | 断言 |
|----|--------|------|
| I50 | `/api/overview\|route\|nodes\|topology` | 200 + 字段齐全 |
| I51 | 拓扑软刷新 | 筛选不丢、不闪退 |
| I52 | Region 策略表单 | split/merge/place 日志成功 |
| I53 | KV 调试页 | put/get/locate |
| I54 | 节点上下线（`-run-dir`） | 状态与探测一致 |

---

## Stage 7 — 发布判定

| 条件 | 要求 |
|------|------|
| 必选 ID | Stage 0–6 中未标「可选 / 夜间」的项全部 PASS |
| 日志 | `./.run/release-gate.log` 可追溯 |
| XFAIL | 仅允许清单内显式登记的已知失败，发版说明中写明 |
| 禁止 | 仅跑 unit / 跳过 Region scatter 读写断言即发版 |

---

## 建议脚本编排

```text
scripts/release-gate.sh
  ├── make test / ctest
  ├── scripts/run-e2e.sh
  ├── scripts/it-web.sh               # I50–I54 + Web DS
  ├── scripts/it-region.sh            # I30–I36 scatter 读硬断言
  ├── scripts/it-txn.sh               # I20–I22
  └── scripts/fault-drill.sh          # F1/F2/F4/F5/F8/F10

make integ-extra → it-disk-fault + it-partition   # F14 / F3 / F11-approx
```

设计说明：[flows/IT_COVERAGE_EXPANSION.md](../flows/IT_COVERAGE_EXPANSION.md)

```bash
# 每次功能改完、发版前
make release-gate

# 夜间 / 周
make integ-extra         # 磁盘 + 分区近似
make fault-drill-extra   # fault-drill + integ-extra
make memcheck
```

---

## 现状对照

| 能力 | 状态 |
|------|------|
| `make test` / `gtest` | 有 |
| `make e2e`（`run-e2e.sh`） | 有；偏 3 Store 冒烟 |
| `make fault-drill` | 有；含 **F2**；未覆盖 F 全表 |
| 统一 `release-gate` | **已落地**（unit+e2e+web+region+txn+drill） |
| `it-region.sh`（scatter 读写硬断言） | **有** |
| `it-web.sh` | **有** |
| `it-txn.sh` / `it-disk-fault` / `it-partition` | **有**（后二者周跑） |
| 默认 6 Store 集成拓扑 | start-cluster / skv 默认 6 Store |

---

## 测试点速查（按功能域）

| 域 | ID 前缀 | 关键断言一句话 |
|----|---------|----------------|
| 构建/单测 | B* | 编译 + gtest 绿 |
| 生命周期 | I1–I4 | 启停稳定、overview 齐 |
| KV/路由 | I10–I15 | 一致读写、重试、Locate |
| Txn/MVCC | I20–I25 | 提交可见、冲突、TSO |
| Region | I30–I36 | split/place/merge + **scatter 后可读** |
| HA | I40–I44 | 杀单副本/Meta/GW 可恢复 |
| Web | I50–I54 | API + 拓扑/策略 UI |
| 故障全表 | F1–F16 | 见 FAULT_DRILL |
