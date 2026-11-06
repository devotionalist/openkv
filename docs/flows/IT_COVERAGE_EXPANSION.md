# 集成覆盖扩展（Web / Region / 故障 / Txn）

## 问题

发版门禁长期停留在「单元 + 3-Store 冒烟 + F1/F4/F5/F8/F10」：Web 全链路、Region scatter 读硬断言、Leader 切换（F2）、网络/磁盘类故障、Txn 硬门禁缺少可复跑脚本。

## 方案

| 脚本 | 门禁 ID | 内容 |
|------|---------|------|
| `scripts/it-web.sh` | I50–I54 + Web DS | HTTP 冒烟、KV/Locate、split/merge、节点启停、`/api/ds` wrong_type |
| `scripts/it-region.sh` | I30–I36 | Split → place scatter → 双侧可读 → merge |
| `scripts/it-txn.sh` | I20–I22 | Begin/Commit 可见、冲突、Rollback（失败即非 0） |
| `scripts/it-disk-fault.sh` | F14（+轻量 F15） | 数据目录只读多数派仍服务；修权限后追赶 |
| `scripts/it-partition.sh` | F3 / F11 近似 | 杀 2/3 副本须不可写；`kill -STOP` 冻结近似分区 |
| `scripts/fault-drill.sh` | +F2 | 解析 Region Leader → 杀掉 → 新 `*L` + seed Get |

`make release-gate` 串联：unit → e2e → **it-web → it-region → it-txn** → fault-drill（含 F2）。  
磁盘/分区脚本默认进 `make integ-extra` / `make fault-drill-extra`（破坏性更强，周跑）。

## 边界与失败

- 前置：6 Store 集群已起（`skv status`）；Web 需 `:19100` 且 `-run-dir`（脚本可自拉起）。
- 不清盘除非 `SKV_FRESH=1`；F14 只改 `data/storage-1` 权限，结束后恢复 `chmod`。
- 真 iptables 分区需 root，本仓库用进程 STOP/KILL 近似；文档 F11 仍保留手册步骤。

## 回滚

删除新增脚本、恢复 `release-gate.sh` / Makefile 旧步骤即可；不影响数据面协议。

## 验收

```bash
# 集群已 up + Web（或由 it-web 自启）
bash scripts/it-web.sh          # ALL PASS: it-web
bash scripts/it-region.sh       # ALL PASS: it-region
bash scripts/it-txn.sh          # ALL PASS: it-txn
bash scripts/it-disk-fault.sh   # ALL PASS: it-disk-fault
bash scripts/it-partition.sh    # ALL PASS: it-partition
make fault-drill                # 含 F2；FAULT DRILL PASSED
```

登记：[TEST_CASES.md](../test/TEST_CASES.md) · 场景表：[INTEGRATION_TEST.md](../test/INTEGRATION_TEST.md) · 故障：[FAULT_DRILL.md](../ops/FAULT_DRILL.md)
