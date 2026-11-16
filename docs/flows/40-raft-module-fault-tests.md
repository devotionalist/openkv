# Raft 协议核故障场景（gtest）

**状态：** A–H 固定矩阵 + X 意外/多轮混沌；本机内存多节点，不依赖 OpenKV 进程  
**相关：** [06-raft-election-commit](06-raft-election-commit.md) · [35-raft-module-call-flows](35-raft-module-call-flows.md)

## 怎么跑

```bash
make raft-fault-local              # 全套一轮
bash scripts/raft-fault-local.sh 10              # 全套重复 10 轮
bash scripts/raft-fault-local.sh 20 'RaftFaultScenario.X*'   # 只压混沌
```

## 清单

| 组 | 内容 |
|----|------|
| A–H | 选主/复制/Quorum/愈合/Joint/持久化/注入/本机分区抖动 |
| **X1** | Follower 反复 isolate/heal |
| **X2** | AE 丢包开关振荡 |
| **X3** | 双节点同时 force_election |
| **X4** | 反复 abdicate 再收敛 |
| **X5** | 延迟 + 隔离组合 |
| **X6** | RPC 重复投递 |
| **X7** | 旧 Leader 愈合后不双主 |
| **X8** | 只丢 Vote、AE 仍可复制 |
| **X9** | 全员隔离再全愈 |
| **X10** | 随机混沌 80 轮（固定种子；分区中允许短暂双主角色，无隔离/愈合检查点要求 ≤1） |
| **X11** | 多种子短混沌 |
| **X12** | 全丢回包卡住提交，恢复后可写 |
| **X13** | 隔离 Leader：少数派不能提交 |
| **X14** | PreVote + 抖动 |
| **X15** | 多种子愈合后必可写 |

不变量：

- 有分区时：节点角色上可能短暂出现 2 个 Leader（旧主未收到更高 term / CheckQuorum 尚未踢下）；这不等于双主可提交。
- 无隔离且故障清干净后：Leader≤1，且多数派能 apply。

## 验收

| ID | 标准 |
|----|------|
| V1 | `RaftFaultScenario*` 一轮全绿 |
| V2 | `raft-fault-local.sh 5` 全套重复 ≥5 轮全绿 |
| V3 | `raft-fault-local.sh 10 'RaftFaultScenario.X*'` 混沌重复全绿 |

未跑 `-mode all`：仅 Raft 模块测试。
