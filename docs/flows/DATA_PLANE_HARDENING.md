# 数据面做扎实（§3：拆薄 / Coprocessor / Snap / 转主）

## 问题

raftstore Ready 路径已固定，但周边仍偏「骨架」：

| 项 | 缺口 |
|----|------|
| 拆薄 | `StorageNode` 组装偏胖；职责边界靠约定 |
| Coprocessor | 仅 range + value 前缀 |
| Snap | 队列满即丢；无同 Region 合并 / 失败重试 / 可观测 |
| 转主 | `abdicate`+`CAMPAIGN` 有；**未先等目标追平** |

## 方案

### 模块边界（拆薄约定）

```
Client/Admin
    │
    ▼
StorageNode          —— 接线：gRPC/ExecLine、Region 元数据、Admin 编排
    ├── Scheduler    —— 仅 key latch
    ├── RaftKV       —— propose / 线性读入口 → Store
    └── Store (raftstore)
            ├── BatchSystem / Peer mailbox
            ├── Ready → persist / send / Apply
            └── SnapWorker —— 快照独立队列
```

不在本迭代做大规模文件拆分；以边界文档 + 行为硬化为准。

### Coprocessor

在 scan 上增加：`key_prefix`、`keys_only`、`count_only`；经 `ADMIN_COP` 暴露。

### SnapWorker

- 同 `region_id` **合并**队列中未发送任务  
- 发送失败 **有限次重试**  
- **Stats**：enqueued / dropped / coalesced / sent / failed / retried  

### 转主

`ADMIN_TRANSFER_LEADER`：让位前轮询 `match_index(target) >= commit_index`；超时 `ERR target_not_caught_up`；再 `abdicate` → 目标 `ADMIN_CAMPAIGN` → 等新主。

## 失败与回滚

- Snap 重试耗尽：打 WARN，计数 `failed`；上层可再触发 snapshot  
- 转主追平超时：不 abdicate，返回错误，原 Leader 保持  
- 回滚：恢复本提交前 SnapWorker / transfer / Coprocessor 行为即可  

## 验收

```bash
./build/tests/opskv_unit_tests --gtest_filter='SnapWorker*:Coprocessor*:RawNode*:StoreAdmin*'
# 期望全部 PASS；Storage 启动日志含 raftstore=BatchSystem+Ready
```

登记：[TEST_CASES.md](../test/TEST_CASES.md) · Backlog：[BACKLOG.md](../BACKLOG.md) §3 → 已完成。
