# 流程：Raft 选举 / 心跳 / 提交

**适用范围：** Storage 每分片一组 Raft；Meta 一组 Raft。算法同一套 `RaftNode`。

**关键代码：** `raft/src/raft_node.cpp` · `GrpcRaftTransport` · `MultiRaft::tick_all`

```mermaid
flowchart TD
  T[tick 每 ~50–100ms] --> R{当前角色}
  R -->|Leader| HB{heartbeat 到期?}
  HB -->|是| BC[broadcast_heartbeat]
  HB -->|否| T
  R -->|Follower/Candidate| EL{election 超时?}
  EL -->|否| T
  EL -->|是| CA[become_candidate<br/>term++ voted_for=self]
  CA --> SE[start_election<br/>RAFT_VOTE → 其它 voter]
  SE --> VR[on_request_vote_reply]
  VR --> MAJ{yes ≥ majority?}
  MAJ -->|否| T
  MAJ -->|是| LD[become_leader<br/>初始化 next/match_index]
  LD --> BC

  BC --> CHK{peer next_index<br/>&lt; log.first_index?}
  CHK -->|是| SNAP[InstallSnapshot<br/>RAFT_SNAP]
  CHK -->|否| AE[AppendEntries<br/>RAFT_AE]
  SNAP --> T
  AE --> REP[Follower 回 OK AE]
  REP --> UPD[更新 match/next_index]
  UPD --> MC[maybe_commit]
  MC --> AP[apply_committed]
  AP --> T
```

## 提交规则（maybe_commit）

```mermaid
flowchart TD
  A[收集各 voter 的 match_index<br/>含 self] --> B[降序排序]
  B --> C[取 matches 下标 majority-1<br/>例 RF=3 → 第 2 大]
  C --> D{candidate &gt; commit_index<br/>且 entry.term == 当前 term?}
  D -->|是| E[commit_index = candidate]
  D -->|否| F[不推进]
  E --> G[apply_committed<br/>回调 on_apply]
```

## 选票授予（on_request_vote）

```mermaid
flowchart TD
  A[收到 RAFT_VOTE] --> B{term &lt; 本地?}
  B -->|是| N[granted=0]
  B -->|否| C[step_down_if_stale]
  C --> D{日志够新?<br/>last_term/last_idx}
  D -->|否| N
  D -->|是| E{未投票或已投给对方?}
  E -->|否| N
  E -->|是| Y[granted=1<br/>persist voted_for]
```

## RPC 线协议

| 类型 | 请求 | 响应 |
|------|------|------|
| RequestVote | `RAFT_VOTE shard from term last_idx last_term` | `OK VOTE term granted` |
| AppendEntries | `RAFT_AE shard from term prev prev_term commit n …` | `OK AE term success match` |
| InstallSnapshot | `RAFT_SNAP shard from term last_idx last_term hex` | `OK SNAP term success match` |
