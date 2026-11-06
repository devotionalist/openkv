# 扩容 / 缩容 SOP（业务持续读写 · 无感）

**目标：** 在用户仍在 put/get 的情况下完成 Store **扩容**与**缩容**，业务侧通过短超时 + 重试做到读写近似无感。

设计总览：[DATA_SCALE.md](../architecture/DATA_SCALE.md) · [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md) · [扩容缩容设计.md](../flows/扩容缩容设计.md)  
命令速查：[SCALE_OUT_SOP.md](SCALE_OUT_SOP.md)（无流量的分步演练）  
自动化：`scripts/it-scale-live.sh`

---

## 0. 原则

| 原则 | 说明 |
|------|------|
| RF 恒定 | 默认扩容 / 缩容都是 **Replace peer**，不抬副本数 |
| 先 Active 再迁 | 新 Store 必须 `join` → Active 后才能 `balance` / `place` |
| Draining 勿杀 | 缩容节点保持进程存活，直到 `Tombstone` |
| 客户端可重试 | 迁主 / ConfChange 窗口可能 `not_leader` 或超时；**短超时 + 重试** = 无感 |
| 不靠 Split 腾盘 | Split 只切元数据；腾盘靠 Place / balance / scale-in |

**无感定义（本仓库 IT）：**

- 后台持续 `put` + `get`（同 key 写后读回）
- 单次 RPC 超时约 2s，put/get 各重试数次
- 全程 `fail_ratio ≤ 0.35`（可配），且成功次数足够
- 结束后种子 key / 抽样 live key 仍可读；路由无已缩容节点

---

## 1. 前置

```bash
./build/tools/skv status          # up=10
./build/tools/skv nodes storage   # 0–2 Active；3–5 Joining
./build/tools/skv route
```

| 项 | 默认 |
|----|------|
| Meta | `127.0.0.1:8500–8502` |
| Storage | `9100–9105` |
| Gateway | `19000` |
| 扩容目标 | `storage-3`（Joining） |
| 缩容目标 | `storage-0`（Active，有副本） |

干净环境：

```bash
./build/tools/skv stop
rm -rf data          # 若 start --fresh 报 Directory not empty
./build/tools/skv start --fresh
sleep 3
./build/tools/skv put bootstrap ok
```

---

## 2. 一键自动化（推荐验收）

```bash
# 集群已 start --fresh 且可写
bash scripts/it-scale-live.sh
```

脚本顺序：

1. seed + `split 0 m`（≥2 Region）
2. **启动后台流量**（贯穿全程，put/get 带短超时与重试）
3. **扩容**：`join storage-3` → 循环 `balance` 直到该节点 `regions≥1`
4. **预备**：`join storage-4` 作为额外空闲 Active（缩容 Replace 补位）
5. **缩容**：自动挑选与 spare 重叠少的 Store → `scale-in` → `Tombstone` → `remove-tombstone`
6. 停流量，校验 `fail_ratio`、KV、最终路由

本地验收（2026-10-22）：`ALL PASS`，全程 `fail_ratio=0.000`（limit 0.35）。

环境变量（可选）：

| 变量 | 默认 | 含义 |
|------|------|------|
| `SCALE_OUT_NODE` | `storage-3` | 扩容加入节点 |
| `FILL_NODE` | `storage-4` | 缩容补位空闲 Active |
| `DRAIN_NODE` | 自动挑选 | 与 spare 重叠少、region 少优先 |
| `MAX_FAIL_RATIO` | `0.35` | 无感阈值 |
| `MIN_TRAFFIC_SEC` | `12` | 最短流量墙钟 |
| `SKV_TIMEOUT_SEC` | `2` | 单次 skv 超时 |
| `PUT_RETRIES` / `GET_RETRIES` | `4` / `5` | 客户端重试 |

仅缩容（已有 spare Active）：`bash scripts/it-scale-in-live.sh`

---

## 3. 人工 SOP（与脚本同构）

### 3.1 准备区间与种子数据

```bash
./build/tools/skv put aaa before-left
./build/tools/skv put mmm before-mid
./build/tools/skv put zzz before-right
./build/tools/skv split 0 m
./build/tools/skv route          # 期望 2 个 Region，副本仍在 0/1/2
```

### 3.2 业务侧持续读写（另开终端）

客户端必须可重试。示意：

```bash
# 伪代码 / 手工循环
while true; do
  k=live-$RANDOM
  ./build/tools/skv put "$k" "v-$(date +%s)" || sleep 0.1   # 失败重试
  ./build/tools/skv get "$k"                                 # 可多试几次
  sleep 0.05
done
```

生产 Gateway / SDK：对 `not_leader`、超时做指数退避重试；**不要**把一次失败当成数据丢失。

### 3.3 扩容（join → balance）

```bash
./build/tools/skv nodes storage          # storage-3 = joining
./build/tools/skv join storage-3         # → Active
./build/tools/skv balance 3              # 显式入队 RF 恒定 Replace
./build/tools/skv scale-in-status storage-3   # 看 regions 是否 >0
# 未迁入可重复 balance；卡住再用：
# ./build/tools/skv place <rid> storage-3,storage-1,storage-2
./build/tools/skv route                  # 至少一个 Region 含 storage-3
```

期望：

- `storage-3` state=Active，`regions≥1`
- RF 仍为 3；流量失败率短暂上升后回落

### 3.4 缩容（scale-in → Tombstone → kick）

扩容后若 spare 已在部分 Region 上，优先缩容**与 spare 重叠少**的 Store（IT 会自动挑选），避免 Replace 只能补「刚被踢掉、本地仍占 RocksDB LOCK」的旧节点。

```bash
./build/tools/skv scale-in storage-1     # 示例：与 spare 重叠更少的节点
./build/tools/skv scale-in-status storage-1
# 循环观察直到 state=tombstone / regions=0
# Draining 期间不要 stop / kill 该进程

./build/tools/skv route                  # 不应再出现被缩容节点
./build/tools/skv remove-tombstone storage-1
# 此后可停进程、删盘
```

卡住（regions 长时间不变）时：

```bash
# 对仍挂 drain 节点的 Region：place 到 remaining + 任意空闲 Active（凑满 RF=3）
./build/tools/skv place <rid> storage-2,storage-3,storage-4
```

### 3.5 验收

```bash
./build/tools/skv get aaa
./build/tools/skv get zzz
./build/tools/skv route
./build/tools/skv nodes storage
```

| 检查 | 通过标准 |
|------|----------|
| 扩容节点 | 在 route 中，且曾 `regions≥1` |
| 缩容节点 | 不在 route；Meta 已 kick 或 Tombstone 已清 |
| 数据 | 种子 key / 业务 key 可读 |
| 无感 | 业务重试后成功率达标；无长时间全失败 |

---

## 4. 状态机速查

```text
REGISTER → Joining --join--> Active --scale-in--> Draining --empty--> Tombstone --remove-tombstone--> 擦除
```

| 操作 | Meta / skv |
|------|------------|
| 加入 | `JOIN_NODE` / `skv join` |
| 打散 | `BALANCE` / `skv balance` 或 `skv place` |
| 下线 | `STORE_DELETE` / `skv scale-in` |
| 状态 | `STORE_STATUS` / `skv scale-in-status` |
| 踢出 | `REMOVE_TOMBSTONE` / `skv remove-tombstone` |

---

## 5. 故障与禁忌

| 现象 | 处理 |
|------|------|
| `join` heartbeat stale | `skv restart storage-N` 后再 join |
| `balance` 返回 0 | 过载差 &lt; 2 或 Active≤RF；先 split 多 Region，或 `skv place` |
| drain 卡住 | `place` 显式迁出；确认 spare 为 Active |
| 大量 `not_leader` | 正常短暂窗口；加大客户端重试，勿杀 Leader 所在 Store |
| Place / drain 中途 | **禁止**强杀被迁 / Draining 节点 |

不要：

- 用 `SCALE_OUT`（加 Learner 抬 RF）当默认扩容
- 指望 Split 降低单机磁盘占用
- 只 DESTROY 不先 REMOVE_PEER

---

## 6. 日志

```bash
./build/tools/skv logs storage-0 -n 80
./build/tools/skv logs meta-0 -n 40
# IT 流量日志
tail -f /tmp/skv_scale_live_traffic.log
```

---

## 7. 相关文档

- [flows/扩容缩容设计.md](../flows/扩容缩容设计.md)
- [SCALE_OUT_SOP.md](SCALE_OUT_SOP.md)
- [flows/10-region-placement.md](../flows/10-region-placement.md)
- [REGION_OPS.md](REGION_OPS.md)
