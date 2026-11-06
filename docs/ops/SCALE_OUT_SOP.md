# 扩容 / 缩容 SOP（本地 / 运维）

**Split 只切元数据（同机不搬 KV）**；**真正腾盘 / 打散靠 Place（跨机迁副本）**；**Store 缩容 = Draining → 迁光 → Tombstone**。  
设计总览：[DATA_SCALE.md](../architecture/DATA_SCALE.md) · [DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md) · [扩容缩容设计.md](../flows/扩容缩容设计.md)  
**业务持续读写下的完整扩缩容（无感）：** [SCALE_LIVE_SOP.md](SCALE_LIVE_SOP.md) · `scripts/it-scale-live.sh`  
详解：[REGION_OPS.md](REGION_OPS.md) · [flows/10-region-placement.md](../flows/10-region-placement.md) · [flows/03-scale-out.md](../flows/03-scale-out.md)

## 0. 前置

```bash
# 状态
./build/tools/skv status
./build/tools/skv nodes storage
./build/tools/skv route
./build/tools/skv who-leader
```

约定（默认本地）：

| 项 | 值 |
|----|-----|
| Meta | `8500–8502` |
| Storage | `9100–9105`（进程 6；bootstrap 仅 `0–2` Active，`3–5` Joining 待加入） |
| Gateway | `19000` |
| 初始 Region 0 | `storage-0,1,2`（RF=3） |

默认启动：`storage-0/1/2` = **在用 Active**；`storage-3/4/5` = **Joining 待加入**（须 `skv join` / Web「加入」后才能 Balance / Place）。

---

## 1. 干净环境（演练前）

```bash
./build/tools/skv stop
./build/tools/skv start --fresh   # 清空 data/ 后拉起
# 或：make cluster-down && rm -rf data && make cluster-up

./build/tools/skv status
./build/tools/skv smoke
./build/tools/skv route
```

期望：`up=10`，`OK ROUTE` 含 region `0`，副本约 3 个。

---

## 2. SOP A — 加节点后迁 Region（容量扩容）

**场景：** 集群已有空闲 Store（如 `storage-3,4,5`），要把 Region 0 从 `0,1,2` 迁到 `3,4,5` 以腾盘 / 打散。

```bash
# A1 确认目标节点在线
./build/tools/skv nodes storage

# A2 写入一点数据（可选，便于验证）
./build/tools/skv put hello world
./build/tools/skv put mmm mid
./build/tools/skv put zzz end

# A3 Place：迁副本（ENSURE→Learner→追赶→Promote→踢旧+DESTROY→Meta PLACE）
./build/tools/skv place 0 storage-3,storage-4,storage-5

# A4 校验
./build/tools/skv route
./build/tools/skv get hello
./build/tools/skv status
```

期望：

- `route` 中 region 0 副本为 `storage-3,4,5`
- `get` 仍可读
- 过程中旧节点可能短暂 `not_leader`；Gateway/`skv` 会跟 Leader
- **不应**再出现 DESTROY 后进程 abort（已修 UAF）

回滚（迁回）：

```bash
./build/tools/skv place 0 storage-0,storage-1,storage-2
```

---

## 3. SOP B — Split 后再 Place（热点 / 区间打散）

**场景：** Region 过大或热点；同机先逻辑切开，再**单独**把右片迁到另一组 Store。  
Split **不**自动 scatter（无感扩容走 SOP C：join + balance）。

```bash
# B1 准备 key 落在两侧（示例 split 点 m）
./build/tools/skv put aaa 1
./build/tools/skv put mmm 2
./build/tools/skv put zzz 3

# B2 仅分裂（meta-only，副本集不变）
./build/tools/skv split 0 m

# B3 需要打散时再 place（建议避开左片）
./build/tools/skv route
RIGHT=$(./build/tools/skv route | awk '/start=m/{print $2; exit}')  # 示例：取右 Region id
./build/tools/skv place "$RIGHT" storage-3,storage-4,storage-5

# B4 校验路由与读写
./build/tools/skv locate aaa    # → 左 region
./build/tools/skv locate zzz    # → 右 region
./build/tools/skv get aaa
./build/tools/skv get zzz
```

期望：

- Meta 出现两个 Region；左 `end=m`，右 `start=m`
- Split **瞬间完成**（无整段 `migrate_ge`）；写暂停窗口极短
- Place 后右片副本在目标 Store（RF 不变）

---

## 4. SOP C — 默认扩容（RF 不变 Balance）

**场景：** 新 Store 已 `REGISTER`（Joining）并已 `join` 为 Active；希望从过载节点迁入部分 Region（**不抬 RF**）。

```bash
./build/tools/skv nodes storage          # 新节点应为 joining
./build/tools/skv join storage-3         # Joining → Active
./build/tools/skv balance                # 显式入队 1 个 Replace
# 或等待 RegionScheduler 后台 suggest_balance_move
./build/tools/skv route
```

---

## 5. SOP D — 缩容（scale-in ≈ store delete）

设计：[DATA_SCALE_IN.md](../architecture/DATA_SCALE_IN.md)。

```bash
./build/tools/skv scale-in storage-0
./build/tools/skv scale-in-status storage-0   # regions/leaders → 0 后变 tombstone
./build/tools/skv nodes storage
# Draining 期间保持进程存活，直到 Tombstone
./build/tools/skv remove-tombstone storage-0  # 清 Meta 元数据后再停机删盘
```

---

## 6. SOP E — Meta SCALE_OUT（高级：加副本，非默认）

**场景：** 显式给 Region **增加**副本（会抬副本数，会抬副本数，非默认 RF 不变扩容）。

```bash
# SCALE_OUT <node_id> [rid,rid…]   # rid 空 = 全部 Region
# GET_SCALE <task_id>
# LIST_SCALE
```

流程见 [flows/03-scale-out.md](../flows/03-scale-out.md)。默认扩容请用 SOP C。

---

## 7. 检查清单

| 检查 | 命令 / 现象 |
|------|-------------|
| 进程全活 | `skv status` → down=0 |
| 路由版本上涨 | `skv route` 中 `ver` 变大 |
| 数据可读 | `skv get` / `smoke` |
| 无 abort | `rg terminating .run/storage-*.log` 无新增 |
| Place 失败 | 看 `not_leader` / `promote_failed`；重试同命令（大致幂等） |

日志：

```bash
./build/tools/skv logs storage-0 -n 80
./build/tools/skv logs meta-0 -n 40
```

---

## 8. 不要做的事

- 不要指望 **Split** 降低单机磁盘占用（同机共享引擎）
- 不要在 Place / scale-in Draining 期间强杀被迁节点
- 不要手动只 DESTROY 而不先 REMOVE_PEER（易脑裂）
- 不要用 `SCALE_OUT` 当默认扩容（会抬 RF）；默认走 Balance / `skv balance`

---

## 9. 一键演练脚本思路

```bash
./build/tools/skv stop
./build/tools/skv start --fresh
./build/tools/skv smoke
./build/tools/skv place 0 storage-3,storage-4,storage-5
./build/tools/skv route
./build/tools/skv get hello   # smoke 写入的 key，若无则先 put
```
