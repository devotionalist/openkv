# skv — C++ 运维命令行

可执行文件：`build/tools/skv`

```bash
cmake --build build -j --target skv
./build/tools/skv help
```

## 命令

| 命令 | 说明 |
|------|------|
| `status` | 进程 pid + 端口探测 |
| `start [--fresh]` | 拉起 meta×3 / storage×3 / gateway（`--fresh` 清空 `data/`） |
| `stop` | 优雅停止全部 |
| `restart <name>` | 重启 `meta-N` / `storage-N` / `gateway` |
| `kill <name>` | 强杀（故障演练） |
| `logs <name> [-n N] [-f]` | 看 `.run/<name>.log` |
| `put` / `get` / `del` | 经 Storage gRPC ExecLine（默认）；先 `locate` Region，自动跟随 `not_leader redirect=` |
| `locate <key>` | Meta `LOCATE` → `region_id` |
| `split <region_id> <split_key>` | 分裂（仅切元数据；跨机迁副本用 `place` / `balance`） |
| `merge <left_id> <right_id>` | 各副本 `ADMIN_MERGE` + Meta `MERGE_REGIONS` |
| `place <rid> <n1,n2,...>` | 编排 ConfChange 后 Meta `PLACE_REGION`（踢出副本会 `DESTROY`；见 [REGION_OPS.md](REGION_OPS.md)、[SCALE_OUT_SOP.md](SCALE_OUT_SOP.md)） |
| `join <node>` | 待加入 Store：`JOIN_NODE` Joining→Active |
| `scale-in <node>` | store delete → Draining，调度迁光后 Tombstone |
| `scale-in-status <node>` | `STORE_STATUS`（state / regions / leaders） |
| `remove-tombstone <node>` | 擦除 Tombstone 元数据 |
| `balance [n]` | 入队 RF 不变的 balance Replace |
| `nodes [storage]` | Meta `LIST_NODES`（附带可读状态） |
| `ping` | Meta / Storage PING + Gateway 端口 |
| `route` | Meta `GET_ROUTE`（跟 Leader） |
| `who-leader` | Meta Leader |
| `smoke` | 写读一轮冒烟 |

## 环境变量

| 变量 | 默认 |
|------|------|
| `SKV_ROOT` | 向上探测含 `conf/bootstrap.conf` 的目录 |
| `SKV_STORAGES` | `127.0.0.1:9100`…`9105`（6 节点） |
| `SKV_META` | `127.0.0.1:8500,8501,8502` |
| `SKV_GATEWAY` | `127.0.0.1:19000`（status/ping 端口探测） |

Storage 自动分裂（进程环境，非 skv）：`SKV_SPLIT_KEYS` / `SKV_SPLIT_BYTES`（默认 `0` = 关），见 [RANGE_SHARDING.md](../architecture/RANGE_SHARDING.md)。

## 示例

```bash
./build/tools/skv status
./build/tools/skv start --fresh
./build/tools/skv put hello world
./build/tools/skv get hello
./build/tools/skv locate hello
./build/tools/skv split 0 m
./build/tools/skv merge 0 1
./build/tools/skv place 0 storage-3,storage-4,storage-5
./build/tools/skv nodes storage
./build/tools/skv route
./build/tools/skv restart storage-1
./build/tools/skv smoke
./build/tools/skv stop
```
