# OpsKV

基于 **Multi-Raft + RocksDB + gRPC** 的存算分离分布式 KV（C++ Meta/Storage + Go Gateway/客户端）。分片为 **Range Region**，默认副本因子 3。

**License:** [Apache License 2.0](LICENSE) · Copyright 2026 devotionalist（见 [NOTICE](NOTICE)）

## 架构一览

```
Go Client / skv
      │ gRPC :19000
      ▼
Compute Gateway (Go, 无状态)  ←── MetaService gRPC ──→  Meta Raft ×3 (:8500–8502)
      │ Store / Admin gRPC
      ▼
Storage ×N (:9100…)  · 每 Region 一组 Raft (RF=3) · RocksDB
      │ RaftService gRPC（节点间）
      └──────────► 其他 Storage
```

| 组件 | 职责 |
|------|------|
| **compute-gateway** | 无状态：Range 路由、Leader 缓存、重试 |
| **storage** | 有状态：Multi-Raft + RocksDB；Admin / Raft 同端口 gRPC |
| **meta** | Region 表、拓扑、分裂/合并/扩容编排；**Raft 3 副本 HA** |
| **client/go** | gRPC SDK |
| **tools/skv** | C++ 运维 CLI（启停、put/get、locate/split/merge） |
| **web/** | 独立 Web 管控模块（`:19100`）：拓扑 / Region / 运维 / KV |

![OpsKV 管控台总览](docs/images/console-overview.png)

**文档：** [docs/README.md](docs/README.md)（用户手册 · 网络 · 架构 · 运维 · 测试 · 流程图）

## 快速开始（本地）

默认路径：**强制 gRPC C++**（系统包或 FetchContent）；**不需要** Seastar（默认 `OPSKV_USE_SEASTAR=OFF`）。Storage 捆绑 RocksDB；进程分配器默认 **jemalloc**（FetchContent）。

```bash
make          # configure + build + unit tests
make e2e      # 编译 → 启集群 → Go 客户端 → 停集群
```

或用 skv：

```bash
cmake --build build -j --target skv
./build/tools/skv start --fresh
./build/tools/skv put hello world
./build/tools/skv get hello
./build/tools/skv locate hello
./build/tools/skv route
./build/tools/skv stop
```

Make 封装：

```bash
make cluster-up      # gateway 默认 :19000
make cluster-test
make admin-run       # http://127.0.0.1:19100 可视化运维
make cluster-down
# 或：SKV_ADMIN=1 make cluster-up   # 集群启动时一并拉起管控台
```

客户端：

```bash
./.run/kv-example 127.0.0.1:19000
cd client/go && go run ./examples/e2e -addr 127.0.0.1:19000
```

### 依赖说明

| 依赖 | 默认 | 说明 |
|------|------|------|
| **gRPC / Protobuf** | **强制 ON** | KV / Meta / Admin / 节点间 Raft；优先系统包，否则 FetchContent |
| **RocksDB / jemalloc** | 捆绑 | FetchContent |
| **Seastar** | **OFF** | 可选 reactor；**当前业务路径未使用**；建议仅 Linux 试验性打开 |

```bash
# 可选：仅 Linux 试验 Seastar（不改变 gRPC 协议面）
sudo ./path/to/seastar/install-dependencies.sh
cmake -B build -DOPSKV_USE_SEASTAR=ON
# 可选钉版本：-DOPSKV_SEASTAR_GIT_TAG=seastar-25.05.0
cmake --build build -j
```

细节：[docs/NETWORKING.md](docs/NETWORKING.md)。

## Kubernetes

见用户手册 [§3 Kubernetes / Helm](docs/USER_GUIDE.md#3-kubernetes--helm-部署) 与 [§4 扩容](docs/USER_GUIDE.md#4-扩容加节点--加入集群)。

## Go 客户端

```bash
make proto
cd client/go && go mod tidy
go run ./examples/basic 127.0.0.1:19000
```

```go
cli, _ := kv.Dial(ctx, "127.0.0.1:19000")
cli.Put(ctx, []byte("k"), []byte("v"))
val, found, rev, _ := cli.Get(ctx, []byte("k"))
```

## 目录

```
proto/      gRPC 定义
common/    配置、Region、日志
raft/      Multi-Raft + GrpcRaftTransport
storage/   RocksDB + Storage Node（gRPC）
meta/      集群状态 + Scaler（gRPC）
client/go/ Go SDK + gateway
tools/skv/ 运维 CLI
web/       管控台
docs/      全部文档（架构/运维/测试/流程图）
deploy/    Docker / Helm
conf/      本地配置
```

## 许可证

本项目采用 [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0) 开源。完整文本见仓库根目录 [LICENSE](LICENSE)；版权归属见 [NOTICE](NOTICE)。

自有源码（C++/Go/TS/proto、脚本与 CMake 等）文件头含 Apache 附录推荐的版权与许可声明（`Copyright 2026 devotionalist`）。构建产物（如 `web/static/`、`*/gen/`）与第三方依赖不另加本声明，仍受其各自许可证约束。

版权仍属于版权所有者；Apache-2.0 仅授予使用、修改与再分发等许可，不转让所有权。
