# 网络栈：gRPC 与 Seastar

本文说明 OpsKV **实际在用的通信方式**，以及可选的 Seastar 编译开关。  
文档总目录：[README.md](./README.md) · gRPC 强制变更：[flows/GRPC_MANDATORY.md](./flows/GRPC_MANDATORY.md) · 业务错误 token：[architecture/ERROR_CODES.md](./architecture/ERROR_CODES.md)。

---

## 1. 总览

| | **gRPC** | **Seastar** |
|--|----------|-------------|
| 状态 | **强制**；生产与本地默认路径 | **可选**；默认 `OFF`，业务路径未接入 |
| 作用 | 集群内外全部 RPC 协议面 | 计划中的异步 reactor 调度（不改变协议） |
| CMake | `OPSKV_USE_GRPC` 固定 `ON` | `-DOPSKV_USE_SEASTAR=ON` 才拉取并链接 |

```
Client / skv / Web
        │  gRPC  :19000
        ▼
Go compute-gateway          KVService / DSService
        │  gRPC
   ┌────┴────┐
   ▼         ▼
Meta :850x   Storage :910x
MetaService  StoreService + RaftService + AdminService
             （Raft 节点间亦走同一端口的 RaftService）
```

**没有**对外裸 TCP 行协议监听；历史行命令（如 `ADMIN_*`）仅作为 `AdminService.ExecLine` / Meta 等价 RPC 的 body 语义保留。

---

## 2. gRPC：在哪里、怎么用

### 2.1 协议定义与代码生成

| 路径 | 内容 |
|------|------|
| `proto/kv/v1/` | 对外 KV / DS / Watch（Gateway 实现） |
| `proto/meta/v1/` | `MetaService`：路由、TSO、节点生命周期、调度 |
| `proto/storage/v1/` | `StoreService` / `RaftService` / `AdminService` |
| C++ 生成 | 构建时写入 `build/generated/`（`opskv_proto`） |
| Go 生成 | `client/go/gen/`（`make proto` / `scripts/gen_proto.sh`） |

### 2.2 按组件

| 组件 | 代码位置 | 监听 / 调用 |
|------|----------|-------------|
| **Gateway** | `client/go/cmd/compute-gateway` | 监听 `:19000`；上游调 Meta / Storage |
| **Go SDK** | `client/go/kv`、`client/go/ds` | 客户端 → Gateway |
| **Meta** | `meta/src/grpc_server.cpp` | 主端口（如 `:8500`）`MetaService` |
| **Storage** | `storage/src/grpc_server.cpp` | 主端口（如 `:9100`）三服务同端口 |
| **Raft 出站** | `raft/src/grpc_transport.cpp` | Storage↔Storage：`AppendEntries` / Vote / Snapshot |
| **运维** | `tools/skv`、`web/`、`client/go/cmd/line-rpc` | 经 gRPC（含 `ExecLine`）访问 Meta/Storage |

### 2.3 Storage 三个服务（同端口）

| 服务 | 典型调用方 | 作用 |
|------|------------|------|
| `StoreService` | Gateway | `Get` / `Propose` / `TransferLeadership` |
| `RaftService` | 其他 Storage | 复制与选举、安装快照 |
| `AdminService` | Meta / skv / Web | 分裂合并、place、转主、进度；`ExecLine` 承接未单独建模的运维行 |

### 2.4 构建

```bash
# gRPC 为硬依赖：系统包优先，否则 FetchContent 拉 grpc（首次较慢）
cmake -B build   # OPSKV_USE_GRPC 强制 ON，无需也不应再 OFF
cmake --build build -j
```

鉴权：运维类 RPC 可带 `x-skv-admin-token`（与 `SKV_ADMIN_TOKEN` 对齐），见代码 `common/include/opskv/grpc_auth.hpp`。

---

## 3. Seastar：在哪里、怎么用

### 3.1 现状（务必读）

- 默认 **不编译、不运行** Seastar。
- 打开 `-DOPSKV_USE_SEASTAR=ON` 后：FetchContent 拉 [scylladb/seastar](https://github.com/scylladb/seastar)，相关 target 链接 `Seastar::seastar`，定义 `OPSKV_HAS_SEASTAR=1`。
- **Meta / Storage / Raft 运行时仍是 gRPC + 普通线程**；reactor 上跑 gRPC completion queue **尚未实现**。
- `compute/include/opskv/compute/seastar_app.hpp` 仅为占位说明与条件 `#include`，无生产入口。

### 3.2 相关文件

| 路径 | 作用 |
|------|------|
| `cmake/FetchSeastar.cmake` | 拉取并编译 Seastar |
| 根 `CMakeLists.txt` | `option(OPSKV_USE_SEASTAR … OFF)` |
| `common` / `meta` / `storage` / `compute` 的 CMake | ON 时 PUBLIC 链接 |
| `compute/.../seastar_app.hpp` | 未来 `app_template` 示意 |

### 3.3 何时打开

```bash
# 建议仅 Linux；先装上游系统依赖
sudo ./path/to/seastar/install-dependencies.sh

cmake -B build -DOPSKV_USE_SEASTAR=ON
# 可选：-DOPSKV_SEASTAR_GIT_TAG=seastar-25.05.0
cmake --build build -j
```

macOS 上 ON 可能配置/编译失败，本地开发请保持 **OFF**。

协议面不因 Seastar 改变：客户端与节点间仍只说 **gRPC**。

---

## 4. 与「行协议」的关系

| 误解 | 事实 |
|------|------|
| Meta/Storage 主端口仍是 TCP 文本行 | **否**；主端口是 gRPC |
| 可以关掉 gRPC C++ | **否**；已强制 ON |
| 必须编 Seastar 才能跑 | **否**；默认 OFF 即可 `make` / `make e2e` |
| `ADMIN_*` 字符串消失了 | **否**；多经 `ExecLine` 进入同一套处理 |

---

## 5. 相关文档

| 文档 | 说明 |
|------|------|
| [USER_GUIDE.md](./USER_GUIDE.md) | 部署与使用 |
| [README.md](./README.md) | 文档分类索引 |
| 仓库 [../README.md](../README.md) | 快速开始 |
| [architecture/SEASTAR_NET.md](./architecture/SEASTAR_NET.md) | Seastar 阶段规划 |
| [flows/GRPC_MANDATORY.md](./flows/GRPC_MANDATORY.md) | gRPC 强制变更说明 |
| [architecture/ARCHITECTURE.md](./architecture/ARCHITECTURE.md) | 架构总览 |

---

## 6. 验收（文档与开关）

```bash
grep OPSKV_USE_GRPC build/CMakeCache.txt    # → BOOL=ON
grep OPSKV_USE_SEASTAR build/CMakeCache.txt # → 默认 OFF
# 进程：Gateway/Meta/Storage 主端口为 gRPC（可用 grpcurl 或 skv / 客户端冒烟）
make e2e   # 或 make cluster-up + kv-example
```
