# 网络栈：gRPC

本文说明 OpenKV **实际在用的通信方式**：全链路强制 gRPC，主端口多路复用。  
文档总目录（默认英文）：[English](./en/README.md) · [中文](./zh/README.md) · 本文英文版：[networking](./en/networking.md) · gRPC 强制变更：[flows/GRPC_MANDATORY.md](./flows/GRPC_MANDATORY.md) · 业务错误 token：[architecture/ERROR_CODES.md](./architecture/ERROR_CODES.md) · 线程模型：[flows/22-async-threading-model.md](./flows/22-async-threading-model.md)。

---

## 1. 总览

| 项 | 说明 |
|----|------|
| 协议 | **gRPC**（强制）；生产与本地默认路径 |
| CMake | `OPENKV_USE_GRPC` 固定 `ON`（无需也不应再 OFF） |
| 线程 | Callback/CQ 接收线程 + Gateway/Meta `WorkPool` + BatchSystem / Transport worker；Storage 无 WorkPool（见 [22](./flows/22-async-threading-model.md)） |
| 关机 | SIGTERM：拒新 RPC + 排空在途（见 [31-graceful-shutdown](./flows/31-graceful-shutdown.md)；`SKV_GRPC_SHUTDOWN_MS`） |

```
Client / skv / Web
        │  gRPC  :19000
        ▼
C++ gateway_node            KVService / DSService
        │  gRPC
   ┌────┴────┐
   ▼         ▼
Meta :850x   Storage :910x
MetaService  StoreService + RaftService + AdminService
             （Raft 节点间亦走同一端口的 RaftService）
```

**没有**对外裸 TCP 行协议监听；历史行命令语义仅保留在类型化 Admin / Meta RPC（及运维工具封装）中。

### 默认端口

| 角色 | 地址 |
|------|------|
| Gateway | `127.0.0.1:19000` |
| Meta ×3 | `127.0.0.1:8500`–`8502` |
| Storage ×N | `127.0.0.1:9100`–… |
| Web 管控台 | `http://127.0.0.1:19100`（HTTP → 后端 gRPC） |

---

## 2. gRPC：在哪里、怎么用

### 2.1 协议定义与代码生成

| 路径 | 内容 |
|------|------|
| `proto/kv/v1/` | 对外 KV / DS / Watch（Gateway 实现） |
| `proto/meta/v1/` | `MetaService`：路由、TSO、节点生命周期、调度 |
| `proto/storage/v1/` | `StoreService` / `RaftService` / `AdminService` |
| C++ 生成 | 构建时写入 `build/generated/`（`openkv_proto`） |
| Go 生成 | `client/go/gen/`（`make proto` / `scripts/gen_proto.sh`） |

### 2.2 按组件

| 组件 | 代码位置 | 监听 / 调用 |
|------|----------|-------------|
| **Gateway** | `compute/` → `gateway_node` | 监听 `:19000`；上游调 Meta / Storage |
| **Go SDK** | 独立仓 `openkv-client-go`（`kv`/`ds`） | 客户端 → Gateway |
| **Meta** | `meta/src/grpc_server.cpp` | 主端口（如 `:8500`）`MetaService` |
| **Storage** | `storage/src/grpc_server.cpp` | 主端口（如 `:9100`）三服务同端口 |
| **Raft 出站** | `raft/src/grpc_transport.cpp` | Storage↔Storage：`AppendEntries` / Vote / Snapshot |
| **运维** | `tools/skv`、`web/`、`client/go/cmd/line-rpc` | 经 gRPC 访问 Meta/Storage |

### 2.3 Storage 三个服务（同端口）

| 服务 | 典型调用方 | 作用 |
|------|------------|------|
| `StoreService` | Gateway | `Get` / `Propose` / `TransferLeadership` |
| `RaftService` | 其他 Storage | 复制与选举、安装快照 |
| `AdminService` | Meta / skv / Web | 分裂合并、place、转主、进度 |

### 2.4 构建

```bash
# gRPC 为硬依赖：系统包优先，否则 FetchContent 拉 grpc（首次较慢）
cmake -B build   # OPENKV_USE_GRPC 强制 ON
cmake --build build -j
```

鉴权：运维类 RPC 可带 `x-skv-admin-token`（与 `SKV_ADMIN_TOKEN` 对齐），见代码 `common/include/openkv/grpc_auth.hpp`。

---

## 3. 与「行协议」的关系

| 误解 | 事实 |
|------|------|
| Meta/Storage 主端口仍是 TCP 文本行 | **否**；主端口是 gRPC |
| 可以关掉 gRPC C++ | **否**；已强制 ON |

---

## 4. 相关文档

| 文档 | 说明 |
|------|------|
| [USER_GUIDE.md](./USER_GUIDE.md) | 部署与使用 |
| [README.md](./README.md) | 文档分类索引 |
| 仓库 [../README.md](../README.md) | 快速开始 |
| [flows/GRPC_MANDATORY.md](./flows/GRPC_MANDATORY.md) | gRPC 强制变更说明 |
| [flows/22-async-threading-model.md](./flows/22-async-threading-model.md) | 接收线程 / CQ / WorkPool |
| [architecture/ARCHITECTURE.md](./architecture/ARCHITECTURE.md) | 架构总览 |

---

## 5. 验收

```bash
grep OPENKV_USE_GRPC build/CMakeCache.txt    # → BOOL=ON
# 进程：Gateway/Meta/Storage 主端口为 gRPC（可用 grpcurl 或 skv / 客户端冒烟）
make e2e   # 或 make cluster-up + kv-example
```
