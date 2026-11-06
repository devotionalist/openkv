# Seastar 网络栈

依赖官方库：**[scylladb/seastar](https://github.com/scylladb/seastar)**。  
与 RocksDB 一样：**CMake `FetchContent` 直接拉源码编进工程**（`cmake/FetchSeastar.cmake`），链接 `Seastar::seastar`。  
不要求本机预先 `cmake --install` Seastar；只需装好上游的**系统依赖**。

**公开说明（推荐先读）：** [docs/NETWORKING.md](../NETWORKING.md)（gRPC 强制现状 + Seastar 可选未接入业务）。

## 目标

在 **gRPC 强制**前提下，可选将 reactor / 异步 I/O 换到 Seastar（调度模型），**不改变** Store/Raft/Admin/Meta 的 gRPC 协议面。  
历史行协议监听已删除，见 [flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md)。

## 现状摘要（2026-10）

| 项 | 状态 |
|----|------|
| FetchContent / CMake 开关 | **有**（默认 OFF） |
| Meta/Storage gRPC 跑在 Seastar reactor | **未做** |
| Raft 出站 reactor 调度 | **未做**（仍 `GrpcRaftTransport` + 线程） |
| `app_template` 生产入口 | **未做**（仅 `seastar_app.hpp` 占位） |

## 编译开关

| 开关 | 默认 | 说明 |
|------|------|------|
| `-DOPSKV_USE_SEASTAR=ON` | OFF | FetchContent 拉取并编译 seastar，定义 `OPSKV_HAS_SEASTAR=1` |
| `-DOPSKV_SEASTAR_GIT_TAG=…` | `seastar-25.05.0` | 固定上游 tag / commit |
| macOS 本地开发 | 建议 OFF | Seastar 官方以 Linux 为主 |
| Linux CI / 生产 | 可 ON | 先跑上游 `install-dependencies.sh` |

```bash
# Linux：只装系统包一次，再开开关（Seastar 源码由 CMake 拉取）
git clone --depth 1 https://github.com/scylladb/seastar.git /tmp/seastar-deps
sudo /tmp/seastar-deps/install-dependencies.sh

cmake -B build -DOPSKV_USE_SEASTAR=ON
cmake --build build -j
```

## 模块映射（更新后）

| 模块 | 现状 | Seastar 目标 |
|------|------|--------------|
| **Meta / Storage** | gRPC server（主端口） | reactor 上跑 gRPC completion queue / 自定义 poller（未做） |
| **Raft** | `GrpcRaftTransport` 出站 gRPC | P2：reactor 友好的出站调度 |
| **compute**（C++） | 占位 `seastar_app.hpp` | P3：`app_template` |
| **Go gateway** | 独立进程 gRPC | 暂不改 |

## 约束

1. **阻塞禁止进 reactor**：RocksDB / `fsync` / 长临界区走 `seastar::async` 或线程池（`alien`）。
2. **协议面不变**：客户端仍只说 gRPC；无行协议双栈。

## 落地阶段

1. **P0**：`FetchSeastar.cmake` 可开关引入（已有）。
2. **P2**：Raft / gRPC 出站与 Seastar 调度集成（待做）。
3. **P3**：`app_template` 入口；Linux CI 可选 `USE_SEASTAR=ON` smoke。

## 与「预装 Seastar」的区别

| 方式 | 说明 |
|------|------|
| FetchContent（本仓库默认） | 版本钉在 `OPSKV_SEASTAR_GIT_TAG`，与 RocksDB 一致 |
| 系统 `find_package(Seastar)` | 需自行 install；本仓库未作为默认路径 |
