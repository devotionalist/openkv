# gRPC 强制（拆除行协议双栈）

**公开读者请看：** [docs/NETWORKING.md](../NETWORKING.md)（现状与组件对照）。本文保留变更动机与验收命令。

## 问题

历史开关允许绕过 gRPC：

- `-DOPSKV_USE_GRPC=OFF`：C++ 只听主端口行协议
- `SKV_LEGACY_TCP=1`：主端口 gRPC 时额外在 port+1000 听行协议；客户端改走行协议

本机 GRPC=OFF 时 Gateway/Web 默认 gRPC 会 `storage not ready` / `http2: frame too large`。

## 方案

- CMake：**强制**拉取/链接 gRPC（系统包优先，否则 FetchContent）；旧 cache `OFF` 自动抬升为 ON
- Meta / Storage：**只**在主端口起 gRPC；`ExecLineHandler` 作进程内 ExecLine 行协议解析（不 listen）
- Go Gateway / Web / `storagetcp` / `line-rpc` / `skv`：**只**走 gRPC ExecLine
- 删除环境变量 `SKV_LEGACY_TCP` 与相关自动导出逻辑

## 废代码清理（随强制 gRPC）

已删除 / 收窄：

| 原物 | 处置 |
|------|------|
| `TcpServer`（listen + 行协议） | 删 listen；类/文件改名为 `ExecLineHandler`（`exec_line_handler.*`） |
| `TcpTransport`（peer 表 / 出站计数 / 历史 TCP 发包） | 删除；并入 `GrpcRaftTransport`（Impl peer 表 + `PeerCounter`） |
| `net::tcp_line` / `tcp_line_auth` | 删除；仅保留 `parse_host_port` |
| `#else //!OPSKV_USE_GRPC` 桩（Meta/Storage/Raft gRPC） | 删除 |

## 边界

- 行协议字符串（`PING` / `GET_ROUTE` / `ADMIN_*`）仍作为 gRPC `ExecLine` 的 body 语义，不是独立 TCP 监听
- 构建机必须能拿到 gRPC（`brew install grpc` 或允许 FetchContent）

## 失败与回滚

- cmake 找不到系统 gRPC：走 FetchContent；**禁止递归子模块**（否则会卡在 opentelemetry/vcpkg）
- `start-cluster.sh` 会浅克隆 `build/_deps/grpc-src` 并传 `FETCHCONTENT_SOURCE_DIR_GRPC`
- GitHub 慢：`OPSKV_GRPC_GIT_REPOSITORY=https://mirror.ghproxy.com/https://github.com/grpc/grpc.git`
- 更省事：`brew install grpc protobuf abseil c-ares re2` 后系统 `find_package` 成功则不再 FetchContent
- 回滚需恢复本提交前的 `OPSKV_USE_GRPC` option 与 `SKV_LEGACY_TCP` 分支（不推荐）

## 验收

```bash
# 配置必须成功且 cache 为 ON
grep OPSKV_USE_GRPC build/CMakeCache.txt   # → BOOL=ON

# 无 TcpTransport 源文件
test ! -f raft/src/tcp_transport.cpp

# 集群起后 Storage/Meta 日志无 “legacy TCP listening”
SKV_FRESH=1 ./scripts/start-cluster.sh
./.run/kv-example 127.0.0.1:19000
curl -sS http://127.0.0.1:19100/api/route   # 需先 make admin-run
```

期望：`cluster started`；kv-example PASS；route JSON 含 regions；`opskv_raft` 链入 `grpc_transport.cpp` 而非 `tcp_transport.cpp`。
