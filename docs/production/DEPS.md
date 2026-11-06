# 第三方依赖（FetchContent）

| 库 | CMake | 用途 |
|----|-------|------|
| [gRPC](https://github.com/grpc/grpc) + Protobuf | `FetchGrpc.cmake` | **强制**：Meta/Storage/Raft/Gateway 协议面 |
| [RocksDB](https://github.com/facebook/rocksdb) | `FetchRocksDB.cmake` | Storage 引擎 |
| [jemalloc](https://github.com/jemalloc/jemalloc) | `FetchJemalloc.cmake` | 进程分配器（默认 ON） |
| [spdlog](https://github.com/gabime/spdlog) | `FetchSpdlog.cmake` | 日志（`SKV_*` 宏） |
| [CLI11](https://github.com/CLIUtils/CLI11) | `FetchCLI11.cmake` | `skv` 子命令解析 |
| [toml11](https://github.com/ToruNiina/toml11) | `FetchToml11.cmake` | `.toml` 配置（`.conf` 仍可用） |
| [cpp-httplib](https://github.com/yhirose/cpp-httplib) | `FetchHttplib.cmake` | `SKV_METRICS_ADDR` → `GET /metrics` |
| [scylladb/seastar](https://github.com/scylladb/seastar) | `FetchSeastar.cmake` | 可选；**默认 OFF，业务未接入** |

协议与开关说明：[docs/NETWORKING.md](../NETWORKING.md)。

公共编解码：`common/include/opskv/codec.hpp`（hex，无额外依赖）。

```bash
# 示例：Storage 暴露 Prometheus HTTP
SKV_METRICS_ADDR=0.0.0.0:19101 ./build/storage/storage_node conf/storage.conf
curl -s localhost:19101/metrics | head
```
