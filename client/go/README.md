# client/go — 本仓 Go 工具（非对外 SDK）

对外 Go 客户端请用独立仓：**[openkv-client-go](https://github.com/devotionalist/openkv-client-go)**。

本目录仅保留：

| 路径 | 用途 |
|------|------|
| `examples/` | 集群 e2e / 冒烟 / bench（依赖 `openkv-client-go`） |
| `cmd/ds-smoke` `cmd/line-rpc` | 内部工具 |
| `gen/meta` `gen/storage` | Web / 运维用的 Meta·Storage gRPC 生成码 |

本地构建默认 `replace` 到同级 `../openkv-client-go`（见 `go.mod`）。
