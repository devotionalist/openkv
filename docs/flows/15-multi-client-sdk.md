# 流程：多语言客户端（独立仓库）

**状态：** 已拆分为四个独立库  
**读者：** SDK 使用者 / 维护者  
**相关：** [01-write-put.md](01-write-put.md) · Gateway 为本仓 C++ `gateway_node`（[18-cpp-gateway.md](18-cpp-gateway.md)）

## 布局

客户端**不在** openkv 主仓内发版；各自独立 GitHub 仓库：

| 库 | GitHub | 模块 / 包 |
|----|--------|-----------|
| **Go** | https://github.com/devotionalist/openkv-client-go | `github.com/devotionalist/openkv-client-go`（`kv` + `ds`） |
| **Rust** | https://github.com/devotionalist/openkv-client-rust | crate `openkv-client-rust` |
| **Java** | https://github.com/devotionalist/openkv-client-java | `io.openkv:openkv-client-java` |
| **TypeScript** | https://github.com/devotionalist/openkv-client-typescript | npm `openkv-client-typescript` |

本仓 `client/go` **仅**保留 examples / e2e / `cmd/*` 与 Meta·Storage 生成码（依赖独立 Go SDK）；应用请依赖上表。见 [client/go/README.md](../../client/go/README.md)。

## IDL 同步

权威 proto：本仓 `proto/kv/v1/{kv,ds}.proto`。Go 客户端仓：

```bash
cd ../openkv-client-go
./scripts/sync-proto-from-openkv.sh   # 默认从 ../openkv 拷贝 kv+ds
./scripts/gen.sh
```

环境变量 `OPENKV_ROOT` 可指向本仓根目录。主仓 `scripts/gen_proto.sh` 只生成 Meta/Storage（给 web / line-rpc）。

## 一期 API

`Dial/connect` · `Get` · `Put`/`PutTTL` · `PutCAS` · `Delete`/`DeleteCAS` · `Scan`/`ListPrefix` · `TTL`/`Expire`/`Persist` · `Watch` · Txn · **DS**（Hash/Set/List/ZSet）

## 验收

```bash
cd ../openkv-client-go && go test ./...
cd ../openkv-client-rust && cargo test
cd ../openkv-client-java && mvn -q test          # JDK 11+ / Maven ≥ 3.6.3
cd ../openkv-client-typescript && npm i && npm test
# 本仓 examples 仍可构建（replace → 同级 SDK）：
(cd client/go && go build ./examples/... ./cmd/...)
```
