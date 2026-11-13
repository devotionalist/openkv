# 流程：Go 客户端（独立仓库）

**状态：** 仅维护 Go SDK  
**读者：** SDK 使用者 / 维护者  
**相关：** [01-write-put.md](01-write-put.md) · Gateway 为本仓 C++ `gateway_node`（[18-cpp-gateway.md](18-cpp-gateway.md)）· [27-raw-vs-txn.md](27-raw-vs-txn.md)

## 布局

对外客户端**不在** openkv 主仓内发版；官方 SDK 仅保留 Go：

| 库 | GitHub | 模块 / 包 |
|----|--------|-----------|
| **Go** | https://github.com/devotionalist/openkv-client-go | `github.com/devotionalist/openkv-client-go`（`kv` + `ds`） |

Rust / Java / TypeScript 客户端仓已废弃删除；应用请只用 Go SDK 或自研 gRPC stub。

本仓 `client/go` **仅**保留 examples / e2e / `cmd/*` 与 Meta·Storage 生成码（`replace` → 同级 `openkv-client-go`）。见 [client/go/README.md](../../client/go/README.md)。

## IDL 同步

权威 proto：本仓 `proto/kv/v1/{kv,ds}.proto`。

```bash
cd ../openkv-client-go
./scripts/sync-proto-from-openkv.sh   # 默认从 ../openkv 拷贝 kv+ds
./scripts/gen.sh
```

环境变量 `OPENKV_ROOT` 可指向本仓根目录。主仓 `scripts/gen_proto.sh` 只生成 Meta/Storage（给 web / line-rpc）。

## API 平面

| plane | 方法 |
|-------|------|
| **txn** | `Get` `Put`/`PutTTL` `PutCAS` `Delete`/`DeleteCAS` `BatchGet`/`BatchPut`（扇出） `Scan`/`ListPrefix` `TTL`/`Expire`/`Persist` `Watch` `Begin` |
| **raw** | **仅** `RawGet`/`RawPut`/`RawDelete`（及 CAS/TTL 变体） |
| **ds** | 包 `ds`：Hash / Set / List / ZSet |

Raw* 约定见 [27-raw-vs-txn.md](27-raw-vs-txn.md)。

## 验收

```bash
cd ../openkv-client-go && go test ./...
(cd client/go && go build ./examples/... ./cmd/...)
```
