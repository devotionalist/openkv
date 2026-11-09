# 流程：多语言客户端（独立仓库）

**状态：** 已拆分为四个独立库  
**读者：** SDK 使用者 / 维护者  
**相关：** [01-write-put.md](01-write-put.md) · Gateway 仍在本仓 `client/go/cmd/compute-gateway`

## 布局

客户端**不在** openkv 主仓内发版；各自独立 GitHub 仓库：

| 库 | GitHub | 模块 / 包 |
|----|--------|-----------|
| **Go** | https://github.com/devotionalist/openkv-client-go | `github.com/devotionalist/openkv-client-go` |
| **Rust** | https://github.com/devotionalist/openkv-client-rust | crate `openkv-client-rust` |
| **Java** | https://github.com/devotionalist/openkv-client-java | `io.openkv:openkv-client-java` |
| **TypeScript** | https://github.com/devotionalist/openkv-client-typescript | npm `openkv-client-typescript` |

本仓 `client/go` **仅**保留 Compute Gateway、内部工具与历史内嵌路径；应用请依赖上表独立库。根 [README](../../README.md) 顶部有 Clients 入口。

## IDL 同步

权威 proto：本仓 `proto/kv/v1/kv.proto`。各客户端仓提供：

```bash
./scripts/sync-proto-from-openkv.sh   # 默认从 ../openkv 拷贝
```

环境变量 `OPENKV_ROOT` 可指向本仓根目录。

## 一期 API

`Dial/connect` · `Get` · `Put`/`PutTTL` · `PutCAS` · `Delete`/`DeleteCAS`

## 验收

```bash
cd ../openkv-client-go && go test ./...
cd ../openkv-client-rust && cargo test
cd ../openkv-client-java && mvn -q test          # JDK 11+ / Maven ≥ 3.6.3
cd ../openkv-client-typescript && npm i && npm test
```
