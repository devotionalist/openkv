# 流程：多语言客户端（独立仓库）

**状态：** 已拆分为四个独立库  
**读者：** SDK 使用者 / 维护者  
**相关：** [01-write-put.md](01-write-put.md) · Gateway 仍在本仓 `client/go/cmd/compute-gateway`

## 布局

客户端**不在** openkv 主仓内发版；与主仓同级、各自 Git 仓：

| 库 | 本地路径（同级） | 模块 / 包 |
|----|------------------|-----------|
| [openkv-client-go](https://github.com/devotionalist/openkv-client-go) | `../openkv-client-go` | `github.com/devotionalist/openkv-client-go` |
| [openkv-client-rust](https://github.com/devotionalist/openkv-client-rust) | `../openkv-client-rust` | crate `openkv-client-rust` |
| [openkv-client-java](https://github.com/devotionalist/openkv-client-java) | `../openkv-client-java` | `io.openkv:openkv-client-java` |
| [openkv-client-typescript](https://github.com/devotionalist/openkv-client-typescript) | `../openkv-client-typescript` | npm `openkv-client-typescript` |

本仓 `client/go` **仅**保留 compute-gateway、内部工具与历史 SDK 路径；新业务请依赖上述独立库。

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
