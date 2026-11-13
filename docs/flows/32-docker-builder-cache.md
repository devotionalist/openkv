# Docker 基础镜像与增量编译缓存

**状态：** 已落地  
**相关：** [K8S_HELM](K8S_HELM.md) · [USER_GUIDE](../USER_GUIDE.md)

## 问题

业务镜像原先 `FROM ubuntu:24.04` 后当场 `apt-get` + FetchContent 编 RocksDB/jemalloc。  
一旦 `docker buildx prune` 或 `COPY . .` 打穿层缓存，每次部署都变成 **15–25 分钟全量编译**，看起来「越来越慢」。

## 方案

| 层 | 镜像 / 机制 | 何时重建 |
|----|-------------|----------|
| 基础 | `openkv-builder:dev`（`deploy/Dockerfile.builder`） | apt / 工具链变更；很少 |
| 业务 | `openkv:dev` / `openkv-gateway:dev` | 每次改 C++ |
| 增量 | BuildKit cache：`/src/build` + `/root/.ccache`（同 id 跨 Dockerfile 共用） | 自动；勿随意 `buildx prune -af` |

```bash
./scripts/docker-build-dev.sh builder   # 首次或工具链升级
./scripts/docker-build-dev.sh openkv    # 只编 meta/storage
./scripts/docker-build-dev.sh gateway   # 只编 gateway
./scripts/docker-build-dev.sh           # 缺 builder 则先编，再两者都编
```

`openkv` 与 `gateway` 共用 cache id `openkv-cmake-build`，先编其一后另一侧大多只链自己的 target。

## 边界

- 运行时仍是瘦 `ubuntu:24.04` + 二进制，不含编译器。  
- `docker builder prune` / `buildx prune` 会丢掉 RocksDB 对象，下次回退到接近全量。  
- macOS 本机 Mach-O 不能进 Linux 容器；继续用 Dockerfile 在 Linux（Colima）里编。

## 验收

| ID | 标准 |
|----|------|
| D1 | `docker image inspect openkv-builder:dev` 成功 |
| D2 | 无 builder 时 `./scripts/docker-build-dev.sh openkv` 会先编 builder 再出 `openkv:dev` |
| D3 | 连续第二次 `./scripts/docker-build-dev.sh openkv` 明显快于首次（2026-11-12：首编 ~32min → 二次 ~2.8min，仅重链） |
