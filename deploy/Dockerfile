# syntax=docker/dockerfile:1
# OpenKV C++ 运行镜像：meta_server + storage_node（同一镜像，按 OPENKV_ROLE 区分）
# 依赖基础镜像 openkv-builder:dev（见 Dockerfile.builder）。
#
#   docker build -f deploy/Dockerfile.builder -t openkv-builder:dev .
#   DOCKER_BUILDKIT=1 docker build -f deploy/Dockerfile -t openkv:dev .
#
# BuildKit cache mount 保住 cmake build 树与 ccache：改业务代码时通常只重链
# storage/meta，不再全量编 RocksDB。

ARG BUILDER_IMAGE=openkv-builder:dev
FROM ${BUILDER_IMAGE} AS build
COPY . .
RUN --mount=type=cache,target=/root/.ccache \
    --mount=type=cache,id=openkv-cmake-build,target=/src/build \
    cmake -B build -DOPENKV_BUILD_TESTS=OFF \
 && cmake --build build -j"$(nproc)" --target meta_server storage_node \
 && ccache -s \
 && cp -a build/meta/meta_server build/storage/storage_node /tmp/

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash \
      libgrpc++1.51t64 libprotobuf32t64 \
    || (apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash libgrpc++1 libprotobuf23) \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /tmp/meta_server /opt/openkv/meta_server
COPY --from=build /tmp/storage_node /opt/openkv/storage_node
COPY deploy/docker-entrypoint.sh /opt/openkv/docker-entrypoint.sh
RUN chmod +x /opt/openkv/docker-entrypoint.sh \
 && mkdir -p /data /etc/openkv
WORKDIR /opt/openkv
ENV OPENKV_ROLE=storage
ENTRYPOINT ["/opt/openkv/docker-entrypoint.sh"]
