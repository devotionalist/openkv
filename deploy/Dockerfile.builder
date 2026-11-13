# OpenKV 编译基础镜像：工具链 + 系统 gRPC/Protobuf + ccache
# 不随业务代码变化；apt / 依赖大版本变更时才重建。
#
#   docker build -f deploy/Dockerfile.builder -t openkv-builder:dev .
#
# 业务镜像（Dockerfile / Dockerfile.gateway）FROM 本镜像，并用 BuildKit
# cache mount 保住 /src/build 与 ccache，避免每次重编 RocksDB。

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    CCACHE_DIR=/root/.ccache \
    PATH=/usr/lib/ccache:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake git pkg-config ccache ca-certificates \
      protobuf-compiler-grpc libgrpc++-dev libprotobuf-dev \
      zlib1g-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ccache --set-config=max_size=8G \
    && ccache --set-config=compression=true \
    && ccache -z

WORKDIR /src
