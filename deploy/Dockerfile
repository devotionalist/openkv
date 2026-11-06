# OpsKV C++ 运行镜像：meta_server + storage_node（同一镜像，按 OPSKV_ROLE 区分）
# 构建：docker build -f deploy/Dockerfile -t opskv:dev .

FROM ubuntu:24.04 AS build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git pkg-config ccache \
    protobuf-compiler-grpc libgrpc++-dev libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -B build \
      -DOPSKV_USE_SEASTAR=OFF \
      -DOPSKV_BUILD_TESTS=OFF \
 && cmake --build build -j"$(nproc)" --target meta_server storage_node

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash \
      libgrpc++1.51t64 libprotobuf32t64 \
    || (apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash libgrpc++1 libprotobuf23) \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/meta/meta_server /opt/opskv/meta_server
COPY --from=build /src/build/storage/storage_node /opt/opskv/storage_node
COPY deploy/docker-entrypoint.sh /opt/opskv/docker-entrypoint.sh
RUN chmod +x /opt/opskv/docker-entrypoint.sh \
 && mkdir -p /data /etc/opskv
WORKDIR /opt/opskv
ENV OPSKV_ROLE=storage
ENTRYPOINT ["/opt/opskv/docker-entrypoint.sh"]
