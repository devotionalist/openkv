# OpenKV C++ 运行镜像：meta_server + storage_node（同一镜像，按 OPENKV_ROLE 区分）
# 构建：docker build -f deploy/Dockerfile -t openkv:dev .

FROM ubuntu:24.04 AS build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git pkg-config ccache \
    protobuf-compiler-grpc libgrpc++-dev libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -B build \
      -DOPENKV_BUILD_TESTS=OFF \
 && cmake --build build -j"$(nproc)" --target meta_server storage_node

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash \
      libgrpc++1.51t64 libprotobuf32t64 \
    || (apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates bash libgrpc++1 libprotobuf23) \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/meta/meta_server /opt/openkv/meta_server
COPY --from=build /src/build/storage/storage_node /opt/openkv/storage_node
COPY deploy/docker-entrypoint.sh /opt/openkv/docker-entrypoint.sh
RUN chmod +x /opt/openkv/docker-entrypoint.sh \
 && mkdir -p /data /etc/openkv
WORKDIR /opt/openkv
ENV OPENKV_ROLE=storage
ENTRYPOINT ["/opt/openkv/docker-entrypoint.sh"]
