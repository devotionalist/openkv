#!/usr/bin/env bash
# Copyright 2026 devotionalist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 在同一进程树内：编译 → 启集群 → 测客户端 → 停集群
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
RUN_DIR="${RUN_DIR:-$ROOT/.run}"
GATEWAY_ADDR="${GATEWAY_ADDR:-127.0.0.1:19000}"
GATEWAY_PORT="${GATEWAY_ADDR##*:}"
mkdir -p "$RUN_DIR" "$ROOT/data"

cleanup() {
  echo "==> cleanup"
  for f in gateway storage-0 storage-1 storage-2 meta-0 meta-1 meta-2 meta; do
    if [[ -f "$RUN_DIR/$f.pid" ]]; then
      pid="$(cat "$RUN_DIR/$f.pid" || true)"
      if [[ -n "${pid:-}" ]]; then
        kill "$pid" 2>/dev/null || true
      fi
      rm -f "$RUN_DIR/$f.pid"
    fi
  done
}
trap cleanup EXIT

echo "==> build C++"
cmake -B "$BUILD_DIR" \
  -DOPSKV_USE_SEASTAR=OFF -DOPSKV_USE_GRPC="${OPSKV_USE_GRPC:-ON}" \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_AR=/usr/bin/ar -DCMAKE_RANLIB=/usr/bin/ranlib \
  >/dev/null
cmake --build "$BUILD_DIR" -j --target meta_server storage_node

echo "==> build Go"
(cd "$ROOT/client/go" && go build -o "$RUN_DIR/compute-gateway" ./cmd/compute-gateway \
  && go build -o "$RUN_DIR/kv-example" ./examples/basic \
  && go build -o "$RUN_DIR/kv-e2e" ./examples/e2e)

cleanup || true

wait_port() {
  local host="$1" port="$2" name="$3"
  for _ in $(seq 1 100); do
    if python3 -c "import socket;s=socket.create_connection(('$host',$port),0.2);s.close()" 2>/dev/null; then
      echo "  $name ready on $host:$port"
      return 0
    fi
    sleep 0.2
  done
  echo "timeout waiting $name on $host:$port"
  return 1
}

echo "==> start meta (3-replica Raft)"
"$BUILD_DIR/meta/meta_server" conf/bootstrap.conf >"$RUN_DIR/meta-0.log" 2>&1 &
echo $! >"$RUN_DIR/meta-0.pid"
"$BUILD_DIR/meta/meta_server" conf/meta-1.conf >"$RUN_DIR/meta-1.log" 2>&1 &
echo $! >"$RUN_DIR/meta-1.pid"
"$BUILD_DIR/meta/meta_server" conf/meta-2.conf >"$RUN_DIR/meta-2.log" 2>&1 &
echo $! >"$RUN_DIR/meta-2.pid"
wait_port 127.0.0.1 8500 meta-0
wait_port 127.0.0.1 8501 meta-1
wait_port 127.0.0.1 8502 meta-2

echo "==> start storages"
"$BUILD_DIR/storage/storage_node" conf/storage.conf >"$RUN_DIR/storage-0.log" 2>&1 &
echo $! >"$RUN_DIR/storage-0.pid"
"$BUILD_DIR/storage/storage_node" conf/storage-1.conf >"$RUN_DIR/storage-1.log" 2>&1 &
echo $! >"$RUN_DIR/storage-1.pid"
"$BUILD_DIR/storage/storage_node" conf/storage-2.conf >"$RUN_DIR/storage-2.log" 2>&1 &
echo $! >"$RUN_DIR/storage-2.pid"

wait_port 127.0.0.1 9100 storage-0
wait_port 127.0.0.1 9101 storage-1
wait_port 127.0.0.1 9102 storage-2

echo "==> start compute-gateway"
"$RUN_DIR/compute-gateway" \
  -listen "0.0.0.0:${GATEWAY_PORT}" \
  -storages 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 \
  -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
  >"$RUN_DIR/gateway.log" 2>&1 &
echo $! >"$RUN_DIR/gateway.pid"
wait_port 127.0.0.1 "$GATEWAY_PORT" gateway

echo "==> client example"
"$RUN_DIR/kv-example" "$GATEWAY_ADDR"

echo "==> client e2e"
"$RUN_DIR/kv-e2e" -addr "$GATEWAY_ADDR"

echo
echo "ALL CLIENT TESTS PASSED"
