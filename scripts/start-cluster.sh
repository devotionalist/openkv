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

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
RUN_DIR="${RUN_DIR:-$ROOT/.run}"
GATEWAY_PORT="${GATEWAY_PORT:-19000}"
mkdir -p "$RUN_DIR" "$ROOT/data"

# 编译并行度：默认约半核、上限 4；可用 BUILD_JOBS=N 覆盖
_ncpu="$(sysctl -n hw.logicalcpu 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
_default_jobs=$(( _ncpu / 2 ))
[[ "$_default_jobs" -lt 1 ]] && _default_jobs=1
[[ "$_default_jobs" -gt 4 ]] && _default_jobs=4
BUILD_JOBS="${BUILD_JOBS:-$_default_jobs}"

# 默认不重复 cmake 配置（避免每次 “Fetching jemalloc/grpc”）；FORCE_CMAKE=1 强制重配
# SKIP_BUILD=1 完全跳过编译（二进制已存在时直接起集群）
_meta_bin="$BUILD_DIR/meta/meta_server"
_stor_bin="$BUILD_DIR/storage/storage_node"

# brew 升级中断时可能把 /usr/local/bin/cmake symlink 摘掉
if ! command -v cmake >/dev/null 2>&1; then
  for _cand in \
    "$(command -v brew >/dev/null 2>&1 && brew --prefix cmake 2>/dev/null)/bin/cmake" \
    /usr/local/opt/cmake/bin/cmake \
    /opt/homebrew/opt/cmake/bin/cmake; do
    if [[ -n "$_cand" && -x "$_cand" ]]; then
      export PATH="$(dirname "$_cand"):$PATH"
      break
    fi
  done
fi
if ! command -v cmake >/dev/null 2>&1; then
  echo "cmake: command not found" >&2
  echo "hint: brew link cmake   # 或 brew install cmake；升级中断常导致 symlink 丢失" >&2
  exit 1
fi

if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
  if [[ ! -x "$_meta_bin" || ! -x "$_stor_bin" ]]; then
    echo "SKIP_BUILD=1 but missing $_meta_bin / $_stor_bin" >&2
    exit 1
  fi
  echo "==> skip C++ build (SKIP_BUILD=1)"
else
  # 仓库改名/搬迁后 CMakeCache 仍写旧路径（如 cplusdemo→opskv）会导致 build 失败。
  # 只清缓存、保留 _deps，避免重新下载 RocksDB/gRPC。
  _cmake_stale=0
  if [[ -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    _cached_home="$(grep -E '^CMAKE_HOME_DIRECTORY:' "$BUILD_DIR/CMakeCache.txt" 2>/dev/null | head -1 | cut -d= -f2- || true)"
    _cached_src="$(grep -E '^opskv_SOURCE_DIR:' "$BUILD_DIR/CMakeCache.txt" 2>/dev/null | head -1 | cut -d= -f2- || true)"
    if [[ -n "$_cached_home" && "$_cached_home" != "$ROOT" ]] || \
       [[ -n "$_cached_src" && "$_cached_src" != "$ROOT" ]]; then
      echo "==> CMakeCache path mismatch: home=${_cached_home:-?} src=${_cached_src:-?} current=$ROOT"
      echo "==> reset cmake files in $BUILD_DIR (keep _deps)"
      _cmake_stale=1
      _deps_bak=""
      if [[ -d "$BUILD_DIR/_deps" ]]; then
        _deps_bak="$(mktemp -d "${TMPDIR:-/tmp}/opskv-deps.XXXXXX")"
        mv "$BUILD_DIR/_deps" "$_deps_bak/_deps"
      fi
      rm -rf "$BUILD_DIR"
      mkdir -p "$BUILD_DIR"
      if [[ -n "$_deps_bak" && -d "$_deps_bak/_deps" ]]; then
        mv "$_deps_bak/_deps" "$BUILD_DIR/_deps"
        rmdir "$_deps_bak" 2>/dev/null || true
      fi
    fi
  fi

  # 若本树 rocksdb-src 空且旁路 build-nogrpc 有完整源码，借过来避免外网 zip 失败
  if [[ ! -f "$BUILD_DIR/_deps/rocksdb-src/CMakeLists.txt" ]] && \
     [[ -f "$ROOT/build-nogrpc/_deps/rocksdb-src/CMakeLists.txt" ]]; then
    echo "==> seed rocksdb-src from build-nogrpc/_deps"
    mkdir -p "$BUILD_DIR/_deps"
    rm -rf "$BUILD_DIR/_deps/rocksdb-src"
    cp -a "$ROOT/build-nogrpc/_deps/rocksdb-src" "$BUILD_DIR/_deps/rocksdb-src"
  fi

  _cmake_extra=()
  if [[ -f "$BUILD_DIR/_deps/rocksdb-src/CMakeLists.txt" ]]; then
    _cmake_extra+=("-DFETCHCONTENT_SOURCE_DIR_ROCKSDB=$ROOT/$BUILD_DIR/_deps/rocksdb-src")
  fi

  # 浅克隆 gRPC（无递归子模块）；避免 FetchContent 默认 --recurse 卡在 opentelemetry/vcpkg
  if [[ ! -f "$BUILD_DIR/_deps/grpc-src/CMakeLists.txt" ]]; then
    echo "==> seed grpc-src (shallow, no submodules; tag ${OPSKV_GRPC_GIT_TAG:-v1.67.1})"
    mkdir -p "$BUILD_DIR/_deps"
    _grpc_url="${OPSKV_GRPC_GIT_REPOSITORY:-https://github.com/grpc/grpc.git}"
    _grpc_tag="${OPSKV_GRPC_GIT_TAG:-v1.67.1}"
    if ! GIT_TERMINAL_PROMPT=0 git clone --depth 1 --branch "$_grpc_tag" --single-branch \
         "$_grpc_url" "$BUILD_DIR/_deps/grpc-src"; then
      echo "hint: GitHub 慢可 OPSKV_GRPC_GIT_REPOSITORY=https://mirror.ghproxy.com/https://github.com/grpc/grpc.git" >&2
      echo "hint: 或 brew install grpc protobuf abseil c-ares re2 后 FORCE_CMAKE=1" >&2
      exit 1
    fi
  fi
  if [[ -f "$BUILD_DIR/_deps/grpc-src/CMakeLists.txt" ]]; then
    _cmake_extra+=("-DFETCHCONTENT_SOURCE_DIR_GRPC=$ROOT/$BUILD_DIR/_deps/grpc-src")
  fi

  # 旧 build 曾 OPSKV_USE_GRPC=OFF：必须重配（gRPC 已强制）
  _grpc_cache=""
  if [[ -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    _grpc_cache="$(grep -E '^OPSKV_USE_GRPC:' "$BUILD_DIR/CMakeCache.txt" 2>/dev/null | head -1 | cut -d= -f2- || true)"
  fi
  if [[ "$_grpc_cache" == "OFF" ]]; then
    echo "==> CMakeCache OPSKV_USE_GRPC=OFF → force reconfigure (gRPC mandatory)"
    _cmake_stale=1
  fi

  if [[ "${FORCE_CMAKE:-0}" == "1" || "$_cmake_stale" == "1" || ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    echo "==> cmake configure (gRPC mandatory; first time / FORCE_CMAKE=1 / stale cache)"
    cmake -B "$BUILD_DIR" \
      -DOPSKV_USE_SEASTAR=OFF -DOPSKV_USE_GRPC=ON \
      -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_AR=/usr/bin/ar -DCMAKE_RANLIB=/usr/bin/ranlib \
      "${_cmake_extra[@]+"${_cmake_extra[@]}"}"
  else
    echo "==> reuse $BUILD_DIR (skip cmake configure; FORCE_CMAKE=1 to redo)"
  fi
  echo "==> cmake --build -j${BUILD_JOBS} (incremental)"
  cmake --build "$BUILD_DIR" -j"${BUILD_JOBS}" --target meta_server storage_node
fi

echo "==> build Go compute-gateway + client"
(cd "$ROOT/client/go" && go build -o "$RUN_DIR/compute-gateway" ./cmd/compute-gateway \
  && go build -o "$RUN_DIR/kv-example" ./examples/basic)

echo "==> start cluster (daemon)"
# 本地 RocksDB/Raft 损坏时：SKV_FRESH=1 ./scripts/start-cluster.sh
if [[ "${SKV_FRESH:-0}" == "1" ]]; then
  echo "==> SKV_FRESH=1: wipe data/"
  rm -rf "$ROOT/data"
  mkdir -p "$ROOT/data"
fi
python3 "$ROOT/scripts/daemon_cluster.py"

# parent of daemon returns immediately; wait until ports are up
for i in $(seq 1 150); do
  if python3 -c "import socket;s=socket.create_connection(('127.0.0.1',$GATEWAY_PORT),0.2);s.close()" 2>/dev/null; then
    break
  fi
  sleep 0.2
  if [[ $i -eq 150 ]]; then
    echo "gateway not ready; see $RUN_DIR/*.log" >&2
    for f in storage-0 storage-1 storage-2 meta-0 gateway; do
      if [[ -f "$RUN_DIR/$f.log" ]] && grep -qE 'Corruption|rocksdb open failed|terminating|Error|FATAL|not ready' "$RUN_DIR/$f.log" 2>/dev/null; then
        echo "--- $f.log ---" >&2
        tail -20 "$RUN_DIR/$f.log" >&2 || true
      fi
    done
    if grep -qE 'LOCK: Resource temporarily unavailable|While lock file' "$RUN_DIR"/storage-*.log 2>/dev/null; then
      echo "hint: RocksDB LOCK 被旧进程占用 → make cluster-down 后再 start（不必 SKV_FRESH）" >&2
    else
      echo "hint: RocksDB 损坏可 SKV_FRESH=1 ./scripts/start-cluster.sh" >&2
    fi
    exit 1
  fi
done

echo "cluster started"
for f in meta-0 meta-1 meta-2 storage-0 storage-1 storage-2 storage-3 storage-4 storage-5 gateway; do
  echo "  $f pid=$(cat "$RUN_DIR/$f.pid" 2>/dev/null || echo '?') log=$RUN_DIR/$f.log"
done
echo "gateway: 127.0.0.1:$GATEWAY_PORT"
echo "test:    ./.run/kv-example 127.0.0.1:$GATEWAY_PORT"
echo "admin:   make admin-run  →  http://127.0.0.1:19100"
echo "stop:    make cluster-down"

# 可选：一并拉起独立 Web 管控模块（SKV_ADMIN=1）
if [[ "${SKV_ADMIN:-0}" == "1" ]]; then
  (cd "$ROOT/web/frontend" && npm install --no-fund --no-audit && npm run build)
  (cd "$ROOT/web" && go build -o "$RUN_DIR/skv-web" .)
  SKV_RUN="$RUN_DIR" SKV_ROOT="$ROOT" "$RUN_DIR/skv-web" \
    -listen "0.0.0.0:19100" \
    -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
    -storages 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105 \
    -gateway "127.0.0.1:$GATEWAY_PORT" \
    -run-dir "$RUN_DIR" \
    -root "$ROOT" \
    >"$RUN_DIR/web.log" 2>&1 &
  echo $! >"$RUN_DIR/web.pid"
  echo "web:     http://127.0.0.1:19100  (pid=$(cat "$RUN_DIR/web.pid"))"
fi
