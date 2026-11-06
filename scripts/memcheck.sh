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

# 内存检查：AddressSanitizer + UBSan + LeakSanitizer
# macOS 优先使用 Homebrew LLVM（AppleClang 不支持 detect_leaks）
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-build-asan}"
JOBS="${JOBS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)}"
SUPP="$ROOT/scripts/asan.supp"
OS="$(uname -s)"

pick_cxx() {
  if [[ -n "${CMAKE_CXX_COMPILER:-}" ]]; then
    echo "$CMAKE_CXX_COMPILER"
    return
  fi
  if [[ "$OS" == "Darwin" ]]; then
    for c in /usr/local/opt/llvm/bin/clang++ /opt/homebrew/opt/llvm/bin/clang++; do
      if [[ -x "$c" ]]; then
        echo "$c"
        return
      fi
    done
  fi
  echo /usr/bin/clang++
}

CXX="$(pick_cxx)"
echo "==> compiler: $CXX"

# Homebrew LLVM 的 ar/ranlib（若存在）
CMAKE_AR=/usr/bin/ar
CMAKE_RANLIB=/usr/bin/ranlib
LLVM_BIN="$(dirname "$CXX")"
if [[ -x "$LLVM_BIN/llvm-ar" ]]; then
  # 与 Apple ld 混用时仍用系统 ar 更稳；仅编译器用 LLVM
  :
fi

echo "==> configure ASan/LSan build -> $BUILD_DIR"
cmake -B "$BUILD_DIR" \
  -DOPSKV_ENABLE_ASAN=ON \
  -DOPSKV_ENABLE_UBSAN=ON \
  -DOPSKV_USE_SEASTAR=OFF \
  -DOPSKV_USE_GRPC="${OPSKV_USE_GRPC:-OFF}" \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DCMAKE_AR="$CMAKE_AR" \
  -DCMAKE_RANLIB="$CMAKE_RANLIB" \
  -DCMAKE_BUILD_TYPE=Debug

echo "==> build"
cmake --build "$BUILD_DIR" -j"$JOBS"

export ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:abort_on_error=1:detect_stack_use_after_return=1"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1"
if [[ -f "$SUPP" ]]; then
  export LSAN_OPTIONS="suppressions=${SUPP}:print_suppressions=0"
fi

# 若仍是 AppleClang，关闭 detect_leaks 避免 abort
if "$CXX" --version 2>&1 | grep -qi 'Apple clang'; then
  export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:abort_on_error=1:detect_stack_use_after_return=1"
  echo "==> warn: AppleClang 无 LSan，仅跑 ASan/UBSan；建议 brew install llvm"
fi

echo "==> ctest under sanitizers"
ctest --test-dir "$BUILD_DIR" --output-on-failure --timeout 120

echo "==> short binary smoke under ASan (SIGINT)"
run_smoke() {
  local bin="$1"
  local conf="$2"
  local log="$BUILD_DIR/$(basename "$bin").asan.log"
  "$bin" "$conf" >"$log" 2>&1 &
  local pid=$!
  sleep 2
  kill -INT "$pid" 2>/dev/null || true
  # 给进程一点时间跑 ASan 退出报告
  for _ in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  if grep -E "ERROR: (Address|Leak|UndefinedBehavior)Sanitizer|SUMMARY: .*Sanitizer" "$log"; then
    echo "sanitizer error in $log"
    cat "$log"
    return 1
  fi
  echo "  smoke ok: $(basename "$bin")"
}

run_smoke "$BUILD_DIR/storage/storage_node" "$ROOT/conf/storage.conf"
run_smoke "$BUILD_DIR/meta/meta_server" "$ROOT/conf/bootstrap.conf"

echo "MEMCHECK PASSED"
