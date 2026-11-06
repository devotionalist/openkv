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

# =============================================================================
# Sanitizers.cmake — 运行时内存 / UB / 数据竞争检查
# -----------------------------------------------------------------------------
# 支持开关（可在 cmake 命令行传入）：
#   -DOPSKV_ENABLE_ASAN=ON   AddressSanitizer（+ Linux 上的 LeakSanitizer）
#   -DOPSKV_ENABLE_UBSAN=ON  UndefinedBehaviorSanitizer
#   -DOPSKV_ENABLE_TSAN=ON   ThreadSanitizer（不可与 ASan/UBSan 同开）
#
# 推荐用法（独立构建目录，避免污染普通 Release/Debug）：
#   cmake -B build-asan -DOPSKV_ENABLE_ASAN=ON -DOPSKV_ENABLE_UBSAN=ON ...
#   或直接：make memcheck
#
# 平台说明：
#   - Linux：ASan 可叠加 -fsanitize=leak
#   - macOS AppleClang：不支持 detect_leaks；memcheck.sh 会优先选 Homebrew LLVM
# =============================================================================

option(OPSKV_ENABLE_ASAN  "开启 AddressSanitizer（含泄漏检测，视平台而定）" OFF)
option(OPSKV_ENABLE_UBSAN "开启 UndefinedBehaviorSanitizer" OFF)
option(OPSKV_ENABLE_TSAN  "开启 ThreadSanitizer（与 ASan/UBSan 互斥）" OFF)

function(opskv_apply_sanitizers)
  set(_san_flags "")
  set(_san_link "")

  # TSan 与 ASan 运行时冲突，禁止同时开启
  if(OPSKV_ENABLE_TSAN AND (OPSKV_ENABLE_ASAN OR OPSKV_ENABLE_UBSAN))
    message(FATAL_ERROR "TSan 不能与 ASan/UBSan 同时开启")
  endif()

  if(OPSKV_ENABLE_ASAN)
    # -fno-omit-frame-pointer：保留栈帧，便于 ASan 打印可读调用栈
    list(APPEND _san_flags -fsanitize=address -fno-omit-frame-pointer -g)
    list(APPEND _san_link -fsanitize=address)
    # Linux 显式打开 LSan；AppleClang 不支持，改由 memcheck.sh 选用 LLVM clang++
    if(NOT APPLE)
      list(APPEND _san_flags -fsanitize=leak)
      list(APPEND _san_link -fsanitize=leak)
    endif()
    message(STATUS "Sanitizer: AddressSanitizer ON")
  endif()

  if(OPSKV_ENABLE_UBSAN)
    # -fno-sanitize-recover：遇到 UB 立即失败，避免静默继续
    list(APPEND _san_flags -fsanitize=undefined -fno-sanitize-recover=undefined -g)
    list(APPEND _san_link -fsanitize=undefined)
    message(STATUS "Sanitizer: UndefinedBehaviorSanitizer ON")
  endif()

  if(OPSKV_ENABLE_TSAN)
    list(APPEND _san_flags -fsanitize=thread -fno-omit-frame-pointer -g)
    list(APPEND _san_link -fsanitize=thread)
    message(STATUS "Sanitizer: ThreadSanitizer ON")
  endif()

  if(_san_flags)
    # 全局施加到后续所有 target（本文件在 add_subdirectory 之前 include）
    add_compile_options(${_san_flags})
    add_link_options(${_san_link})
    # 代码里可用 #ifdef OPSKV_SANITIZER 做条件逻辑
    add_compile_definitions(OPSKV_SANITIZER=1)
  endif()
endfunction()

# 配置阶段立即应用（若对应 option 为 ON）
opskv_apply_sanitizers()
