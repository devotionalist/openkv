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
# FetchGoogleTest.cmake — 通过 FetchContent 拉取 GoogleTest
# -----------------------------------------------------------------------------
# 目的：
#   无需系统预装 gtest；首次配置时自动下载固定版本 zip 并加入构建树。
#
# 固定 v1.15.2 的原因：
#   避免跟踪 HEAD 导致 CI 行为漂移；需要升级时只改 URL/tag。
#
# 提供的目标（供 tests/CMakeLists.txt 链接）：
#   GTest::gtest / GTest::gtest_main / GTest::gmock / GTest::gmock_main
#
# 另：加载 CMake 自带 GoogleTest.cmake，以使用 gtest_discover_tests()。
# =============================================================================

include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

# MSVC 下与动态 CRT 对齐；其它平台无害
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# 开启 GoogleMock（本仓库部分测试可选用）
set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
# 不把 gtest 安装到系统前缀，仅参与本工程构建
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# 注意：不要 include(GoogleTest) 走 CMAKE_MODULE_PATH，
# 否则可能递归到本文件；显式加载 CMake 发行版自带模块。
include(${CMAKE_ROOT}/Modules/GoogleTest.cmake)
