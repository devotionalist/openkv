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

# Bundled ScyllaDB Seastar (FetchContent): 与工程一起编译，导出 Seastar::seastar。
# 上游：https://github.com/scylladb/seastar
#
# 系统级依赖（boost / hwloc / gnutls / liburing …）仍须本机已装：
#   git clone … && ./install-dependencies.sh
# 本文件只拉取并编译 Seastar 源码本身（不再要求预先 cmake --install Seastar）。

include(FetchContent)

set(OPSKV_SEASTAR_GIT_TAG "seastar-25.05.0" CACHE STRING
    "Bundled scylladb/seastar git tag / commit (FetchContent)")

set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# 作为子工程：只编库，关 DPDK / 测试 / 示例 / 安装
set(Seastar_APPS OFF CACHE BOOL "" FORCE)
set(Seastar_TESTING OFF CACHE BOOL "" FORCE)
set(Seastar_DEMOS OFF CACHE BOOL "" FORCE)
set(Seastar_DOCS OFF CACHE BOOL "" FORCE)
set(Seastar_DPDK OFF CACHE BOOL "" FORCE)
set(Seastar_INSTALL OFF CACHE BOOL "" FORCE)
set(Seastar_EXCLUDE_APPS_FROM_ALL ON CACHE BOOL "" FORCE)
set(Seastar_EXCLUDE_TESTS_FROM_ALL ON CACHE BOOL "" FORCE)
set(Seastar_EXCLUDE_DEMOS_FROM_ALL ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  seastar
  GIT_REPOSITORY https://github.com/scylladb/seastar.git
  GIT_TAG ${OPSKV_SEASTAR_GIT_TAG}
  GIT_SHALLOW TRUE
  # DPDK 已关：跳过 .gitmodules 里的 dpdk，避免无用子模块拉取失败
  GIT_SUBMODULES ""
)

message(STATUS "Fetching scylladb/seastar (${OPSKV_SEASTAR_GIT_TAG}) …")
FetchContent_MakeAvailable(seastar)

if(TARGET seastar AND NOT TARGET Seastar::seastar)
  add_library(Seastar::seastar ALIAS seastar)
endif()
if(NOT TARGET Seastar::seastar)
  message(FATAL_ERROR
    "Bundled Seastar did not create Seastar::seastar. "
    "Install system deps first: https://github.com/scylladb/seastar "
    "(./install-dependencies.sh), then reconfigure.")
endif()

message(STATUS
  "Seastar enabled (FetchContent ${OPSKV_SEASTAR_GIT_TAG} → Seastar::seastar)")
