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

# Bundled toml11：可选 .toml 配置。导出 toml11::toml11。
# 使用 release 压缩包，避免 git submodule（tests/extlib/json）拖死配置。

include(FetchContent)

set(OPSKV_TOML11_VERSION "4.2.0" CACHE STRING "Bundled toml11 version")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(TOML11_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TOML11_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(TOML11_INSTALL OFF CACHE BOOL "" FORCE)
set(TOML11_PRECOMPILE OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  toml11
  URL https://github.com/ToruNiina/toml11/archive/refs/tags/v${OPSKV_TOML11_VERSION}.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

message(STATUS "Fetching toml11 v${OPSKV_TOML11_VERSION} …")
FetchContent_MakeAvailable(toml11)

if(NOT TARGET toml11::toml11)
  if(TARGET toml11)
    add_library(toml11::toml11 ALIAS toml11)
  else()
    message(FATAL_ERROR "Bundled toml11 did not create toml11::toml11")
  endif()
endif()
message(STATUS "toml11 v${OPSKV_TOML11_VERSION} enabled (FetchContent)")
