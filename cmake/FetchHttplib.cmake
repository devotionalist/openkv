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

# Bundled cpp-httplib：可选 Prometheus /metrics HTTP。导出 httplib::httplib。
# 使用 release 压缩包，避免 git 浅克隆卡住。

include(FetchContent)

set(OPSKV_HTTPLIB_VERSION "0.18.3" CACHE STRING "Bundled cpp-httplib version")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(HTTPLIB_USE_OPENSSL_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZLIB_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_BROTLI_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_INSTALL OFF CACHE BOOL "" FORCE)
set(HTTPLIB_COMPILE OFF CACHE BOOL "" FORCE)  # header-only

FetchContent_Declare(
  httplib
  URL https://github.com/yhirose/cpp-httplib/archive/refs/tags/v${OPSKV_HTTPLIB_VERSION}.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

message(STATUS "Fetching cpp-httplib v${OPSKV_HTTPLIB_VERSION} …")
FetchContent_MakeAvailable(httplib)

if(NOT TARGET httplib::httplib)
  if(TARGET httplib)
    add_library(httplib::httplib ALIAS httplib)
  else()
    # 极简 fallback：仅头文件
    add_library(httplib INTERFACE)
    add_library(httplib::httplib ALIAS httplib)
    target_include_directories(httplib INTERFACE ${httplib_SOURCE_DIR})
  endif()
endif()
message(STATUS "cpp-httplib v${OPSKV_HTTPLIB_VERSION} enabled (FetchContent)")
