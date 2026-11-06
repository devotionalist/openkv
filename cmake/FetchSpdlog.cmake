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

# Bundled spdlog（含 fmt）：日志后端。导出 spdlog::spdlog。

include(FetchContent)

set(OPSKV_SPDLOG_VERSION "1.14.1" CACHE STRING "Bundled spdlog version")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)
set(SPDLOG_FMT_EXTERNAL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  spdlog
  URL https://github.com/gabime/spdlog/archive/refs/tags/v${OPSKV_SPDLOG_VERSION}.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

message(STATUS "Fetching spdlog v${OPSKV_SPDLOG_VERSION} …")
FetchContent_MakeAvailable(spdlog)

if(NOT TARGET spdlog::spdlog)
  message(FATAL_ERROR "Bundled spdlog did not create spdlog::spdlog")
endif()
message(STATUS "spdlog v${OPSKV_SPDLOG_VERSION} enabled (FetchContent)")
