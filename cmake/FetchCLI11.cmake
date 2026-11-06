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

# Bundled CLI11：skv 等 CLI 参数解析。导出 CLI11::CLI11。

include(FetchContent)

set(OPSKV_CLI11_VERSION "2.4.2" CACHE STRING "Bundled CLI11 version")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(CLI11_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CLI11_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CLI11_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(CLI11_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  cli11
  URL https://github.com/CLIUtils/CLI11/archive/refs/tags/v${OPSKV_CLI11_VERSION}.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

message(STATUS "Fetching CLI11 v${OPSKV_CLI11_VERSION} …")
FetchContent_MakeAvailable(cli11)

if(NOT TARGET CLI11::CLI11)
  message(FATAL_ERROR "Bundled CLI11 did not create CLI11::CLI11")
endif()
message(STATUS "CLI11 v${OPSKV_CLI11_VERSION} enabled (FetchContent)")
