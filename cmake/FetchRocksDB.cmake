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

# Bundled RocksDB (FetchContent): default v9.10.0, static lib, minimal deps.
# Exports RocksDB::rocksdb (ALIAS of target rocksdb).

include(FetchContent)

set(OPSKV_ROCKSDB_VERSION "9.10.0" CACHE STRING "Bundled RocksDB version (FetchContent tag)")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(_rocksdb_off
  WITH_GFLAGS WITH_TESTS WITH_BENCHMARK_TOOLS WITH_CORE_TOOLS WITH_TOOLS
  WITH_TRACE_TOOLS WITH_JNI WITH_SNAPPY WITH_LZ4 WITH_ZSTD WITH_BZ2 WITH_LIBURING
)
foreach(_opt IN LISTS _rocksdb_off)
  set(${_opt} OFF CACHE BOOL "" FORCE)
endforeach()
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  rocksdb
  URL https://github.com/facebook/rocksdb/archive/refs/tags/v${OPSKV_ROCKSDB_VERSION}.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

message(STATUS "Fetching RocksDB v${OPSKV_ROCKSDB_VERSION} (build with project)...")
FetchContent_MakeAvailable(rocksdb)

if(TARGET rocksdb AND NOT TARGET RocksDB::rocksdb)
  add_library(RocksDB::rocksdb ALIAS rocksdb)
endif()
if(NOT TARGET RocksDB::rocksdb)
  message(FATAL_ERROR "Bundled RocksDB did not create target 'rocksdb'")
endif()

message(STATUS "RocksDB v${OPSKV_ROCKSDB_VERSION} enabled (FetchContent, static)")
