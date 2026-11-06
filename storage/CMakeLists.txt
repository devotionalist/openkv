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
# storage — 存储层（引擎 + Raft Apply + TCP 内部协议 + 节点进程）
# -----------------------------------------------------------------------------
# 产出目标：
#   opskv_storage  — 静态库（排除 main.cpp）
#   storage_node       — 可执行文件，监听 TCP，对接 Compute Gateway
#
# 硬依赖：
#   RocksDB（RocksDB::rocksdb）— FetchContent 固定版本，与工程一起编译；
#   存储节点启动默认打开 RocksDB
#
# 可选：
#   Seastar → OPSKV_HAS_SEASTAR=1（预留 reactor 集成）
# =============================================================================

# 库代码不含入口；main.cpp 单独给 executable
skv_collect_sources(STORAGE_SRCS DIR src EXCLUDE main.cpp)

add_library(opskv_storage ${STORAGE_SRCS})
target_include_directories(opskv_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 始终启用 RocksDB 实现（kv_engine.cpp 中 #if OPSKV_HAS_ROCKSDB）
target_compile_definitions(opskv_storage PUBLIC OPSKV_HAS_ROCKSDB=1)

# 存储层：Raft + 公共库 + RocksDB；proto 未开 gRPC 时为 INTERFACE 占位
target_link_libraries(opskv_storage
  PUBLIC
    opskv_raft
    opskv_common
    opskv_proto
    RocksDB::rocksdb
)

if(OPSKV_USE_SEASTAR)
  target_compile_definitions(opskv_storage PUBLIC OPSKV_HAS_SEASTAR=1)
  target_link_libraries(opskv_storage PUBLIC Seastar::seastar)
endif()

add_executable(storage_node src/main.cpp)
target_link_libraries(storage_node PRIVATE opskv_storage)
if(COMMAND opskv_link_jemalloc)
  opskv_link_jemalloc(storage_node)
endif()
