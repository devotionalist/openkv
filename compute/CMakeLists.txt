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
# compute — 计算层（无状态 Proxy）
# -----------------------------------------------------------------------------
# 产出目标：
#   opskv_compute — 路由表 Router + KvProxy
#   compute_node      — 进程入口（当前为 demo；生产应对接 gRPC KVService）
#
# 设计要点：存算分离后 Compute 不落盘，只负责路由 / 重试 / 限流等。
# =============================================================================

skv_collect_sources(COMPUTE_SRCS DIR src EXCLUDE main.cpp)

add_library(opskv_compute ${COMPUTE_SRCS})
target_include_directories(opskv_compute PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(opskv_compute PUBLIC opskv_common opskv_proto)

if(OPSKV_USE_SEASTAR)
  target_compile_definitions(opskv_compute PUBLIC OPSKV_HAS_SEASTAR=1)
  target_link_libraries(opskv_compute PUBLIC Seastar::seastar)
endif()

add_executable(compute_node src/main.cpp)
target_link_libraries(compute_node PRIVATE opskv_compute)
if(COMMAND opskv_link_jemalloc)
  opskv_link_jemalloc(compute_node)
endif()
