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
# compute — 计算层（无状态 Proxy / Gateway）
# -----------------------------------------------------------------------------
# 产出目标：
#   openkv_compute — 路由表 Router + 出站客户端 + GatewayServer
#   gateway_node   — gRPC KVService+DSService 生产入口
# =============================================================================

skv_collect_sources(COMPUTE_SRCS DIR src EXCLUDE gateway_main.cpp)

add_library(openkv_compute ${COMPUTE_SRCS})
target_include_directories(openkv_compute PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(openkv_compute PUBLIC openkv_common openkv_proto)

add_executable(gateway_node src/gateway_main.cpp)
target_link_libraries(gateway_node PRIVATE openkv_compute CLI11::CLI11)
if(COMMAND openkv_link_jemalloc)
  openkv_link_jemalloc(gateway_node)
endif()
