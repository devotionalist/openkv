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
# meta — 元数据服务（拓扑、路由表、在线扩容编排）
# -----------------------------------------------------------------------------
# 产出目标：
#   opskv_meta — ClusterState + Scaler + MetaNode
#   meta_server    — 进程入口（托管 MetaNode + MetaGrpcServer）
#
# 扩容主路径：AddLearner → Catch-up → PromoteVoter → 发布新 RoutingTable
# 高可用：Meta Raft 多副本（默认 3），多数派强一致
# =============================================================================

skv_collect_sources(META_SRCS DIR src EXCLUDE main.cpp)

add_library(opskv_meta ${META_SRCS})
target_include_directories(opskv_meta PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(opskv_meta PUBLIC opskv_common opskv_raft opskv_proto)

add_executable(meta_server src/main.cpp)
target_link_libraries(meta_server PRIVATE opskv_meta)
if(COMMAND opskv_link_jemalloc)
  opskv_link_jemalloc(meta_server)
endif()
# Seastar 由 opskv_common PUBLIC 传递；ON 时 meta 网络走 reactor（P1）
