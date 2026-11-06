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
# raft — Multi-Raft 共识库
# -----------------------------------------------------------------------------
# 产出目标：opskv_raft
# 内容：RaftLog / RaftNode（单分片）/ MultiRaft（本节点托管的多个 Group）
# 依赖：opskv_common
# =============================================================================

skv_collect_sources(RAFT_SRCS DIR src)

add_library(opskv_raft ${RAFT_SRCS})
target_include_directories(opskv_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(opskv_raft PUBLIC opskv_common)
# GrpcRaftTransport 依赖 proto（gRPC 强制）
target_link_libraries(opskv_raft PUBLIC opskv_proto)
