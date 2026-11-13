# openkv_raft：协议核 (openkv::raft) + 运行时 (openkv::raftstore)。
# 文件职责见 raft/README.md 与各源文件 @file 注释。

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
# 产出目标：openkv_raft
# 内容：RaftLog / RaftNode（单分片）/ MultiRaft（本节点托管的多个 Group）
# 依赖：openkv_common
# =============================================================================

skv_collect_sources(RAFT_SRCS DIR src)

add_library(openkv_raft ${RAFT_SRCS})
target_include_directories(openkv_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(openkv_raft PUBLIC openkv_common)
# GrpcRaftTransport 依赖 proto（gRPC 强制）
target_link_libraries(openkv_raft PUBLIC openkv_proto)
