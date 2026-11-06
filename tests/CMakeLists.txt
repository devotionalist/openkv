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
# tests — GoogleTest 单元测试
# -----------------------------------------------------------------------------
# 单一可执行文件 opskv_unit_tests，内含各模块 TEST()/TEST_F()。
# gtest_discover_tests 会在构建后扫描用例，把每个 TEST 注册成独立 ctest 项，
# 便于：
#   ctest -R RaftLogTest
#   ctest -L unit --parallel 4
# =============================================================================

# ---- 测试源文件清单（按模块分子目录，便于维护） ----------------------------
set(OPSKV_TEST_SOURCES
  # common：哈希、Range、配置、Admin ACL
  common/hash_test.cpp
  common/region_test.cpp
  common/config_test.cpp
  common/admin_acl_test.cpp
  # raft：日志、单节点、MultiRaft、持久化恢复
  raft/raft_log_test.cpp
  raft/raft_node_test.cpp
  raft/multi_raft_test.cpp
  raft/raft_persist_test.cpp
  raft/raft_engine_test.cpp
  storage/rocks_raft_engine_test.cpp
  raft/snap_stream_test.cpp
  raft/raft_joint_test.cpp
  raft/raft_read_index_test.cpp
  raft/raw_node_test.cpp
  raft/raftstore_test.cpp
  # storage：引擎、命令编解码、StoreFacade 端到端
  storage/kv_engine_test.cpp
  storage/store_codec_test.cpp
  storage/store_facade_test.cpp
  storage/mvcc_test.cpp
  storage/watch_hub_test.cpp
  storage/store_admin_raft_test.cpp
  storage/latency_hist_test.cpp
  storage/watch_log_test.cpp
  storage/inflight_gate_test.cpp
  storage/scheduler_cop_test.cpp
  # meta：集群状态、扩容编排、HA FSM
  meta/cluster_state_test.cpp
  meta/scaler_test.cpp
  meta/meta_ha_test.cpp
  meta/region_scheduler_test.cpp
  # compute：路由与 Proxy 重试
  compute/router_test.cpp
  compute/kv_proxy_test.cpp
)

add_executable(opskv_unit_tests ${OPSKV_TEST_SOURCES})

# gtest_main 提供 main()；gmock 备用
# 业务库全部 PRIVATE 链接进测试二进制
target_link_libraries(opskv_unit_tests
  PRIVATE
    GTest::gtest_main
    GTest::gmock
    opskv_common
    opskv_raft
    opskv_storage
    opskv_meta
    opskv_compute
)

target_compile_features(opskv_unit_tests PRIVATE cxx_std_20)
if(COMMAND opskv_link_jemalloc)
  opskv_link_jemalloc(opskv_unit_tests)
endif()

# 将每个 TEST() 注册为 ctest 用例；WORKING_DIRECTORY 设为源码根，
# 方便测试里用相对路径读 conf/（若需要）
gtest_discover_tests(opskv_unit_tests
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  PROPERTIES LABELS "unit"
  DISCOVERY_TIMEOUT 60
)
