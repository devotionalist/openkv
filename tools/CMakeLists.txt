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
# tools — 运维 / 辅助可执行文件
# =============================================================================

add_executable(skv
  skv/main.cpp
  skv/cluster.cpp
)
target_include_directories(skv PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/skv
)
target_link_libraries(skv PRIVATE opskv_common opskv_proto CLI11::CLI11)
target_compile_features(skv PRIVATE cxx_std_20)
if(COMMAND opskv_link_jemalloc)
  opskv_link_jemalloc(skv)
endif()

# 安装到 build/tools/skv，方便 PATH / scripts 引用
set_target_properties(skv PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tools
)
