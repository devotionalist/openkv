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
# common — 公共基础库（哈希、配置、日志、编解码、metrics HTTP）
# =============================================================================

skv_collect_sources(COMMON_SRCS DIR src)

add_library(opskv_common ${COMMON_SRCS})

target_include_directories(opskv_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(opskv_common
  PUBLIC
    Threads::Threads
    spdlog::spdlog
    toml11::toml11
    httplib::httplib
)

if(OPSKV_USE_SEASTAR)
  target_compile_definitions(opskv_common PUBLIC OPSKV_HAS_SEASTAR=1)
  target_link_libraries(opskv_common PUBLIC Seastar::seastar)
endif()
