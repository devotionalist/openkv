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
# CollectSources.cmake — 自动收集源文件宏
# -----------------------------------------------------------------------------
# 作用：
#   用 GLOB_RECURSE + CONFIGURE_DEPENDS 扫描目录下的 .cpp/.cc/.cxx，
#   避免在 CMakeLists 里手工罗列每一个源文件。
#
# 注意：
#   - CONFIGURE_DEPENDS 要求 CMake >= 3.12；新增文件后下次构建会触发重新配置。
#   - 生产大型项目有时仍偏好显式列举（便于 review / 精确控制编译单元）。
#
# 用法示例：
#   skv_collect_sources(STORAGE_SRCS
#     DIR src                 # 相对当前 CMakeLists 的目录，默认 src
#     EXCLUDE main.cpp)       # 可排除可执行入口，避免链进静态库
#   add_library(opskv_storage ${STORAGE_SRCS})
# =============================================================================

macro(skv_collect_sources out_var)
  # 解析命名参数：DIR（单值）、EXCLUDE（列表）
  cmake_parse_arguments(_SKV "" "DIR" "EXCLUDE" ${ARGN})

  # 解析扫描根目录
  if(NOT _SKV_DIR)
    set(_SKV_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
  else()
    set(_SKV_DIR "${CMAKE_CURRENT_SOURCE_DIR}/${_SKV_DIR}")
  endif()

  # 递归收集 C++ 源文件；CONFIGURE_DEPENDS 使文件增删可被感知
  file(GLOB_RECURSE ${out_var} CONFIGURE_DEPENDS
    "${_SKV_DIR}/*.cpp"
    "${_SKV_DIR}/*.cc"
    "${_SKV_DIR}/*.cxx"
  )

  # 按文件名排除（例如 main.cpp 留给 add_executable）
  if(_SKV_EXCLUDE)
    set(_filtered "")
    foreach(_f IN LISTS ${out_var})
      get_filename_component(_name "${_f}" NAME)
      list(FIND _SKV_EXCLUDE "${_name}" _idx)
      if(_idx EQUAL -1)
        list(APPEND _filtered "${_f}")
      endif()
    endforeach()
    set(${out_var} "${_filtered}")
  endif()
endmacro()
