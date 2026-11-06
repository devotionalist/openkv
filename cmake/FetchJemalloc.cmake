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

# Bundled jemalloc（FetchContent）：进程级内存分配器，导出 jemalloc::jemalloc。
# 上游：https://github.com/jemalloc/jemalloc
#
# 与 ASan/TSan 互斥。macOS 须用系统 ar/ranlib/libtool，避免 GNU ar 的 `/` 符号表
# 导致 Apple ld：archive member '/' not a mach-o file。

include_guard(GLOBAL)
include(FetchContent)

set(OPSKV_JEMALLOC_VERSION "5.3.0" CACHE STRING
    "Bundled jemalloc version (GitHub release tag)")

option(OPSKV_USE_JEMALLOC
  "FetchContent 引入 jemalloc 作为进程分配器" ON)

if(OPSKV_ENABLE_ASAN OR OPSKV_ENABLE_TSAN)
  if(OPSKV_USE_JEMALLOC)
    message(STATUS "OpsKV: jemalloc disabled (incompatible with ASan/TSan)")
  endif()
  set(OPSKV_USE_JEMALLOC OFF CACHE BOOL
    "FetchContent 引入 jemalloc 作为进程分配器" FORCE)
endif()

if(NOT OPSKV_USE_JEMALLOC)
  message(STATUS "OpsKV: OPSKV_USE_JEMALLOC=OFF")
  return()
endif()

set(_jemalloc_url
  "https://github.com/jemalloc/jemalloc/releases/download/${OPSKV_JEMALLOC_VERSION}/jemalloc-${OPSKV_JEMALLOC_VERSION}.tar.bz2")

FetchContent_Declare(
  jemalloc
  URL ${_jemalloc_url}
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

if(POLICY CMP0169)
  cmake_policy(SET CMP0169 OLD)
endif()

FetchContent_GetProperties(jemalloc)
if(NOT jemalloc_POPULATED)
  message(STATUS "Fetching jemalloc ${OPSKV_JEMALLOC_VERSION} …")
  FetchContent_Populate(jemalloc)

  set(_je_cc "${CMAKE_C_COMPILER}")
  if(NOT _je_cc)
    set(_je_cc "cc")
  endif()

  set(_je_cfg_env "")
  if(APPLE)
    # 强制 Apple 工具链打出 BSD ar，避免 ld 拒收
    set(_je_cfg_env
      "AR=/usr/bin/ar"
      "RANLIB=/usr/bin/ranlib"
      "NM=/usr/bin/nm"
    )
  endif()

  execute_process(
    COMMAND ${CMAKE_COMMAND} -E env ${_je_cfg_env}
      ${jemalloc_SOURCE_DIR}/configure
        "CC=${_je_cc}"
        --disable-shared
        --enable-static
        --disable-libdl
        --with-jemalloc-prefix=
        --disable-cxx
    WORKING_DIRECTORY ${jemalloc_BINARY_DIR}
    RESULT_VARIABLE _je_cfg
    OUTPUT_VARIABLE _je_cfg_out
    ERROR_VARIABLE _je_cfg_err
  )
  if(NOT _je_cfg EQUAL 0)
    message(FATAL_ERROR
      "jemalloc configure failed (${_je_cfg}):\n${_je_cfg_out}\n${_je_cfg_err}")
  endif()
endif()

find_package(Threads REQUIRED)
find_program(OPSKV_JEMALLOC_MAKE NAMES gmake make REQUIRED)

# 最终给 Apple/通用链接用的库名
set(_je_link_lib "${jemalloc_BINARY_DIR}/lib/libjemalloc_link.a")

if(APPLE)
  add_custom_target(opskv_build_jemalloc
    COMMAND ${OPSKV_JEMALLOC_MAKE} build_lib_static
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${jemalloc_BINARY_DIR}/lib/_je_objs
    COMMAND ${CMAKE_COMMAND} -E make_directory ${jemalloc_BINARY_DIR}/lib/_je_objs
    # 归档含 /、//、__.SYMDEF 等非 .o 成员；ar -x 会对其刷屏，抽完后删掉再 libtool
    COMMAND /bin/sh -c "cd '${jemalloc_BINARY_DIR}/lib/_je_objs' && /usr/bin/ar -x '../libjemalloc_pic.a' 2>/dev/null; /usr/bin/find . -type f ! -name '*.o' -delete && /usr/bin/libtool -static -o '${_je_link_lib}' *.o"
    BYPRODUCTS ${_je_link_lib}
    COMMENT "Building bundled jemalloc ${OPSKV_JEMALLOC_VERSION} (Apple libtool)"
    VERBATIM
    WORKING_DIRECTORY ${jemalloc_BINARY_DIR}
  )
else()
  set(_je_link_lib "${jemalloc_BINARY_DIR}/lib/libjemalloc.a")
  add_custom_target(opskv_build_jemalloc
    COMMAND ${OPSKV_JEMALLOC_MAKE} build_lib_static
    WORKING_DIRECTORY ${jemalloc_BINARY_DIR}
    BYPRODUCTS ${_je_link_lib}
    COMMENT "Building bundled jemalloc ${OPSKV_JEMALLOC_VERSION}"
    VERBATIM
  )
endif()

add_library(jemalloc_static STATIC IMPORTED GLOBAL)
set_target_properties(jemalloc_static PROPERTIES
  IMPORTED_LOCATION ${_je_link_lib}
)
add_dependencies(jemalloc_static opskv_build_jemalloc)

add_library(jemalloc_iface INTERFACE)
add_library(jemalloc::jemalloc ALIAS jemalloc_iface)
target_include_directories(jemalloc_iface INTERFACE
  $<BUILD_INTERFACE:${jemalloc_BINARY_DIR}/include>
  $<BUILD_INTERFACE:${jemalloc_SOURCE_DIR}/include>
)
target_link_libraries(jemalloc_iface INTERFACE jemalloc_static Threads::Threads)
target_compile_definitions(jemalloc_iface INTERFACE OPSKV_HAS_JEMALLOC=1)

if(UNIX AND NOT APPLE)
  target_link_libraries(jemalloc_iface INTERFACE m)
endif()

function(opskv_link_jemalloc target)
  if(NOT TARGET jemalloc::jemalloc)
    return()
  endif()
  if(NOT TARGET ${target})
    message(FATAL_ERROR "opskv_link_jemalloc: unknown target '${target}'")
  endif()
  target_link_libraries(${target} PRIVATE jemalloc::jemalloc)
  add_dependencies(${target} opskv_build_jemalloc)
endfunction()

message(STATUS
  "jemalloc ${OPSKV_JEMALLOC_VERSION} enabled (FetchContent → jemalloc::jemalloc)")
