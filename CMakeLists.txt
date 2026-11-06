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
# OpsKV — 存算分离分布式 KV 存储（根 CMakeLists）
# -----------------------------------------------------------------------------
# 模块关系：
#   common → raft → storage
#   common → compute / meta
#   tests 依赖以上全部静态库 + GoogleTest
#
# 常用配置示例：
#   cmake -B build \
#     -DOPSKV_USE_SEASTAR=OFF \
#     -DOPSKV_USE_JEMALLOC=ON \
#     -DOPSKV_ENABLE_ASAN=ON
#   # gRPC 强制开启（系统包优先，否则 FetchContent）；RocksDB/jemalloc 同理
#   cmake --build build -j && ctest --test-dir build --output-on-failure
# =============================================================================

cmake_minimum_required(VERSION 3.20)
project(opskv VERSION 0.1.0 LANGUAGES CXX)

# ---- 语言标准与 IDE 支持 ----------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# 生成 compile_commands.json，供 clangd / Cursor IntelliSense 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# IDE/clangd 默认在「源码根目录」查找 compile_commands.json，
# 而 CMake 默认只写到 build/，因此创建软链到源码根。
if(CMAKE_EXPORT_COMPILE_COMMANDS)
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${CMAKE_BINARY_DIR}/compile_commands.json
            ${CMAKE_SOURCE_DIR}/compile_commands.json
  )
endif()

# ---- 功能开关 --------------------------------------------------------------
option(OPSKV_BUILD_TESTS "是否编译单元测试（GoogleTest）" ON)
option(OPSKV_USE_SEASTAR
  "FetchContent 引入 scylladb/seastar（cmake/FetchSeastar.cmake）" OFF)
# gRPC 为硬依赖；不再提供 OFF / 行协议双栈。旧 cache 的 OFF 自动抬升为 ON。
if(DEFINED CACHE{OPSKV_USE_GRPC} AND NOT OPSKV_USE_GRPC)
  message(WARNING
    "OPSKV_USE_GRPC=OFF is removed — forcing ON (gRPC-only). "
    "Clear build/ or FORCE_CMAKE=1 if configure still fails.")
endif()
set(OPSKV_USE_GRPC ON CACHE BOOL "gRPC/Protobuf required (mandatory)" FORCE)
# RocksDB / jemalloc：FetchContent 随工程编译（见 cmake/Fetch*.cmake）。

find_package(Threads REQUIRED)

# 将 cmake/ 加入模块搜索路径，以便 include(CollectSources) / FetchRocksDB 等
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CollectSources)   # skv_collect_sources：GLOB 收集 .cpp
include(Sanitizers)       # ASan / UBSan / TSan（须先于 jemalloc：互斥）
include(FetchJemalloc)    # 默认 ON；导出 jemalloc::jemalloc / opskv_link_jemalloc
include(FetchSpdlog)      # 日志
include(FetchCLI11)       # skv CLI
include(FetchToml11)      # .toml 配置
include(FetchHttplib)     # /metrics HTTP
include(FetchRocksDB)     # bundled static RocksDB (default v9.10.0)

# ---- Protobuf / gRPC 代码生成（强制；无系统包则 FetchContent）--------------
include(FetchGrpc)  # system 优先，否则拉 grpc + protobuf
set(PROTO_ROOT ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})
set(PROTO_FILES
  ${PROTO_ROOT}/kv/v1/kv.proto
  ${PROTO_ROOT}/storage/v1/storage.proto
  ${PROTO_ROOT}/meta/v1/meta.proto
)
set(PROTO_SRCS)
foreach(proto ${PROTO_FILES})
  get_filename_component(proto_name ${proto} NAME_WE)
  get_filename_component(proto_dir ${proto} DIRECTORY)
  file(RELATIVE_PATH proto_rel ${PROTO_ROOT} ${proto_dir})
  set(out_dir ${PROTO_GEN_DIR}/${proto_rel})
  file(MAKE_DIRECTORY ${out_dir})
  set(pb_cc ${out_dir}/${proto_name}.pb.cc)
  set(grpc_cc ${out_dir}/${proto_name}.grpc.pb.cc)
  # 调用 protoc 生成 *.pb.cc / *.grpc.pb.cc
  add_custom_command(
    OUTPUT ${pb_cc} ${out_dir}/${proto_name}.pb.h ${grpc_cc} ${out_dir}/${proto_name}.grpc.pb.h
    COMMAND protobuf::protoc
      --proto_path=${PROTO_ROOT}
      --cpp_out=${PROTO_GEN_DIR}
      --grpc_out=${PROTO_GEN_DIR}
      --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
      ${proto}
    DEPENDS ${proto}
    COMMENT "生成 gRPC/Protobuf: ${proto_rel}/${proto_name}"
  )
  list(APPEND PROTO_SRCS ${pb_cc} ${grpc_cc})
endforeach()
add_library(opskv_proto ${PROTO_SRCS})
target_include_directories(opskv_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(opskv_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_compile_definitions(opskv_proto PUBLIC OPSKV_USE_GRPC=1)
set(OPSKV_HAS_PROTO_LIB TRUE)

# ---- 可选第三方：ScyllaDB Seastar（FetchContent，同 RocksDB）---------------
# 源码：https://github.com/scylladb/seastar → cmake/FetchSeastar.cmake
# OFF 时 Meta/Storage/Raft 走 common/net POSIX（macOS / 无系统依赖时默认）。
if(OPSKV_USE_SEASTAR)
  if(APPLE)
    message(WARNING
      "OPSKV_USE_SEASTAR=ON on macOS: scylladb/seastar 以 Linux 为主，"
      "配置/编译可能失败；本地开发请保持 OFF。")
  endif()
  include(FetchSeastar)  # 产出 Seastar::seastar
  message(STATUS "OpsKV: OPSKV_USE_SEASTAR=ON (bundled scylladb/seastar)")
else()
  message(STATUS "OpsKV: OPSKV_USE_SEASTAR=OFF (POSIX thread-per-conn)")
endif()

# ---- 业务子目录（顺序：被依赖方在前） --------------------------------------
add_subdirectory(common)   # 哈希、配置、日志
add_subdirectory(raft)     # Multi-Raft 状态机
add_subdirectory(storage)  # RocksDB/内存引擎 + Storage 节点
add_subdirectory(compute)  # 无状态 Proxy / 路由
add_subdirectory(meta)     # 元数据与在线扩容编排
add_subdirectory(tools)    # skv 运维 CLI（C++）

# ---- 单元测试 --------------------------------------------------------------
if(OPSKV_BUILD_TESTS)
  enable_testing()
  include(FetchGoogleTest)  # FetchContent 拉取 googletest v1.15.2
  add_subdirectory(tests)
endif()
