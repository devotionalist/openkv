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

# Bundled gRPC + Protobuf（系统未安装 gRPC 时 FetchContent）。
# 导出：protobuf::libprotobuf / protobuf::protoc / gRPC::grpc++ / gRPC::grpc_cpp_plugin
#
# 加速策略（避免默认递归拉 opentelemetry/vcpkg 等子模块卡死）：
# - 优先 find_package 系统/Homebrew 包
# - FetchContent 时：浅克隆 + 不递归子模块；absl/protobuf/cares/re2/zlib/ssl 尽量用 package

include(FetchContent)

set(OPSKV_GRPC_GIT_TAG "v1.67.1" CACHE STRING "Bundled gRPC git tag (FetchContent)")
set(OPSKV_GRPC_GIT_REPOSITORY "https://github.com/grpc/grpc.git" CACHE STRING
  "Bundled gRPC git URL（可换成镜像，如 https://mirror.ghproxy.com/https://github.com/grpc/grpc.git）")

# Homebrew 前缀进 CMAKE_PREFIX_PATH，便于找 Protobuf/absl/re2/c-ares
if(APPLE)
  execute_process(
    COMMAND brew --prefix
    OUTPUT_VARIABLE _skv_brew_prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
  if(_skv_brew_prefix)
    list(PREPEND CMAKE_PREFIX_PATH
      "${_skv_brew_prefix}"
      "${_skv_brew_prefix}/opt/protobuf"
      "${_skv_brew_prefix}/opt/abseil"
      "${_skv_brew_prefix}/opt/re2"
      "${_skv_brew_prefix}/opt/c-ares"
      "${_skv_brew_prefix}/opt/openssl@3"
      "${_skv_brew_prefix}/opt/openssl"
    )
  endif()
endif()

set(_skv_grpc_ok FALSE)
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf QUIET)
endif()
find_package(gRPC CONFIG QUIET)
if(Protobuf_FOUND AND gRPC_FOUND AND TARGET gRPC::grpc++ AND TARGET gRPC::grpc_cpp_plugin)
  set(_skv_grpc_ok TRUE)
  message(STATUS "OpsKV: using system Protobuf + gRPC")
endif()

if(NOT _skv_grpc_ok)
  message(STATUS
    "OpsKV: system gRPC not found — FetchContent ${OPSKV_GRPC_GIT_TAG} "
    "(shallow, no recursive submodules; prefer Homebrew absl/protobuf/…)")

  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

  # 尽量用系统/Homebrew 依赖，避免拉 third_party 子模块
  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_CODEGEN ON CACHE BOOL "" FORCE)
  set(gRPC_DOWNLOAD_ARCHIVES OFF CACHE BOOL "" FORCE)

  find_package(ZLIB QUIET)
  if(ZLIB_FOUND)
    set(gRPC_ZLIB_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  find_package(OpenSSL QUIET)
  if(OpenSSL_FOUND)
    set(gRPC_SSL_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  find_package(absl CONFIG QUIET)
  if(absl_FOUND OR TARGET absl::base)
    set(gRPC_ABSL_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  find_package(c-ares CONFIG QUIET)
  if(NOT c-ares_FOUND)
    find_package(CARES QUIET)
  endif()
  if(c-ares_FOUND OR CARES_FOUND OR TARGET c-ares::cares)
    set(gRPC_CARES_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  find_package(re2 CONFIG QUIET)
  if(re2_FOUND OR TARGET re2::re2)
    set(gRPC_RE2_PROVIDER "package" CACHE STRING "" FORCE)
  endif()
  if(Protobuf_FOUND)
    set(gRPC_PROTOBUF_PROVIDER "package" CACHE STRING "" FORCE)
    set(gRPC_PROTOBUF_PACKAGE_TYPE "CONFIG" CACHE STRING "" FORCE)
  endif()

  # 不递归子模块：否则会卡在 opentelemetry → vcpkg / nlohmann-json 等
  # 已设 package provider 时不需要完整 third_party 树
  FetchContent_Declare(
    grpc
    GIT_REPOSITORY ${OPSKV_GRPC_GIT_REPOSITORY}
    GIT_TAG        ${OPSKV_GRPC_GIT_TAG}
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   TRUE
    GIT_SUBMODULES ""
    GIT_SUBMODULES_RECURSE FALSE
  )
  FetchContent_MakeAvailable(grpc)

  if(TARGET libprotobuf AND NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf ALIAS libprotobuf)
  endif()
  if(TARGET protoc AND NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc ALIAS protoc)
  endif()
  if(TARGET grpc++ AND NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ ALIAS grpc++)
  endif()
  if(TARGET grpc_cpp_plugin AND NOT TARGET gRPC::grpc_cpp_plugin)
    add_executable(gRPC::grpc_cpp_plugin ALIAS grpc_cpp_plugin)
  endif()

  if(NOT TARGET protobuf::libprotobuf OR NOT TARGET protobuf::protoc OR
     NOT TARGET gRPC::grpc++ OR NOT TARGET gRPC::grpc_cpp_plugin)
    message(FATAL_ERROR
      "Bundled gRPC did not export protobuf::*/gRPC::* targets.\n"
      "  OPSKV_GRPC_GIT_TAG=${OPSKV_GRPC_GIT_TAG}\n"
      "  Prefer: brew install grpc protobuf abseil c-ares re2\n"
      "  Or mirror: -DOPSKV_GRPC_GIT_REPOSITORY=https://mirror.ghproxy.com/https://github.com/grpc/grpc.git")
  endif()
  message(STATUS "OpsKV: bundled gRPC ${OPSKV_GRPC_GIT_TAG} ready (package providers where available)")
endif()
