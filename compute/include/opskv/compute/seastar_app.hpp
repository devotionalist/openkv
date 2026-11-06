// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// @file seastar_app.hpp
// Seastar 集成说明与可选头文件（编译开关 OPSKV_USE_SEASTAR）。
//
// 架构位置：未来高性能 compute 网关；当前默认走 std::thread + 占位 transport。

#pragma once

// Seastar 集成入口说明（需 -DOPSKV_USE_SEASTAR=ON）
//
// 推荐结构：
//   1. app_template 启动，每核一个 shard 本地 Router 缓存
//   2. 用 seastar::alien 或专用核心跑 gRPC 完成队列，避免阻塞 reactor
//   3. 路由表更新通过 smp::invoke_on_all 广播到各核
//
// 示例伪代码：
//
//   int main(int argc, char** argv) {
//     seastar::app_template app;
//     return app.run(argc, argv, [] {
//       return seastar::async([] {
//         opskv::compute::ComputeNode node(load_node_config("conf/compute.conf"));
//         node.start();
//         // co_await start_grpc_gateway(node);
//         seastar::engine().exit(0);
//       });
//     });
//   }

#if defined(OPSKV_HAS_SEASTAR) && OPSKV_HAS_SEASTAR
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#endif
