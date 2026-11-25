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

// @file grpc_server.hpp
// Meta gRPC：对外暴露 MetaService（路由 / TSO / 运维 / 节点生命周期）。

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "openkv/meta/meta_node.hpp"

namespace openkv::meta {

/* MetaService gRPC 监听器；把请求转到 MetaNode。 */
class MetaGrpcServer {
 public:
  /**
   * 绑定 MetaNode 与监听地址；尚未 start。
   * @param meta 非拥有的 MetaNode 引用
   * @param listen_addr 监听 host:port
   */
  MetaGrpcServer(MetaNode& meta, std::string listen_addr);
  ~MetaGrpcServer();

  /**
   * 启动 gRPC Server（阻塞建听后返回）。
   */
  void start();
  /**
   * 优雅关机并 join；幂等。
   */
  void stop();
  /**
   * 是否已 start 且未 stop。
   * @return 运行中为 true
   */
  bool running() const { return running_.load(); }

 private:
  MetaNode& meta_;                    // 非拥有：业务入口
  std::string listen_addr_;           // 监听地址
  std::string admin_token_;           // SKV_ADMIN_TOKEN 缓存
  std::atomic<bool> running_{false};  // 生命周期
  struct Impl;                        // Server / service 实现
  std::unique_ptr<Impl> impl_;        // Pimpl
};

}  // namespace openkv::meta
