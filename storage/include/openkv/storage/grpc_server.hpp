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
// Storage gRPC：StoreService + RaftService + AdminService。

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "openkv/storage/store_service.hpp"

namespace openkv::storage {

/* 在 listen_addr 上启动 gRPC（同端口多路复用三类服务）。 */
class StorageGrpcServer {
 public:
  /**
   * 构造未启动的服务端；真正监听在 start()。
   * @param facade 业务门面（Propose/Get/Admin 等）
   * @param listen_addr 监听地址，如 0.0.0.0:20170
   */
  StorageGrpcServer(StoreFacade& facade, std::string listen_addr);
  /**
   * 析构时若仍 running 则 stop。
   */
  ~StorageGrpcServer();

  /**
   * 拉起 gRPC server 与完成队列线程；重复调用无害。
   */
  void start();
  /**
   * 停服并 join 后台线程；析构前应调用。
   */
  void stop();
  /**
   * 是否已 start 且未 stop。
   * @return true 表示对外可接请求
   */
  bool running() const { return running_.load(); }

 private:
  StoreFacade& facade_;               // 业务门面（Propose/Get/Admin）
  std::string listen_addr_;           // 监听地址
  std::string admin_token_;           // Admin RPC 鉴权 token（空=不校验）
  std::atomic<bool> running_{false};  // start/stop 生命周期
  struct Impl;                        // pimpl：gRPC Server/CQ 细节
  std::unique_ptr<Impl> impl_;        // 实现体；stop 后可空
};

}  // namespace openkv::storage
