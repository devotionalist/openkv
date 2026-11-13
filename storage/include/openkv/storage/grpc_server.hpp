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

#include "openkv/storage/store_service.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace openkv::storage {

/// 在 listen_addr 上启动 gRPC（同端口多路复用三类服务）。
class StorageGrpcServer {
 public:
  StorageGrpcServer(StoreFacade& facade, std::string listen_addr);
  ~StorageGrpcServer();

  void start();
  void stop();
  bool running() const { return running_.load(); }

 private:
  StoreFacade& facade_; // 存储 facade
  std::string listen_addr_; // 监听地址
  std::string admin_token_; // 管理员 token
  std::atomic<bool> running_{false}; // 运行状态
  struct Impl; // 实现
  std::unique_ptr<Impl> impl_; // 实现
}; // 存储 gRPC 服务器

}  // namespace openkv::storage
