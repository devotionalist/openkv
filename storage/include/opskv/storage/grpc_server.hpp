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

#include "opskv/storage/store_service.hpp"
#include "opskv/storage/exec_line_handler.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace opskv::storage {

/// 在 listen_addr 上启动 gRPC（同端口多路复用三类服务）。
class StorageGrpcServer {
 public:
  /// @param line_server 提供则注册 AdminService.ExecLine（复用行协议解析）。
  StorageGrpcServer(StoreFacade& facade, std::string listen_addr, ExecLineHandler* line_server = nullptr);
  ~StorageGrpcServer();

  void start();
  void stop();
  bool running() const { return running_.load(); }

 private:
  StoreFacade& facade_;
  ExecLineHandler* line_server_{nullptr};
  std::string listen_addr_;
  std::string admin_token_;
  std::atomic<bool> running_{false};
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace opskv::storage
