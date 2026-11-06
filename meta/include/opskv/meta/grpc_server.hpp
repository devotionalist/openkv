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
// Meta gRPC：MetaService（PD 风格）。

#pragma once

#include "opskv/meta/meta_node.hpp"

#include <atomic>
#include <memory>
#include <string>

namespace opskv::meta {

class MetaGrpcServer {
 public:
  MetaGrpcServer(MetaNode& meta, std::string listen_addr);
  ~MetaGrpcServer();

  void start();
  void stop();
  bool running() const { return running_.load(); }

 private:
  MetaNode& meta_;
  std::string listen_addr_;
  std::string admin_token_;
  std::atomic<bool> running_{false};
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace opskv::meta
