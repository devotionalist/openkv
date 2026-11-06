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

// @file compute_node.hpp
// 计算节点：持有 Router + KvProxy，后台 watch Meta 路由（占位）。
//
// 架构位置：compute 进程；无本地持久状态。

#pragma once

#include "opskv/compute/kv_proxy.hpp"
#include "opskv/config.hpp"

#include <atomic>
#include <memory>
#include <thread>

namespace opskv::compute {

class ComputeNode {
 public:
  explicit ComputeNode(NodeConfig cfg);
  ~ComputeNode();

  void start();
  void stop();

  Router& router() { return *router_; }
  KvProxy& proxy() { return *proxy_; }

 private:
  void watch_routing();

  NodeConfig cfg_;
  std::unique_ptr<Router> router_;
  std::unique_ptr<KvProxy> proxy_;
  std::atomic<bool> running_{false};
  std::thread watch_thread_;
  std::string backend_storage_addr_{"127.0.0.1:9100"};
};

}  // namespace opskv::compute
