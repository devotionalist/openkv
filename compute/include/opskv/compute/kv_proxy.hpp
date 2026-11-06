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

// @file kv_proxy.hpp
// 计算层 KV 代理：路由 + 存储 RPC 回调 + Leader 重定向重试。
//
// 架构位置：ComputeNode 对外业务入口；StorageGetFn/PutFn 由 gRPC 或 TCP 客户端注入。

#pragma once

#include "opskv/compute/router.hpp"

#include <functional>
#include <string>
#include <string_view>

namespace opskv::compute {

struct ProxyGetResult {
  bool ok{false};
  bool found{false};
  std::string value;
  uint64_t revision{0};
  std::string error;
};

struct ProxyPutResult {
  bool ok{false};
  uint64_t revision{0};
  std::string error;
};

// 向 Storage 发起内部 RPC 的回调（生产由 gRPC stub 实现）
using StorageGetFn = std::function<ProxyGetResult(const std::string& addr, uint32_t shard,
                                                  std::string_view key, bool linearizable)>;
using StoragePutFn = std::function<ProxyPutResult(const std::string& addr, uint32_t shard,
                                                  std::string_view key, std::string_view value)>;

// 计算层无状态代理：路由 + 重试 + 重定向
class KvProxy {
 public:
  KvProxy(Router& router, StorageGetFn get_fn, StoragePutFn put_fn);

  /// linearizable 传给存储层；not_leader 时尝试副本列表重试。
  ProxyGetResult get(std::string_view key, bool linearizable = true);
  ProxyPutResult put(std::string_view key, std::string_view value);
  ProxyPutResult del(std::string_view key);

 private:
  Router& router_;
  StorageGetFn get_fn_;
  StoragePutFn put_fn_;
  int max_redirects_{3};
};

}  // namespace opskv::compute
