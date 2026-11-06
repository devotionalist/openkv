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

// @file kv_proxy.cpp
// KvProxy：按 key 路由到 Leader，not_leader 时轮询副本地址重试。

#include "opskv/compute/kv_proxy.hpp"

#include "opskv/logging.hpp"

namespace opskv::compute {

KvProxy::KvProxy(Router& router, StorageGetFn get_fn, StoragePutFn put_fn)
    : router_(router), get_fn_(std::move(get_fn)), put_fn_(std::move(put_fn)) {}

ProxyGetResult KvProxy::get(std::string_view key, bool linearizable) {
  ProxyGetResult r;
  auto addr = router_.leader_addr_for_key(key);
  if (!addr) {
    r.error = "no_route";
    return r;
  }
  uint32_t shard = router_.shard_of(key);
  for (int i = 0; i < max_redirects_; ++i) {
    r = get_fn_(*addr, shard, key, linearizable);
    if (r.ok || r.error != "not_leader") {
      return r;
    }
    // error 中可能携带 redirect；简化：刷新后重试全部副本
    auto replicas = router_.replica_addrs(shard);
    if (static_cast<size_t>(i) + 1 < replicas.size()) {
      addr = replicas[i + 1];
    } else {
      break;
    }
  }
  return r;
}

ProxyPutResult KvProxy::put(std::string_view key, std::string_view value) {
  ProxyPutResult r;
  auto addr = router_.leader_addr_for_key(key);
  if (!addr) {
    r.error = "no_route";
    return r;
  }
  uint32_t shard = router_.shard_of(key);
  for (int i = 0; i < max_redirects_; ++i) {
    r = put_fn_(*addr, shard, key, value);
    if (r.ok || r.error != "not_leader") {
      return r;
    }
    auto replicas = router_.replica_addrs(shard);
    if (static_cast<size_t>(i) + 1 < replicas.size()) {
      addr = replicas[i + 1];
    } else {
      break;
    }
  }
  return r;
}

ProxyPutResult KvProxy::del(std::string_view key) {
  // 生产：独立 Delete RPC；此处用 put_fn 占位，真实协议见 proto Delete
  static const char marker[] = {'\0', '\x01', 'D', 'E', 'L'};
  return put(key, std::string_view(marker, sizeof(marker)));
}

}  // namespace opskv::compute
