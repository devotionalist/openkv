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

// @file kv_proxy_test.cpp
// 测试 KvProxy 在 mock transport 下的 put/get 与 not_leader 重试路径。

#include "opskv/compute/kv_proxy.hpp"
#include "opskv/compute/router.hpp"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

using opskv::compute::KvProxy;
using opskv::compute::ProxyGetResult;
using opskv::compute::ProxyPutResult;
using opskv::compute::Router;

TEST(KvProxyTest, PutGetViaTransport) {
  Router router;
  router.bootstrap_single("127.0.0.1:9100", 8);

  std::unordered_map<std::string, std::string> store;
  auto get_fn = [&](const std::string&, uint32_t, std::string_view key, bool) {
    ProxyGetResult r;
    r.ok = true;
    auto it = store.find(std::string(key));
    if (it != store.end()) {
      r.found = true;
      r.value = it->second;
      r.revision = 1;
    }
    return r;
  };
  auto put_fn = [&](const std::string&, uint32_t, std::string_view key, std::string_view value) {
    store[std::string(key)] = std::string(value);
    ProxyPutResult r;
    r.ok = true;
    r.revision = 1;
    return r;
  };

  KvProxy proxy(router, get_fn, put_fn);
  auto pr = proxy.put("k", "v");
  EXPECT_TRUE(pr.ok);
  auto gr = proxy.get("k");
  EXPECT_TRUE(gr.ok);
  EXPECT_TRUE(gr.found);
  EXPECT_EQ(gr.value, "v");
}

TEST(KvProxyTest, NoRoute) {
  Router router;  // 空路由
  KvProxy proxy(
      router,
      [](auto&&...) {
        return ProxyGetResult{};
      },
      [](auto&&...) {
        return ProxyPutResult{};
      });
  auto pr = proxy.put("k", "v");
  EXPECT_FALSE(pr.ok);
  EXPECT_EQ(pr.error, "no_route");
}

TEST(KvProxyTest, RedirectRetry) {
  Router router;
  router.bootstrap_single("127.0.0.1:9100", 1);
  // 补充第二副本，便于 redirect
  opskv::compute::RoutingTable t;
  t.version = 2;
  t.shard_count = 1;
  opskv::compute::RegionRoute sr;
  sr.region_id = 0;
  sr.replicas.push_back({"s0", "127.0.0.1:9100", true});
  sr.replicas.push_back({"s1", "127.0.0.1:9101", false});
  t.regions[0] = sr;
  router.update(t);

  int calls = 0;
  auto put_fn = [&](const std::string& addr, uint32_t, std::string_view, std::string_view) {
    ++calls;
    ProxyPutResult r;
    if (addr == "127.0.0.1:9100") {
      r.error = "not_leader";
      return r;
    }
    r.ok = true;
    r.revision = 7;
    return r;
  };

  KvProxy proxy(
      router, [](auto&&...) { return ProxyGetResult{.ok = true}; }, put_fn);
  auto pr = proxy.put("k", "v");
  EXPECT_TRUE(pr.ok);
  EXPECT_EQ(pr.revision, 7u);
  EXPECT_GE(calls, 2);
}
