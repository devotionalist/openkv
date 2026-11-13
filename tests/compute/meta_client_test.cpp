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

// @file meta_client_test.cpp
// parse_route / parse_redirect / TSO 本地缓存 / ActiveTs。

#include "openkv/codec.hpp"
#include "openkv/compute/active_ts.hpp"
#include "openkv/compute/grpc_channel.hpp"
#include "openkv/compute/meta_client.hpp"
#include "openkv/compute/tso.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

using openkv::compute::ActiveTsTracker;
using openkv::compute::parse_redirect;
using openkv::compute::parse_route;
using openkv::compute::TSOracle;

TEST(GrpcUtilTest, ParseRedirect) {
  EXPECT_EQ(parse_redirect("ERR not_leader redirect=127.0.0.1:8500"), "127.0.0.1:8500");
  EXPECT_EQ(parse_redirect("OK"), "");
}

TEST(MetaClientTest, ParseRouteNewFormat) {
  // OK ROUTE ver n n rid:startHex:endHex:epoch:nrep,node@addr*L
  const std::string start = openkv::to_hex("a");
  const std::string end = openkv::to_hex("z");
  std::string line = "OK ROUTE 7 1 1 3:" + start + ":" + end + ":9:1,s0@10.0.0.1:9100*L";
  auto t = parse_route(line);
  EXPECT_EQ(t.version, 7u);
  ASSERT_EQ(t.regions.size(), 1u);
  auto& rr = t.regions.at(3);
  EXPECT_EQ(rr.range.start_key, "a");
  EXPECT_EQ(rr.range.end_key, "z");
  EXPECT_EQ(rr.epoch, 9u);
  ASSERT_EQ(rr.replicas.size(), 1u);
  EXPECT_TRUE(rr.replicas[0].is_leader);
  EXPECT_EQ(rr.replicas[0].address, "10.0.0.1:9100");
}

TEST(MetaClientTest, ParseRouteEmptyFallsBack) {
  auto t = parse_route("OK ROUTE 1 0 0");
  ASSERT_EQ(t.regions.size(), 1u);
  EXPECT_EQ(t.regions.begin()->second.region_id, 0u);
  EXPECT_EQ(t.regions.begin()->second.epoch, 1u);
}

TEST(TsoTest, AllocMonotonicLocal) {
  // 空 meta：低于水位也不会预取，避免打空地址。
  TSOracle o({}, 100, /*low_watermark=*/1);
  o.inject_cache_for_test(1000, 1100);
  EXPECT_EQ(o.alloc(1), 1000u);
  EXPECT_EQ(o.alloc(2), 1001u);
  EXPECT_EQ(o.remaining_for_test(), 97u);
}

TEST(TsoTest, DefaultBatchAndWatermark) {
  TSOracle o({}, 0, 0);
  EXPECT_EQ(o.batch_size(), openkv::compute::kDefaultTsoBatch);
  EXPECT_EQ(o.low_watermark(), openkv::compute::kDefaultTsoLowWatermark);
}

TEST(TsoTest, QueueDrainOrder) {
  TSOracle o({}, 5000, 2000);
  o.inject_cache_for_test(100, 102);
  o.inject_range_for_test(500, 600);
  EXPECT_EQ(o.remaining_for_test(), 102u);  // 2 + 100
  EXPECT_EQ(o.alloc(2), 100u);
  EXPECT_EQ(o.alloc(1), 500u);
  EXPECT_EQ(o.remaining_for_test(), 99u);
}

TEST(TsoTest, AllocAsyncLocalHit) {
  TSOracle o({}, 100, 1);
  o.inject_cache_for_test(2000, 2100);
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  grpc::Status st;
  uint64_t start = 0;
  o.alloc_async(2, [&](grpc::Status s, uint64_t ts) {
    std::lock_guard<std::mutex> lock(mu);
    st = s;
    start = ts;
    done = true;
    cv.notify_one();
  });
  {
    std::unique_lock<std::mutex> lock(mu);
    cv.wait_for(lock, std::chrono::seconds(1), [&] { return done; });
  }
  ASSERT_TRUE(done);
  ASSERT_TRUE(st.ok()) << st.error_message();
  EXPECT_EQ(start, 2000u);
  EXPECT_EQ(o.alloc(1), 2002u);
}

TEST(TsoTest, ConcurrentUnique) {
  TSOracle o({}, 10000, 1);
  o.inject_cache_for_test(1, 10001);
  constexpr int kN = 2000;
  std::vector<uint64_t> got(kN);
  std::vector<std::thread> ths;
  for (int t = 0; t < 8; ++t) {
    ths.emplace_back([&, t] {
      for (int j = 0; j < kN / 8; ++j) {
        got[t * (kN / 8) + j] = o.alloc(1);
      }
    });
  }
  for (auto& th : ths) th.join();
  std::sort(got.begin(), got.end());
  for (size_t i = 1; i < got.size(); ++i) {
    EXPECT_NE(got[i], got[i - 1]);
  }
}

TEST(ActiveTsTest, MinAndExpire) {
  ActiveTsTracker tr(std::chrono::milliseconds(50));
  EXPECT_EQ(tr.min(), std::numeric_limits<uint64_t>::max());
  tr.add(100);
  tr.add(50);
  EXPECT_EQ(tr.min(), 50u);
  tr.remove(50);
  EXPECT_EQ(tr.min(), 100u);
  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  EXPECT_EQ(tr.min(), std::numeric_limits<uint64_t>::max());
}
