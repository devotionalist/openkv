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

#include "opskv/storage/inflight_gate.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

using opskv::storage::classify_op;
using opskv::storage::InflightGate;
using opskv::storage::InflightGuard;
using opskv::storage::ReqClass;

TEST(InflightGateTest, Classify) {
  EXPECT_EQ(classify_op("PUT"), ReqClass::kClient);
  EXPECT_EQ(classify_op("RAFT_AE"), ReqClass::kRaft);
  EXPECT_EQ(classify_op("ADMIN_SPLIT"), ReqClass::kAdmin);
  EXPECT_EQ(classify_op("STATS"), ReqClass::kBypass);
}

TEST(InflightGateTest, IsolateClasses) {
  InflightGate g;
  g.set_max(ReqClass::kClient, 1);
  g.set_max(ReqClass::kRaft, 2);
  g.set_max(ReqClass::kAdmin, 1);

  InflightGuard c1(g, ReqClass::kClient);
  ASSERT_TRUE(c1);
  InflightGuard c2(g, ReqClass::kClient);
  EXPECT_FALSE(c2);
  EXPECT_EQ(g.rejected(ReqClass::kClient), 1u);

  // Client 满时 Raft 仍可进入
  InflightGuard r1(g, ReqClass::kRaft);
  InflightGuard r2(g, ReqClass::kRaft);
  ASSERT_TRUE(r1);
  ASSERT_TRUE(r2);
  InflightGuard r3(g, ReqClass::kRaft);
  EXPECT_FALSE(r3);

  InflightGuard a1(g, ReqClass::kAdmin);
  ASSERT_TRUE(a1);
}

TEST(InflightGateTest, ConcurrentClientCap) {
  InflightGate g;
  g.set_max(ReqClass::kClient, 4);
  std::atomic<uint32_t> peak{0};
  std::atomic<int> busy{0};
  std::vector<std::thread> ths;
  for (int i = 0; i < 64; ++i) {
    ths.emplace_back([&] {
      InflightGuard guard(g, ReqClass::kClient);
      if (!guard) {
        busy.fetch_add(1);
        return;
      }
      uint32_t cur = g.inflight(ReqClass::kClient);
      uint32_t prev = peak.load();
      while (cur > prev && !peak.compare_exchange_weak(prev, cur)) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    });
  }
  for (auto& t : ths) t.join();
  EXPECT_LE(peak.load(), 4u);
  EXPECT_GT(busy.load(), 0);
  EXPECT_EQ(g.inflight(ReqClass::kClient), 0u);
}
