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

// @file ts_range_queue_test.cpp
// 无锁 TSO 区间环：顺序、丢碎片、并发唯一。

#include "openkv/compute/ts_range_queue.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <thread>
#include <vector>

using openkv::compute::TsRangeQueue;

TEST(TsRangeQueueTest, PushTakeOrder) {
  TsRangeQueue<8> q;
  ASSERT_TRUE(q.push(100, 2));
  ASSERT_TRUE(q.push(500, 100));
  EXPECT_EQ(q.remaining(), 102u);
  uint64_t s = 0;
  ASSERT_TRUE(q.try_take(2, &s));
  EXPECT_EQ(s, 100u);
  ASSERT_TRUE(q.try_take(1, &s));
  EXPECT_EQ(s, 500u);
  EXPECT_EQ(q.remaining(), 99u);
}

TEST(TsRangeQueueTest, DropFragmentThenNextRange) {
  TsRangeQueue<8> q;
  ASSERT_TRUE(q.push(10, 1));   // only 1 left
  ASSERT_TRUE(q.push(100, 5));
  uint64_t s = 0;
  ASSERT_TRUE(q.try_take(2, &s));  // drop [10,11), take from 100
  EXPECT_EQ(s, 100u);
  EXPECT_EQ(q.remaining(), 3u);
}

TEST(TsRangeQueueTest, ConcurrentUnique) {
  TsRangeQueue<16> q;
  ASSERT_TRUE(q.push(1, 4000));
  constexpr int kN = 2000;
  std::vector<uint64_t> got(kN);
  std::vector<std::thread> ths;
  for (int t = 0; t < 8; ++t) {
    ths.emplace_back([&, t] {
      for (int j = 0; j < kN / 8; ++j) {
        uint64_t s = 0;
        ASSERT_TRUE(q.try_take(1, &s));
        got[t * (kN / 8) + j] = s;
      }
    });
  }
  for (auto& th : ths) th.join();
  std::sort(got.begin(), got.end());
  for (size_t i = 1; i < got.size(); ++i) {
    EXPECT_NE(got[i], got[i - 1]);
  }
}
