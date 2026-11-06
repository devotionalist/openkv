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

// @file hash_test.cpp
// 测试 fnv1a64 稳定性、空 key、shard_of 范围与 shard_count=0 行为。

#include "opskv/hash.hpp"

#include <gtest/gtest.h>

#include <set>
#include <string>

using opskv::fnv1a64;
using opskv::shard_of;

TEST(HashTest, Fnv1aStable) {
  EXPECT_EQ(fnv1a64("hello"), fnv1a64("hello"));
  EXPECT_NE(fnv1a64("hello"), fnv1a64("world"));
  EXPECT_NE(fnv1a64("a"), fnv1a64("b"));
}

TEST(HashTest, EmptyKey) {
  EXPECT_EQ(fnv1a64(""), 14695981039346656037ULL);
}

TEST(HashTest, ShardOfRange) {
  constexpr uint32_t kShards = 8;
  for (int i = 0; i < 100; ++i) {
    auto s = shard_of("key-" + std::to_string(i), kShards);
    EXPECT_LT(s, kShards);
  }
}

TEST(HashTest, ShardOfZeroCount) {
  EXPECT_EQ(shard_of("any", 0), 0u);
}

TEST(HashTest, ShardDistributionNotCollapsed) {
  std::set<uint32_t> seen;
  for (int i = 0; i < 200; ++i) {
    seen.insert(shard_of("user:" + std::to_string(i), 8));
  }
  EXPECT_GT(seen.size(), 4u);
}
