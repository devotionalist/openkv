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

// @file router_test.cpp
// Range 路由：bootstrap、版本守卫、定位。

#include "opskv/compute/router.hpp"

#include <gtest/gtest.h>

using opskv::compute::RegionRoute;
using opskv::compute::Router;
using opskv::compute::RoutingTable;

TEST(RouterTest, BootstrapSingle) {
  Router r;
  r.bootstrap_single("127.0.0.1:9100", 8);
  EXPECT_EQ(r.version(), 1u);
  EXPECT_EQ(r.shard_of("hello"), 0u);
  auto addr = r.leader_addr_for_key("hello");
  ASSERT_TRUE(addr);
  EXPECT_EQ(*addr, "127.0.0.1:9100");
}

TEST(RouterTest, UpdateIgnoresStaleVersion) {
  Router r;
  r.bootstrap_single("127.0.0.1:9100", 4);
  RoutingTable newer;
  newer.version = 5;
  newer.shard_count = 1;
  RegionRoute sr;
  sr.region_id = 0;
  sr.replicas.push_back({"s1", "127.0.0.1:9101", true});
  newer.regions[0] = sr;
  r.update(newer);
  EXPECT_EQ(r.version(), 5u);
  EXPECT_EQ(*r.leader_addr(0), "127.0.0.1:9101");

  RoutingTable stale;
  stale.version = 2;
  RegionRoute sr2;
  sr2.region_id = 0;
  sr2.replicas.push_back({"s0", "127.0.0.1:9100", true});
  stale.regions[0] = sr2;
  r.update(stale);
  EXPECT_EQ(r.version(), 5u);
  EXPECT_EQ(*r.leader_addr(0), "127.0.0.1:9101");
}

TEST(RouterTest, RangeLocateAfterSplit) {
  Router r;
  RoutingTable t;
  t.version = 1;
  RegionRoute left;
  left.region_id = 0;
  left.range.end_key = "m";
  left.replicas.push_back({"s0", "a:1", true});
  RegionRoute right;
  right.region_id = 1;
  right.range.start_key = "m";
  right.replicas.push_back({"s1", "b:2", true});
  t.regions[0] = left;
  t.regions[1] = right;
  t.shard_count = 2;
  r.update(t);
  EXPECT_EQ(*r.region_of("a"), 0u);
  EXPECT_EQ(*r.region_of("m"), 1u);
  EXPECT_EQ(*r.leader_addr_for_key("z"), "b:2");
}

TEST(RouterTest, ReplicaAddrsAndFallback) {
  Router r;
  RoutingTable t;
  t.version = 1;
  t.shard_count = 1;
  RegionRoute sr;
  sr.region_id = 0;
  sr.replicas.push_back({"s0", "a:1", false});
  sr.replicas.push_back({"s1", "b:2", false});
  t.regions[0] = sr;
  r.update(t);
  auto leader = r.leader_addr(0);
  ASSERT_TRUE(leader);
  EXPECT_EQ(*leader, "a:1");
  EXPECT_EQ(r.replica_addrs(0).size(), 2u);
}

TEST(RouterTest, MissingShard) {
  Router r;
  EXPECT_FALSE(r.leader_addr(99).has_value());
  EXPECT_TRUE(r.replica_addrs(99).empty());
}
