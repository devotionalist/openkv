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

// @file cluster_state_test.cpp
// Range bootstrap、分裂、Learner、心跳。

#include "opskv/meta/cluster_state.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <thread>

using opskv::meta::ClusterState;
using opskv::meta::NodeInfo;
using opskv::meta::NodeRole;
using opskv::meta::NodeState;
using opskv::meta::Replica;

static std::vector<NodeInfo> three_storages() {
  return {
      {"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0},
      {"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kActive, 0},
      {"s2", NodeRole::kStorage, "127.0.0.1:9102", NodeState::kActive, 0},
  };
}

TEST(ClusterStateTest, AllocTsMonotonic) {
  ClusterState st;
  // alloc_ts 以墙钟 ms 为下界（防 Meta 硬重启 TSO 回退），不假定从 1 起。
  const uint64_t a = st.alloc_ts(1);
  const uint64_t b = st.alloc_ts(3);
  EXPECT_GT(a, 0u);
  EXPECT_EQ(b, a + 1);
  EXPECT_EQ(st.next_ts(), a + 1 + 3);
}

TEST(ClusterStateTest, GcSafePointAdvancesAndRespectsMinStart) {
  ClusterState st;
  st.set_gc_retain_ts(10);
  const uint64_t base = st.alloc_ts(100);  // next_ts = base+100
  EXPECT_EQ(st.gc_safe_ts(), 0u);
  const uint64_t safe1 = st.advance_gc_safe_ts(10);
  EXPECT_EQ(safe1, base + 100 - 10);
  st.report_min_start_ts(50);
  // 已到 safe1，hint 不能回退水位；再 alloc 后仍受 hint 上界约束
  st.alloc_ts(50);  // next = base+150
  EXPECT_EQ(st.advance_gc_safe_ts(10), safe1);  // candidate 被 hint 压到 50 < safe1 → 不前进
  st.report_min_start_ts(base + 1000);
  EXPECT_EQ(st.advance_gc_safe_ts(10), base + 150 - 10);
}

TEST(ClusterStateTest, MinStartAggregatesReporters) {
  ClusterState st;
  st.report_min_start_ts(100, "gw");
  st.report_min_start_ts(40, "storage-0");
  st.report_min_start_ts(std::numeric_limits<uint64_t>::max(), "gw");  // gw 无活跃
  EXPECT_EQ(st.min_start_ts_hint(), 40u);  // 仍受 storage 约束
}

TEST(ClusterStateTest, BootstrapRangeOneRegion) {
  ClusterState st;
  st.bootstrap(4, 3, three_storages());  // shard_count ignored
  auto rt = st.routing_table();
  EXPECT_EQ(rt.version, 1u);
  EXPECT_EQ(rt.shard_count, 1u);
  ASSERT_EQ(rt.regions.size(), 1u);
  EXPECT_EQ(rt.regions[0].region_id, 0u);
  EXPECT_TRUE(rt.regions[0].range.start_key.empty());
  EXPECT_TRUE(rt.regions[0].range.end_key.empty());
  EXPECT_TRUE(rt.regions[0].replicas[0].is_leader);
}

TEST(ClusterStateTest, LocateAndSplit) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  EXPECT_EQ(*st.locate("abc"), 0u);
  auto right = st.split_region(0, "m");
  ASSERT_TRUE(right);
  EXPECT_EQ(*right, 1u);
  EXPECT_EQ(*st.locate("a"), 0u);    // < m → left
  EXPECT_EQ(*st.locate("m"), 1u);    // >= m → right
  EXPECT_EQ(*st.locate("zzz"), 1u);
  auto rt = st.routing_table();
  EXPECT_EQ(rt.regions.size(), 2u);
  EXPECT_GT(rt.version, 1u);
}

TEST(ClusterStateTest, MergeAdjacent) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  ASSERT_TRUE(st.split_region(0, "m"));
  EXPECT_EQ(st.routing_table().regions.size(), 2u);
  EXPECT_TRUE(st.merge_regions(0, 1));
  EXPECT_EQ(st.routing_table().regions.size(), 1u);
  EXPECT_EQ(*st.locate("a"), 0u);
  EXPECT_EQ(*st.locate("z"), 0u);
  EXPECT_FALSE(st.merge_regions(0, 0));
}

TEST(ClusterStateTest, RegisterStaysJoiningUntilActivate) {
  ClusterState st;
  st.bootstrap(2, 3, three_storages());
  NodeInfo n;
  n.node_id = "s3";
  n.address = "127.0.0.1:9103";
  n.role = NodeRole::kStorage;
  EXPECT_EQ(st.register_node(n), "s3");
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);
  EXPECT_TRUE(st.heartbeat("s3", {}));
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);  // 不因心跳自动加入
  EXPECT_TRUE(st.set_node_state("s3", NodeState::kActive));
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kActive);
  // 待加入节点不参与放置
  NodeInfo pending;
  pending.node_id = "s4";
  pending.address = "127.0.0.1:9104";
  pending.role = NodeRole::kStorage;
  st.register_node(pending);
  auto place = st.suggest_placement({});
  for (const auto& id : place) {
    EXPECT_NE(id, "s4");
  }
}

TEST(ClusterStateTest, HeartbeatUpdatesLeader) {
  ClusterState st;
  st.bootstrap(2, 3, three_storages());
  const uint64_t ver0 = st.routing_table().version;
  EXPECT_TRUE(st.heartbeat("s1", {0}));
  auto rt = st.routing_table();
  EXPECT_GT(rt.version, ver0);
  int leaders = 0;
  bool s1 = false;
  for (const auto& r : rt.regions[0].replicas) {
    if (r.is_leader) {
      ++leaders;
      if (r.node_id == "s1") s1 = true;
    }
  }
  EXPECT_TRUE(s1);
  EXPECT_EQ(leaders, 1);
}

TEST(ClusterStateTest, MigratePersistRoundTrip) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  opskv::meta::PersistMigrate pm;
  pm.task_id = "mig-1";
  pm.region_id = 0;
  pm.state = 1;
  pm.desired_peers = {"s0", "s1", "s2"};
  st.upsert_migrate(pm);
  auto snap = st.serialize();
  ClusterState st2;
  ASSERT_TRUE(st2.load_snapshot(snap));
  auto list = st2.list_migrates();
  ASSERT_EQ(list.size(), 1u);
  EXPECT_EQ(list[0].task_id, "mig-1");
  EXPECT_EQ(list[0].desired_peers.size(), 3u);
  st2.erase_migrate("mig-1");
  EXPECT_TRUE(st2.list_migrates().empty());
}

TEST(ClusterStateTest, DrainExcludesPlacement) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  ASSERT_TRUE(st.set_node_state("s1", NodeState::kDraining));
  EXPECT_EQ(st.get_node("s1")->state, NodeState::kDraining);
  auto place = st.suggest_placement({});
  ASSERT_EQ(place.size(), 2u);
  for (const auto& id : place) EXPECT_NE(id, "s1");
  ASSERT_TRUE(st.set_node_state("s1", NodeState::kActive));
  EXPECT_EQ(st.suggest_placement({}).size(), 3u);
}

TEST(ClusterStateTest, SweepStaleExcludesPlacement) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // 仅刷新 s0/s1；s2 仍为 bootstrap 时的 last_hb → 过期
  ASSERT_TRUE(st.heartbeat("s0", {0}));
  ASSERT_TRUE(st.heartbeat("s1", {}));
  EXPECT_EQ(st.sweep_stale(1), 1u);
  EXPECT_EQ(st.get_node("s2")->state, NodeState::kOffline);
  EXPECT_EQ(st.get_node("s0")->state, NodeState::kActive);
  auto place = st.suggest_placement({});
  ASSERT_EQ(place.size(), 2u);
  for (const auto& id : place) {
    EXPECT_NE(id, "s2");
  }
}

TEST(ClusterStateTest, OfflineReregisterBecomesJoining) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  NodeInfo n;
  n.node_id = "s3";
  n.address = "127.0.0.1:9103";
  n.role = NodeRole::kStorage;
  ASSERT_EQ(st.register_node(n), "s3");
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);
  ASSERT_TRUE(st.set_node_state("s3", NodeState::kOffline));
  // 再 REGISTER：Offline → Joining（待运维 JOIN），不得直接 Active
  EXPECT_EQ(st.register_node(n), "s3");
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);
}

TEST(ClusterStateTest, SweepSkipsJoining) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  NodeInfo n;
  n.node_id = "s3";
  n.address = "127.0.0.1:9103";
  n.role = NodeRole::kStorage;
  ASSERT_EQ(st.register_node(n), "s3");
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_TRUE(st.heartbeat("s0", {0}));
  ASSERT_TRUE(st.heartbeat("s1", {}));
  ASSERT_TRUE(st.heartbeat("s2", {}));
  EXPECT_EQ(st.sweep_stale(1), 0u);  // Joining 不因 TTL 变 Offline
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);
}

TEST(ClusterStateTest, LearnerPromoteAndBump) {
  ClusterState st;
  st.bootstrap(1, 3, three_storages());
  Replica learner;
  learner.node_id = "s3";
  learner.address = "127.0.0.1:9103";
  learner.is_learner = true;
  ASSERT_TRUE(st.set_learner(0, learner));
  ASSERT_TRUE(st.promote_learner(0, "s3"));

  auto before = st.routing_table().version;
  st.bump_route_version();
  EXPECT_EQ(st.routing_table().version, before + 1);

  auto rt = st.routing_table();
  ASSERT_EQ(rt.regions.size(), 1u);
  ASSERT_GE(rt.regions[0].replicas.size(), 4u);
}

TEST(ClusterStateTest, ListNodesFilter) {
  ClusterState st;
  st.bootstrap(1, 3, three_storages());
  NodeInfo c;
  c.node_id = "c0";
  c.role = NodeRole::kCompute;
  c.address = "127.0.0.1:19000";
  st.register_node(c);
  EXPECT_EQ(st.list_nodes(NodeRole::kCompute).size(), 1u);
  EXPECT_GE(st.list_nodes(NodeRole::kStorage).size(), 3u);
}

TEST(ClusterStateTest, BootstrapExtrasAreJoining) {
  std::vector<NodeInfo> six = three_storages();
  six.push_back({"s3", NodeRole::kStorage, "127.0.0.1:9103", NodeState::kActive, 0});
  six.push_back({"s4", NodeRole::kStorage, "127.0.0.1:9104", NodeState::kActive, 0});
  six.push_back({"s5", NodeRole::kStorage, "127.0.0.1:9105", NodeState::kActive, 0});
  ClusterState st;
  st.bootstrap_range(3, six);
  EXPECT_EQ(st.get_node("s0")->state, NodeState::kActive);
  EXPECT_EQ(st.get_node("s3")->state, NodeState::kJoining);
  EXPECT_EQ(st.get_node("s5")->state, NodeState::kJoining);
  // 待加入不进放置
  auto sug = st.suggest_placement({});
  ASSERT_EQ(sug.size(), 3u);
  for (const auto& id : sug) {
    EXPECT_TRUE(id == "s0" || id == "s1" || id == "s2");
  }
}

TEST(ClusterStateTest, SuggestPlacementAvoidsBusy) {
  std::vector<NodeInfo> six = three_storages();
  six.push_back({"s3", NodeRole::kStorage, "127.0.0.1:9103", NodeState::kActive, 0});
  six.push_back({"s4", NodeRole::kStorage, "127.0.0.1:9104", NodeState::kActive, 0});
  six.push_back({"s5", NodeRole::kStorage, "127.0.0.1:9105", NodeState::kActive, 0});
  ClusterState st;
  st.bootstrap_range(3, six);
  // 模拟运维 join 后再调度
  ASSERT_TRUE(st.set_node_state("s3", NodeState::kActive));
  ASSERT_TRUE(st.set_node_state("s4", NodeState::kActive));
  ASSERT_TRUE(st.set_node_state("s5", NodeState::kActive));
  auto sug = st.suggest_placement({"s0", "s1", "s2"});
  ASSERT_EQ(sug.size(), 3u);
  for (const auto& id : sug) {
    EXPECT_TRUE(id == "s3" || id == "s4" || id == "s5");
  }
  auto right = st.adjacent_right(0);
  EXPECT_FALSE(right.has_value());
  ASSERT_TRUE(st.split_region(0, "m"));
  right = st.adjacent_right(0);
  ASSERT_TRUE(right);
  EXPECT_EQ(*right, 1u);
}

TEST(ClusterStateTest, SnapshotRoundTrip) {
  ClusterState st;
  st.bootstrap_range(3, three_storages());
  ASSERT_TRUE(st.split_region(0, "m").has_value());
  (void)st.alloc_ts(10);
  st.report_min_start_ts(3);
  st.advance_gc_safe_ts(2);
  st.bump_route_version();
  auto blob = st.serialize();
  ASSERT_FALSE(blob.empty());
  EXPECT_EQ(blob.substr(0, 4), "MSK1");

  ClusterState loaded;
  ASSERT_TRUE(loaded.load_snapshot(blob));
  EXPECT_EQ(loaded.next_ts(), st.next_ts());
  EXPECT_EQ(loaded.gc_safe_ts(), st.gc_safe_ts());
  EXPECT_EQ(loaded.routing_table().version, st.routing_table().version);
  EXPECT_EQ(loaded.routing_table().regions.size(), 2u);
  EXPECT_EQ(loaded.list_nodes().size(), 3u);
  auto loc = loaded.locate("zzz");
  ASSERT_TRUE(loc.has_value());
  EXPECT_NE(*loc, 0u);
  EXPECT_FALSE(loaded.load_snapshot("BAD"));
  EXPECT_FALSE(loaded.load_snapshot(""));
}

TEST(ClusterStateTest, PlaceReplicasOntoSubset) {
  std::vector<NodeInfo> six = three_storages();
  six.push_back({"s3", NodeRole::kStorage, "127.0.0.1:9103", NodeState::kActive, 0});
  six.push_back({"s4", NodeRole::kStorage, "127.0.0.1:9104", NodeState::kActive, 0});
  six.push_back({"s5", NodeRole::kStorage, "127.0.0.1:9105", NodeState::kActive, 0});
  ClusterState st;
  st.bootstrap_range(3, six);  // Region 0 on s0,s1,s2；s3–s5 Joining
  ASSERT_TRUE(st.set_node_state("s3", NodeState::kActive));
  ASSERT_TRUE(st.set_node_state("s4", NodeState::kActive));
  ASSERT_TRUE(st.set_node_state("s5", NodeState::kActive));
  ASSERT_EQ(st.find_region(0)->replicas.size(), 3u);
  EXPECT_EQ(st.find_region(0)->replicas[0].node_id, "s0");

  std::vector<Replica> want;
  for (const char* id : {"s3", "s4", "s5"}) {
    Replica r;
    r.node_id = id;
    want.push_back(r);
  }
  ASSERT_TRUE(st.place_replicas(0, want));
  auto r0 = st.find_region(0);
  ASSERT_TRUE(r0);
  ASSERT_EQ(r0->replicas.size(), 3u);
  EXPECT_EQ(r0->replicas[0].node_id, "s3");
  EXPECT_EQ(r0->replicas[1].node_id, "s4");
  EXPECT_EQ(r0->replicas[2].node_id, "s5");
  EXPECT_TRUE(st.remove_replica(0, "s3"));
  EXPECT_EQ(st.find_region(0)->replicas.size(), 2u);
}
