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

#include "opskv/meta/cluster_state.hpp"
#include "opskv/meta/region_scheduler.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using opskv::meta::ClusterState;
using opskv::meta::MigrateState;
using opskv::meta::NodeInfo;
using opskv::meta::NodeRole;
using opskv::meta::NodeState;
using opskv::meta::RegionScheduler;

TEST(RegionSchedulerTest, SubmitAndCompleteRouteOnly) {
  ClusterState st;
  std::vector<NodeInfo> stores = {
      {"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0},
      {"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kActive, 0},
      {"s2", NodeRole::kStorage, "127.0.0.1:9102", NodeState::kActive, 0},
  };
  st.bootstrap_range(3, stores);

  std::atomic<int> place_calls{0};
  std::atomic<int> commit_calls{0};
  RegionScheduler sched(
      st,
      [&](uint32_t, const std::vector<std::string>&) {
        ++place_calls;
        return true;
      },
      [&](uint32_t rid, const std::vector<std::string>& desired) {
        ++commit_calls;
        std::vector<opskv::meta::Replica> reps;
        for (const auto& id : desired) {
          auto n = st.get_node(id);
          if (!n) return false;
          reps.push_back({n->node_id, n->address, false, false});
        }
        return st.place_replicas(rid, reps);
      },
      [] { return true; }, 2);

  auto t = sched.submit(0, {"s0", "s1", "s2"});
  EXPECT_EQ(t.state, MigrateState::kPending);

  for (int i = 0; i < 50; ++i) {
    auto cur = sched.get_task(t.task_id);
    ASSERT_TRUE(cur.has_value());
    if (cur->state == MigrateState::kCompleted || cur->state == MigrateState::kFailed) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  auto done = sched.get_task(t.task_id);
  ASSERT_TRUE(done.has_value());
  EXPECT_EQ(done->state, MigrateState::kCompleted);
  EXPECT_GE(place_calls.load(), 1);
  EXPECT_GE(commit_calls.load(), 1);
}

TEST(RegionSchedulerTest, RestorePersistedTasks) {
  ClusterState st;
  st.bootstrap_range(3, {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0},
                         {"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kActive, 0},
                         {"s2", NodeRole::kStorage, "127.0.0.1:9102", NodeState::kActive, 0}});
  opskv::meta::PersistMigrate pm;
  pm.task_id = "mig-9";
  pm.region_id = 0;
  pm.state = static_cast<int>(MigrateState::kPlacing);
  pm.desired_peers = {"s0", "s1", "s2"};
  st.upsert_migrate(pm);

  std::atomic<int> commits{0};
  RegionScheduler sched(
      st, [](auto&&...) { return true; },
      [&](uint32_t, const std::vector<std::string>&) {
        ++commits;
        return true;
      },
      [] { return true; });
  sched.restore_persisted(st.list_migrates());
  auto t = sched.get_task("mig-9");
  ASSERT_TRUE(t);
  EXPECT_EQ(t->state, MigrateState::kPending);  // 恢复后重试
  for (int i = 0; i < 40; ++i) {
    auto cur = sched.get_task("mig-9");
    if (cur && cur->state == MigrateState::kCompleted) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  auto done = sched.get_task("mig-9");
  ASSERT_TRUE(done);
  EXPECT_EQ(done->state, MigrateState::kCompleted);
  EXPECT_GE(commits.load(), 1);
}

TEST(RegionSchedulerTest, ScanFindsUnderReplicated) {
  ClusterState st;
  std::vector<NodeInfo> stores = {
      {"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0},
      {"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kActive, 0},
      {"s2", NodeRole::kStorage, "127.0.0.1:9102", NodeState::kActive, 0},
  };
  st.bootstrap_range(3, stores);
  // place onto single peer → under-replicated vs rf=3
  ASSERT_TRUE(st.place_replicas(0, {{"s0", "127.0.0.1:9100", true, false}}));

  RegionScheduler sched(
      st, [](auto&&...) { return true; },
      [](uint32_t, const std::vector<std::string>&) { return true; }, [] { return true; });
  auto cands = sched.scan_candidates();
  ASSERT_FALSE(cands.empty());
  EXPECT_EQ(cands[0], 0u);
}
