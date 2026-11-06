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

// @file scaler_test.cpp
// 测试 Scaler scale-out 任务从 Pending 到 Completed 及钩子调用次数。

#include "opskv/meta/scaler.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

using opskv::meta::ClusterState;
using opskv::meta::NodeInfo;
using opskv::meta::NodeRole;
using opskv::meta::NodeState;
using opskv::meta::Replica;
using opskv::meta::ScaleTaskState;
using opskv::meta::Scaler;

TEST(ScalerTest, ScaleOutCompletes) {
  ClusterState st;
  st.bootstrap(4, 3,
               {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0},
                {"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kActive, 0},
                {"s2", NodeRole::kStorage, "127.0.0.1:9102", NodeState::kActive, 0}});
  NodeInfo n{"s3", NodeRole::kStorage, "127.0.0.1:9103", NodeState::kJoining, 0};
  st.register_node(n);
  ASSERT_TRUE(st.set_node_state("s3", NodeState::kActive));  // scale-out 要求 Active

  int add_calls = 0;
  int promote_calls = 0;
  Scaler scaler(
      st,
      [&](uint32_t sid, const Replica& r) {
        ++add_calls;
        return st.set_learner(sid, r);
      },
      [&](uint32_t sid, const std::string& id) {
        ++promote_calls;
        return st.promote_learner(sid, id);
      });

  auto before = st.routing_table().version;
  // Range 模式初始仅 Region 0
  auto task = scaler.start_scale_out("s3", {0});
  ASSERT_FALSE(task.task_id.empty());

  bool done = false;
  for (int i = 0; i < 50; ++i) {
    auto t = scaler.get_task(task.task_id);
    ASSERT_TRUE(t);
    if (t->state == ScaleTaskState::kCompleted) {
      done = true;
      EXPECT_DOUBLE_EQ(t->progress, 1.0);
      break;
    }
    ASSERT_NE(t->state, ScaleTaskState::kFailed) << t->error;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(done);
  EXPECT_GT(add_calls, 0);
  EXPECT_GT(promote_calls, 0);
  EXPECT_GT(st.routing_table().version, before);
}

TEST(ScalerTest, UnknownNodeFails) {
  ClusterState st;
  st.bootstrap(1, 1,
               {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0}});
  Scaler scaler(
      st, [](uint32_t, const Replica&) { return true; },
      [](uint32_t, const std::string&) { return true; });
  auto task = scaler.start_scale_out("missing", {0});
  EXPECT_EQ(task.state, ScaleTaskState::kFailed);
  EXPECT_EQ(task.error, "unknown_node");
}

TEST(ScalerTest, ConcurrentScaleOutQueues) {
  ClusterState st;
  st.bootstrap(1, 1,
               {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0}});
  for (const char* id : {"s1", "s2"}) {
    NodeInfo n{id, NodeRole::kStorage, std::string("127.0.0.1:910") + id[1], NodeState::kJoining, 0};
    st.register_node(n);
    ASSERT_TRUE(st.set_node_state(id, NodeState::kActive));
  }

  std::atomic<int> add_calls{0};
  Scaler scaler(
      st,
      [&](uint32_t sid, const Replica& r) {
        ++add_calls;
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        return st.set_learner(sid, r);
      },
      [&](uint32_t sid, const std::string& id) { return st.promote_learner(sid, id); });

  auto t1 = scaler.start_scale_out("s1", {0});
  auto t2 = scaler.start_scale_out("s2", {0});
  EXPECT_EQ(t1.state, ScaleTaskState::kPending);
  EXPECT_EQ(t2.state, ScaleTaskState::kPending);  // 排队，不是 scale_busy Failed
  EXPECT_TRUE(t2.error.empty());

  auto dup = scaler.start_scale_out("s1", {0});
  EXPECT_EQ(dup.state, ScaleTaskState::kFailed);
  EXPECT_EQ(dup.error, "duplicate_inflight");

  bool d1 = false, d2 = false;
  for (int i = 0; i < 100; ++i) {
    auto a = scaler.get_task(t1.task_id);
    auto b = scaler.get_task(t2.task_id);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    if (a->state == ScaleTaskState::kCompleted) d1 = true;
    if (b->state == ScaleTaskState::kCompleted) d2 = true;
    if (d1 && d2) break;
    ASSERT_NE(a->state, ScaleTaskState::kFailed) << a->error;
    ASSERT_NE(b->state, ScaleTaskState::kFailed) << b->error;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(d1);
  EXPECT_TRUE(d2);
  EXPECT_GE(add_calls.load(), 2);
}

TEST(ScalerTest, CancelInflight) {
  ClusterState st;
  st.bootstrap(1, 1,
               {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0}});
  NodeInfo n{"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kJoining, 0};
  st.register_node(n);
  ASSERT_TRUE(st.set_node_state("s1", NodeState::kActive));

  std::atomic<bool> release{false};
  Scaler scaler(
      st,
      [&](uint32_t sid, const Replica& r) { return st.set_learner(sid, r); },
      [&](uint32_t sid, const std::string& id) { return st.promote_learner(sid, id); },
      nullptr,
      [&](uint32_t, const std::string&) {
        // 一直不追平，直到被 cancel
        return release.load();
      });

  auto task = scaler.start_scale_out("s1", {0});
  bool saw_catching = false;
  for (int i = 0; i < 40; ++i) {
    auto t = scaler.get_task(task.task_id);
    ASSERT_TRUE(t);
    if (t->state == ScaleTaskState::kCatchingUp) {
      saw_catching = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(saw_catching);
  EXPECT_EQ(scaler.cancel_inflight("s1", 0), 1);
  auto t = scaler.get_task(task.task_id);
  ASSERT_TRUE(t);
  EXPECT_EQ(t->state, ScaleTaskState::kFailed);
  EXPECT_EQ(t->error, "cancelled");
  EXPECT_FALSE(scaler.has_inflight());
}

TEST(ScalerTest, CatchUpGateWaits) {
  ClusterState st;
  st.bootstrap(1, 1,
               {{"s0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kActive, 0}});
  NodeInfo n{"s1", NodeRole::kStorage, "127.0.0.1:9101", NodeState::kJoining, 0};
  st.register_node(n);
  ASSERT_TRUE(st.set_node_state("s1", NodeState::kActive));
  st.heartbeat("s1", {});

  std::atomic<int> checks{0};
  Scaler scaler(
      st,
      [&](uint32_t sid, const Replica& r) { return st.set_learner(sid, r); },
      [&](uint32_t sid, const std::string& id) { return st.promote_learner(sid, id); },
      nullptr,
      [&](uint32_t, const std::string&) {
        // 前 3 次未追平，之后追平
        return ++checks >= 3;
      });

  auto task = scaler.start_scale_out("s1", {0});
  bool done = false;
  bool saw_catching = false;
  for (int i = 0; i < 80; ++i) {
    auto t = scaler.get_task(task.task_id);
    ASSERT_TRUE(t);
    if (t->state == ScaleTaskState::kCatchingUp) saw_catching = true;
    if (t->state == ScaleTaskState::kCompleted) {
      done = true;
      break;
    }
    ASSERT_NE(t->state, ScaleTaskState::kFailed) << t->error;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(saw_catching);
  EXPECT_TRUE(done);
  EXPECT_GE(checks.load(), 3);
}
