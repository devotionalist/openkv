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

#include "opskv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using opskv::raft::ConfChange;
using opskv::raft::RaftNode;

namespace {

std::unique_ptr<RaftNode> make_leader(std::vector<std::string>* applied, bool async = true) {
  RaftNode::Options opts;
  opts.shard_id = 0;
  opts.self_id = "n0";
  opts.peers = {{"n0", "127.0.0.1:1", false}};
  opts.election_timeout_ms = 200;
  opts.heartbeat_ms = 50;
  opts.enable_async_apply = async;
  opts.coalesce_replication = false;
  opts.check_quorum = false;
  auto on_apply = [applied](uint64_t, const std::string& data) {
    if (applied) applied->push_back(data);
  };
  auto node = std::make_unique<RaftNode>(std::move(opts), std::move(on_apply), [](auto&&...) {});
  for (int i = 0; i < 40; ++i) {
    node->tick();
    if (node->is_leader()) break;
  }
  return node;
}

}  // namespace

TEST(RaftJointTest, PromoteUsesEnterThenLeaveJoint) {
  std::vector<std::string> applied;
  auto node = make_leader(&applied);

  ConfChange add;
  add.type = ConfChange::kAddLearner;
  add.peer = {"n1", "127.0.0.1:2", true};
  ASSERT_TRUE(node->propose_conf_change(add));
  for (int i = 0; i < 20; ++i) node->tick();

  bool has_learner = false;
  for (const auto& p : node->peers()) {
    if (p.id == "n1" && p.learner) has_learner = true;
  }
  EXPECT_TRUE(has_learner);

  ConfChange prom;
  prom.type = ConfChange::kPromoteLearner;
  prom.peer = {"n1", "127.0.0.1:2", false};
  ASSERT_TRUE(node->propose_conf_change(prom));
  for (int i = 0; i < 5; ++i) node->tick();
  ASSERT_TRUE(node->in_joint());
  // C_new={n0,n1} 需要 n1 追平才能提交 LeaveJoint（无网络时模拟 reply）
  const auto st = node->soft_state();
  node->on_append_entries_reply("n1", st.term, true, node->last_index());
  for (int i = 0; i < 40; ++i) node->tick();

  EXPECT_FALSE(node->in_joint());
  bool n1_voter = false;
  for (const auto& p : node->peers()) {
    if (p.id == "n1" && !p.learner) n1_voter = true;
  }
  EXPECT_TRUE(n1_voter);
}

TEST(RaftJointTest, RejectConfWhileJointPending) {
  auto node = make_leader(nullptr, /*async=*/false);
  ConfChange add;
  add.type = ConfChange::kAddLearner;
  add.peer = {"n1", "a", true};
  ASSERT_TRUE(node->propose_conf_change(add));
  for (int i = 0; i < 10; ++i) node->tick();

  ConfChange prom;
  prom.type = ConfChange::kPromoteLearner;
  prom.peer = {"n1", "a", false};
  ASSERT_TRUE(node->propose_conf_change(prom));
  for (int i = 0; i < 2; ++i) node->tick();
  ASSERT_TRUE(node->in_joint());
  EXPECT_FALSE(node->propose_conf_change(prom));
  const auto st = node->soft_state();
  node->on_append_entries_reply("n1", st.term, true, node->last_index());
  for (int i = 0; i < 40; ++i) node->tick();
  EXPECT_FALSE(node->in_joint());
}

TEST(RaftAsyncApplyTest, ProposeAppliesViaWorker) {
  std::vector<std::string> applied;
  auto node = make_leader(&applied, /*async=*/true);
  ASSERT_TRUE(node->propose("a"));
  ASSERT_TRUE(node->propose("b"));
  for (int i = 0; i < 50; ++i) {
    node->tick();
    if (applied.size() >= 2) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_GE(applied.size(), 2u);
  EXPECT_EQ(applied[0], "a");
  EXPECT_EQ(applied[1], "b");
}
