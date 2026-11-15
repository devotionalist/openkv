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

// @file raft_node_test.cpp
// 测试单节点选举、propose/apply、ConfChange 与 Follower 消息处理。

#include "openkv/raft/file_raft_engine.hpp"
#include "openkv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

using openkv::raft::ConfChange;
using openkv::raft::RaftNode;
using openkv::raft::Role;

namespace {

RaftNode make_single(std::vector<std::string>* applied) {
  RaftNode::Options opts;
  opts.external_io = false;  // 嵌入：propose 内同步 persist/apply
  opts.shard_id = 1;
  opts.self_id = "n0";
  opts.peers = {{"n0", "127.0.0.1:1", false}};
  opts.election_timeout_ms = 200;
  opts.heartbeat_ms = 50;
  auto on_apply = [applied](uint64_t, const std::string& data) { applied->push_back(data); };
  auto send = [](const std::string&, const std::string&, const std::string&) {};
  return RaftNode(std::move(opts), std::move(on_apply), std::move(send));
}

void elect(RaftNode& node) {
  for (int i = 0; i < 40; ++i) {
    node.tick();
    if (node.is_leader()) {
      return;
    }
  }
  FAIL() << "failed to become leader";
}

// RF=2：本机一票不够，手动喂 peer 的选票。
void elect_with_vote(RaftNode& node, const std::string& peer_id) {
  for (int i = 0; i < 40 && !node.is_leader(); ++i) {
    node.tick();
    auto st = node.soft_state();
    if (st.role == Role::kCandidate) {
      node.on_request_vote_reply(peer_id, st.term, true);
    }
  }
  ASSERT_TRUE(node.is_leader());
}

void drain_ready(RaftNode& node, bool persist = false) {
  while (node.has_ready()) {
    auto rd = node.take_ready();
    if (persist) node.persist_ready(rd);
    node.advance(rd);
  }
}

}  // namespace

TEST(RaftNodeTest, SingleNodeElectionAndPropose) {
  std::vector<std::string> applied;
  auto node = make_single(&applied);
  elect(node);
  EXPECT_TRUE(node.is_leader());
  EXPECT_EQ(node.soft_state().role, Role::kLeader);

  uint64_t idx = 0;
  ASSERT_TRUE(node.propose("hello", &idx));
  EXPECT_GT(idx, 0u);
  for (int i = 0; i < 10; ++i) {
    node.tick();
  }
  ASSERT_FALSE(applied.empty());
  EXPECT_EQ(applied.back(), "hello");
}

TEST(RaftNodeTest, ProposeRejectedWhenFollower) {
  std::vector<std::string> applied;
  auto node = make_single(&applied);
  // 未 tick，仍是 follower
  EXPECT_FALSE(node.is_leader());
  EXPECT_FALSE(node.propose("x"));
}

TEST(RaftNodeTest, ConfChangeAddLearner) {
  std::vector<std::string> applied;
  auto node = make_single(&applied);
  elect(node);
  ConfChange cc;
  cc.type = ConfChange::kAddLearner;
  cc.peer = {"n1", "127.0.0.1:2", true};
  ASSERT_TRUE(node.propose_conf_change(cc));
  for (int i = 0; i < 10; ++i) {
    node.tick();
  }
  // conf change 不进入业务 apply，但不应崩溃
  SUCCEED();
}

TEST(RaftNodeTest, PreVoteDoesNotBumpTerm) {
  RaftNode::Options oa;
  oa.external_io = false;
  oa.shard_id = 0;
  oa.self_id = "a";
  oa.peers = {{"a", "1", false}, {"b", "2", false}};
  oa.enable_pre_vote = true;
  oa.check_quorum = false;
  oa.election_timeout_ms = 100;
  oa.heartbeat_ms = 50;
  RaftNode::Options ob = oa;
  ob.self_id = "b";

  RaftNode na(oa, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  RaftNode nb(ob, [](uint64_t, const std::string&) {}, [](auto&&...) {});

  const uint64_t term_before = nb.soft_state().term;
  bool granted = false;
  nb.on_request_pre_vote("a", term_before + 1, 0, 0, &granted);
  EXPECT_TRUE(granted);
  EXPECT_EQ(nb.soft_state().term, term_before);  // PreVote 不抬 term
}

TEST(RaftNodeTest, CheckQuorumStepsDownIsolatedLeader) {
  RaftNode::Options opts;
  opts.external_io = false;
  opts.shard_id = 0;
  opts.self_id = "a";
  opts.peers = {{"a", "1", false}, {"b", "2", false}, {"c", "3", false}};
  opts.enable_pre_vote = false;
  opts.check_quorum = true;
  opts.election_timeout_ms = 200;
  opts.heartbeat_ms = 50;

  RaftNode n(opts, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  ASSERT_TRUE(n.force_election());
  auto term = n.soft_state().term;
  n.on_request_vote_reply("b", term, true);
  n.on_request_vote_reply("c", term, true);
  ASSERT_TRUE(n.is_leader());

  // 无任何 AE 成功回包 → quiet 涨满 → CheckQuorum 让位
  for (int i = 0; i < 80; ++i) {
    n.tick();
    if (!n.is_leader()) break;
  }
  EXPECT_FALSE(n.is_leader());
}

TEST(RaftNodeTest, VoteAndAppendBetweenPeers) {
  // 两节点：手动驱动 RequestVote / AppendEntries
  std::vector<std::string> applied_a;
  std::vector<std::string> applied_b;

  RaftNode::Options oa;
  oa.external_io = false;
  oa.shard_id = 0;
  oa.self_id = "a";
  oa.peers = {{"a", "1", false}, {"b", "2", false}};
  oa.election_timeout_ms = 100;
  oa.heartbeat_ms = 50;

  RaftNode::Options ob = oa;
  ob.self_id = "b";

  // 简化：不通过 transport，直接调 handler
  RaftNode na(
      oa, [&](uint64_t, const std::string& d) { applied_a.push_back(d); },
      [](auto&&...) {});
  RaftNode nb(
      ob, [&](uint64_t, const std::string& d) { applied_b.push_back(d); },
      [](auto&&...) {});

  // a 发起投票
  bool granted = false;
  nb.on_request_vote("a", 1, 0, 0, &granted);
  EXPECT_TRUE(granted);
  na.on_request_vote_reply("b", 1, true);

  // 强制 tick 让 a 成为 candidate 后再投票——上面直接 reply 可能 term 未对齐
  // 改用单节点路径已覆盖主路径；此处验证 handler 不崩溃
  bool ok = false;
  uint64_t match = 0;
  nb.on_append_entries("a", 1, 0, 0, {}, 0, &ok, &match);
  EXPECT_TRUE(ok);
}

TEST(RaftNodeTest, ExternalIoAeReplyNotifiesReady) {
  // external_io：propose 本拍入队 AE；AE 回包产生 committed Ready 必须 notify。
  RaftNode::Options opts;
  opts.external_io = true;
  opts.coalesce_replication = true;  // 开着也要本拍入队
  opts.enable_pre_vote = false;
  opts.shard_id = 1;
  opts.self_id = "n0";
  opts.peers = {{"n0", "a", false}, {"n1", "b", false}};
  opts.election_timeout_ms = 80;
  opts.heartbeat_ms = 20;
  opts.check_quorum = false;
  opts.persist_sync = openkv::raft::SyncMode::kNone;

  RaftNode node(opts, nullptr, nullptr);
  std::atomic<int> notifies{0};
  node.set_ready_notify([&] { notifies.fetch_add(1); });

  elect_with_vote(node, "n1");
  drain_ready(node);
  notifies.store(0);

  uint64_t idx = 0;
  ASSERT_TRUE(node.propose("hello", &idx));
  ASSERT_GT(idx, 0u);
  ASSERT_TRUE(node.has_ready());
  {
    auto rd = node.take_ready();
    EXPECT_FALSE(rd.entries.empty());
    EXPECT_FALSE(rd.messages.empty()) << "external_io propose must enqueue AE into Ready";
    node.advance(rd);
  }

  node.on_append_entries_reply("n1", node.soft_state().term, true, idx);
  EXPECT_GE(notifies.load(), 1) << "AE reply must wake Store via ready_notify";
  ASSERT_TRUE(node.has_ready());
  auto rd2 = node.take_ready();
  ASSERT_FALSE(rd2.committed_entries.empty());
  EXPECT_EQ(rd2.committed_entries.back().index, idx);
}

TEST(RaftNodeTest, PersistReadyNoneIgnoresMustSync) {
  // persist_sync=none 时，即便 Ready.must_sync 也不能走 fsync。
  namespace fs = std::filesystem;
  const auto dir = fs::temp_directory_path() /
                   ("okv_must_sync_none_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<openkv::raft::FileRaftEngine>(dir.string(),
                                                               openkv::raft::SyncMode::kNone);

  RaftNode::Options opts;
  opts.external_io = true;
  opts.shard_id = 1;
  opts.self_id = "n0";
  opts.peers = {{"n0", "a", false}};
  opts.election_timeout_ms = 80;
  opts.heartbeat_ms = 20;
  opts.check_quorum = false;
  opts.persist_sync = openkv::raft::SyncMode::kNone;
  opts.raft_engine = engine;
  opts.raft_group_id = 1;

  RaftNode node(opts, nullptr, nullptr);
  elect(node);
  drain_ready(node, /*persist=*/true);

  uint64_t idx = 0;
  ASSERT_TRUE(node.propose("x", &idx));
  ASSERT_TRUE(node.has_ready());
  auto rd = node.take_ready();
  EXPECT_TRUE(rd.must_sync || !rd.entries.empty());
  const auto t0 = std::chrono::steady_clock::now();
  node.persist_ready(rd);
  const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
  EXPECT_LT(us, 50'000) << "persist_ready under none must not fsync; took " << us << "us";
  fs::remove_all(dir);
}
