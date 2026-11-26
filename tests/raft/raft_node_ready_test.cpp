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

// @file raft_node_ready_test.cpp
// RaftNode(external_io) 纯内存 Ready 循环：无盘、无网，外层手动 drain。

#include <gtest/gtest.h>

#include <string>
#include <variant>
#include <vector>

#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/raft_rpc.hpp"

namespace {

using openkv::raft::LogEntry;
using openkv::raft::Peer;
using openkv::raft::PersistedHardState;
using openkv::raft::RaftNode;
using openkv::raft::Ready;

RaftNode make_external(const std::string& id = "n1") {
  RaftNode::Options o;
  o.shard_id = 1;
  o.self_id = id;
  o.peers = {Peer{id, "127.0.0.1:1", false}};
  o.election_timeout_ms = 200;
  o.heartbeat_ms = 50;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  o.external_io = true;
  return RaftNode(std::move(o), nullptr, nullptr);
}

void drain_ready(RaftNode& n) {
  while (n.has_ready()) {
    Ready rd = n.take_ready();
    for (const auto& e : rd.committed_entries) {
      n.report_applied(e.index);
    }
    n.advance(rd);
  }
}

TEST(RaftNodeReadyTest, SingleNodeElectAndPropose) {
  auto n = make_external();
  for (int i = 0; i < 40 && !n.is_leader(); ++i) {
    n.tick();
    drain_ready(n);
  }
  ASSERT_TRUE(n.is_leader());
  uint64_t idx = 0;
  ASSERT_TRUE(n.propose("hello", &idx));
  ASSERT_GT(idx, 0u);
  ASSERT_TRUE(n.has_ready());
  Ready rd = n.take_ready();
  ASSERT_FALSE(rd.entries.empty());
  bool saw = false;
  for (const auto& e : rd.committed_entries) {
    if (e.data == "hello") {
      saw = true;
    }
  }
  EXPECT_TRUE(saw) << "committed_entries size=" << rd.committed_entries.size();
  if (rd.hard_state) {
    EXPECT_GE(rd.hard_state->commit, idx);
  }
  for (const auto& e : rd.committed_entries) {
    n.report_applied(e.index);
  }
  n.advance(rd);
  EXPECT_GE(n.soft_state().last_applied, idx);
}

TEST(RaftNodeReadyTest, BootstrapThenPropose) {
  auto n = make_external();
  std::vector<LogEntry> ents;
  ents.push_back(LogEntry{1, 1, "seed"});
  PersistedHardState hs;
  hs.term = 1;
  hs.voted_for = "n1";
  hs.commit_index = 1;
  hs.last_applied = 1;
  hs.first_index = 1;
  hs.snapshot_term = 0;
  n.load_hard_state(hs, ents);
  for (int i = 0; i < 40 && !n.is_leader(); ++i) {
    n.tick();
    drain_ready(n);
  }
  ASSERT_TRUE(n.is_leader());
  ASSERT_TRUE(n.propose("more"));
  drain_ready(n);
  EXPECT_GE(n.last_index(), 2u);
}

TEST(RaftNodeReadyTest, ThreeNodeVoteAndProposeReady) {
  RaftNode::Options oa;
  oa.shard_id = 7;
  oa.self_id = "a";
  oa.peers = {Peer{"a", "", false}, Peer{"b", "", false}, Peer{"c", "", false}};
  oa.election_timeout_ms = 300;
  oa.heartbeat_ms = 40;
  oa.enable_pre_vote = false;
  oa.check_quorum = false;
  oa.external_io = true;
  RaftNode a(oa, nullptr, nullptr);

  RaftNode::Options ob = oa;
  ob.self_id = "b";
  RaftNode b(std::move(ob), nullptr, nullptr);
  RaftNode::Options oc = oa;
  oc.self_id = "c";
  RaftNode c(std::move(oc), nullptr, nullptr);

  ASSERT_TRUE(a.force_election());
  for (int round = 0; round < 30 && !a.is_leader(); ++round) {
    if (!a.has_ready()) {
      a.tick();
      continue;
    }
    Ready rd = a.take_ready();
    for (const auto& m : rd.messages) {
      if (m.type != openkv::raft::MsgType::kRequestVote) {
        continue;
      }
      RaftNode* dst = m.to == "b" ? &b : (m.to == "c" ? &c : nullptr);
      if (!dst) {
        continue;
      }
      const auto* v = std::get_if<openkv::raft::VoteReq>(&m.payload);
      if (!v) {
        continue;
      }
      bool granted = false;
      dst->on_request_vote("a", v->term, v->last_log_index, v->last_log_term, &granted);
      a.on_request_vote_reply(m.to, v->term, granted);
      if (dst->has_ready()) {
        Ready r2 = dst->take_ready();
        dst->advance(r2);
      }
    }
    a.advance(rd);
  }
  ASSERT_TRUE(a.is_leader());
  ASSERT_TRUE(a.propose("kv"));
  ASSERT_TRUE(a.has_ready());
  Ready rd = a.take_ready();
  EXPECT_FALSE(rd.entries.empty());
  EXPECT_FALSE(rd.messages.empty());  // AE to b/c
  a.advance(rd);
}

}  // namespace
