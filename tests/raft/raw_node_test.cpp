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

// @file raw_node_test.cpp
// RawNode 纯内存 Ready 循环：无盘、无网。

#include "opskv/raft/raw_node.hpp"
#include "opskv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

namespace {

using opskv::raft::LogEntry;
using opskv::raft::Peer;
using opskv::raft::RawNode;
using opskv::raft::Ready;

RawNode make_single(const std::string& id = "n1") {
  RawNode::Options o;
  o.shard_id = 1;
  o.self_id = id;
  o.peers = {Peer{id, "127.0.0.1:1", false}};
  o.election_timeout_ms = 200;
  o.heartbeat_ms = 50;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  return RawNode(std::move(o));
}

void drain_ready(RawNode& n) {
  while (n.has_ready()) {
    Ready rd = n.ready();
    // ApplyFsm 语义：外层 apply 后再 report_applied；单测内联模拟。
    for (const auto& e : rd.committed_entries) {
      if (auto* rn = n.raft_node()) rn->report_applied(e.index);
    }
    n.advance(rd);
  }
}

TEST(RawNodeTest, SingleNodeElectAndPropose) {
  auto n = make_single();
  for (int i = 0; i < 40 && !n.is_leader(); ++i) {
    n.tick();
    drain_ready(n);
  }
  ASSERT_TRUE(n.is_leader());
  uint64_t idx = 0;
  ASSERT_TRUE(n.propose("hello", &idx));
  ASSERT_GT(idx, 0u);
  ASSERT_TRUE(n.has_ready());
  Ready rd = n.ready();
  ASSERT_FALSE(rd.entries.empty());
  bool saw = false;
  for (const auto& e : rd.committed_entries) {
    if (e.data == "hello") saw = true;
  }
  EXPECT_TRUE(saw) << "committed_entries size=" << rd.committed_entries.size();
  if (rd.hard_state) {
    EXPECT_GE(rd.hard_state->commit, idx);
  }
  for (const auto& e : rd.committed_entries) {
    if (auto* rn = n.raft_node()) rn->report_applied(e.index);
  }
  n.advance(rd);
  EXPECT_GE(n.soft_state().last_applied, idx);
}

TEST(RawNodeTest, BootstrapThenPropose) {
  auto n = make_single();
  std::vector<LogEntry> ents;
  ents.push_back(LogEntry{1, 1, "seed"});
  n.bootstrap_from_disk(/*term=*/1, /*vote=*/"n1", /*commit=*/1, /*applied=*/1, ents,
                        /*first=*/1, /*snap_term=*/0);
  for (int i = 0; i < 40 && !n.is_leader(); ++i) {
    n.tick();
    drain_ready(n);
  }
  ASSERT_TRUE(n.is_leader());
  ASSERT_TRUE(n.propose("more"));
  drain_ready(n);
  EXPECT_GE(n.last_index(), 2u);
}

TEST(RawNodeTest, ThreeNodeVoteAndProposeReady) {
  RawNode::Options oa;
  oa.shard_id = 7;
  oa.self_id = "a";
  oa.peers = {Peer{"a", "", false}, Peer{"b", "", false}, Peer{"c", "", false}};
  oa.election_timeout_ms = 300;
  oa.heartbeat_ms = 40;
  oa.enable_pre_vote = false;
  oa.check_quorum = false;
  RawNode a(oa);

  RawNode::Options ob = oa;
  ob.self_id = "b";
  RawNode b(std::move(ob));
  RawNode::Options oc = oa;
  oc.self_id = "c";
  RawNode c(std::move(oc));

  ASSERT_TRUE(a.force_election());
  for (int round = 0; round < 30 && !a.is_leader(); ++round) {
    if (!a.has_ready()) {
      a.tick();
      continue;
    }
    Ready rd = a.ready();
    for (const auto& m : rd.messages) {
      if (m.type != "RequestVote") continue;
      RawNode* dst = m.to == "b" ? &b : (m.to == "c" ? &c : nullptr);
      if (!dst) continue;
      unsigned long long term = 0, li = 0, lt = 0;
      if (sscanf(m.payload.c_str(), "%llu|%llu|%llu", &term, &li, &lt) != 3) continue;
      bool granted = false;
      dst->step_request_vote("a", static_cast<uint64_t>(term), static_cast<uint64_t>(li),
                            static_cast<uint64_t>(lt), &granted);
      a.step_request_vote_reply(m.to, static_cast<uint64_t>(term), granted);
      if (dst->has_ready()) {
        Ready r2 = dst->ready();
        dst->advance(r2);
      }
    }
    a.advance(rd);
  }
  ASSERT_TRUE(a.is_leader());
  ASSERT_TRUE(a.propose("kv"));
  ASSERT_TRUE(a.has_ready());
  Ready rd = a.ready();
  EXPECT_FALSE(rd.entries.empty());
  EXPECT_FALSE(rd.messages.empty());  // AE to b/c
  a.advance(rd);
}

}  // namespace
