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

// @file raft_rpc_structured_test.cpp
// 出站 Message 必须是结构体载荷（二进制 data），禁止再走 pipe|hex。

#include <gtest/gtest.h>

#include <string>
#include <variant>
#include <vector>

#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/raft_rpc.hpp"

namespace {

using openkv::raft::AppendEntriesReq;
using openkv::raft::MsgType;
using openkv::raft::Peer;
using openkv::raft::RaftNode;
using openkv::raft::Ready;
using openkv::raft::VoteReq;

TEST(RaftRpcStructured, LeaderAeCarriesBinaryEntriesNotHex) {
  RaftNode::Options oa;
  oa.shard_id = 1;
  oa.self_id = "a";
  oa.peers = {Peer{"a", "a", false}, Peer{"b", "b", false}, Peer{"c", "c", false}};
  oa.election_timeout_ms = 200;
  oa.heartbeat_ms = 50;
  oa.enable_pre_vote = false;
  oa.external_io = true;
  oa.persist_sync = openkv::raft::SyncMode::kNone;

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
      if (m.type != MsgType::kRequestVote) {
        continue;
      }
      RaftNode* dst = m.to == "b" ? &b : (m.to == "c" ? &c : nullptr);
      if (!dst) {
        continue;
      }
      const auto* v = std::get_if<VoteReq>(&m.payload);
      ASSERT_NE(v, nullptr);
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
  // 清掉当选后 probe 在途，否则 maybe_replicate 不会再带日志条目。
  if (a.has_ready()) {
    Ready rd0 = a.take_ready();
    for (const auto& m : rd0.messages) {
      if (m.type == MsgType::kAppendEntries) {
        a.on_append_entries_reply(m.to, a.soft_state().term, true, 0);
      }
    }
    a.advance(rd0);
  }

  std::string blob;
  blob.push_back('\0');
  blob.push_back(static_cast<char>(0xff));
  blob.append("raw");
  ASSERT_TRUE(a.propose(blob));
  ASSERT_TRUE(a.has_ready());
  Ready rd = a.take_ready();

  bool found = false;
  for (const auto& m : rd.messages) {
    if (m.type != MsgType::kAppendEntries) {
      continue;
    }
    const auto* ae = std::get_if<AppendEntriesReq>(&m.payload);
    ASSERT_NE(ae, nullptr);
    if (ae->entries.empty()) {
      continue;
    }
    EXPECT_EQ(ae->entries.back().data, blob);
    found = true;
  }
  EXPECT_TRUE(found);
  a.advance(rd);
}

TEST(RaftRpcStructured, StaleInstallSnapshotReplyDoesNotRegressMatch) {
  // 单投票人即可当选；b 只用来测 match 进度回退防护。
  RaftNode::Options oa;
  oa.shard_id = 1;
  oa.self_id = "a";
  oa.peers = {Peer{"a", "a", false}, Peer{"b", "b", false}};
  oa.election_timeout_ms = 200;
  oa.heartbeat_ms = 50;
  oa.enable_pre_vote = false;
  oa.external_io = true;
  oa.check_quorum = false;
  oa.persist_sync = openkv::raft::SyncMode::kNone;

  RaftNode a(oa, nullptr, nullptr);
  ASSERT_TRUE(a.force_election());
  for (int i = 0; i < 40 && !a.is_leader(); ++i) {
    if (!a.has_ready()) {
      a.tick();
      continue;
    }
    Ready rd = a.take_ready();
    for (const auto& m : rd.messages) {
      if (m.type != MsgType::kRequestVote) {
        continue;
      }
      const auto* v = std::get_if<VoteReq>(&m.payload);
      ASSERT_NE(v, nullptr);
      a.on_request_vote_reply(m.to, v->term, true);
    }
    a.advance(rd);
  }
  ASSERT_TRUE(a.is_leader());
  const uint64_t term = a.soft_state().term;

  a.on_append_entries_reply("b", term, true, 100);
  EXPECT_EQ(a.match_index_of("b"), 100u);

  // 迟到的旧快照应答不得把 match 打回 50。
  a.on_install_snapshot_reply("b", term, true, 50);
  EXPECT_EQ(a.match_index_of("b"), 100u);
}

TEST(RaftRpcStructured, CompactKeepsTrailingWindowForAe) {
  // apply 超过 threshold 后仍保留窗；peer 落后 < threshold 时应发 AE 而不是 InstallSnapshot。
  int snap_msgs = 0;
  int ae_data_msgs = 0;
  std::string fsm;

  RaftNode::Options oa;
  oa.shard_id = 1;
  oa.self_id = "a";
  oa.peers = {Peer{"a", "a", false}, Peer{"b", "b", false}};
  oa.election_timeout_ms = 200;
  oa.heartbeat_ms = 20;
  oa.enable_pre_vote = false;
  oa.external_io = false;
  oa.check_quorum = false;
  oa.compact_threshold = 16;
  oa.snapshot_min_interval_ms = 0;
  oa.persist_sync = openkv::raft::SyncMode::kNone;

  auto send = [&](openkv::raft::Message m) {
    if (m.type == MsgType::kInstallSnapshot) {
      ++snap_msgs;
      return;
    }
    if (m.type == MsgType::kAppendEntries) {
      const auto* ae = std::get_if<AppendEntriesReq>(&m.payload);
      if (ae && !ae->entries.empty()) {
        ++ae_data_msgs;
      }
    }
  };
  auto provider = [&](uint64_t*, uint64_t*) { return fsm; };
  auto applier = [&](uint64_t, uint64_t, std::string_view d) {
    fsm.assign(d);
    return true;
  };
  RaftNode a(oa, [&](uint64_t, const std::string& d) { fsm = d; }, send, provider, applier);

  ASSERT_TRUE(a.force_election());
  for (int i = 0; i < 40 && !a.is_leader(); ++i) {
    if (!a.has_ready() && a.soft_state().role != openkv::raft::Role::kLeader) {
      // force_election 已发起；直接假装 b 投票。
    }
    a.on_request_vote_reply("b", a.soft_state().term, true);
    a.tick();
  }
  ASSERT_TRUE(a.is_leader());

  for (int i = 0; i < 48; ++i) {
    ASSERT_TRUE(a.propose("p" + std::to_string(i)));
    // 单节点多数：自己即可提交；仍给 b 一个较新的 match，避免一上来就掉窗。
    a.on_append_entries_reply("b", a.soft_state().term, true, a.last_index());
  }
  ASSERT_GE(a.soft_state().last_applied, 40u);
  snap_msgs = 0;
  ae_data_msgs = 0;

  // 让 b 落后 8 条（小于 threshold=16）——旧逻辑窗已砍空，会走 InstallSnapshot。
  const uint64_t lag_match = a.soft_state().last_applied - 8;
  a.on_append_entries_reply("b", a.soft_state().term, true, lag_match);
  a.tick();
  a.tick();

  EXPECT_EQ(snap_msgs, 0) << "lag within compact_threshold must use AE";
  EXPECT_GT(ae_data_msgs, 0);
}

}  // namespace
