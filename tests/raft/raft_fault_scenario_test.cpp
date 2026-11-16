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

// @file raft_fault_scenario_test.cpp
// Raft 协议核故障全场景：内存集群 + 分区/丢包/丢回包；覆盖 docs/flows/40 清单 A–F。

#include "openkv/codec.hpp"
#include "openkv/raft/file_raft_engine.hpp"
#include "openkv/raft/raft_fault_inject.hpp"
#include "openkv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using openkv::raft::ConfChange;
using openkv::raft::LogEntry;
using openkv::raft::Peer;
using openkv::raft::RaftNode;
using openkv::raft::Role;
namespace fs = std::filesystem;

std::vector<std::string> split_pipe(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == '|') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

struct ClusterOpts {
  bool enable_pre_vote{true};
  bool check_quorum{true};
  bool enable_lease_read{true};
  uint32_t leader_lease_ms{200};
  uint32_t election_timeout_ms{120};
  uint32_t heartbeat_ms{40};
  size_t compact_threshold{256};
  std::vector<Peer> peers{{"a", "a", false}, {"b", "b", false}, {"c", "c", false}};
  bool with_snap{false};
};

class RaftFaultCluster {
 public:
  explicit RaftFaultCluster(ClusterOpts o = {}) : opts_(std::move(o)) {
    for (const auto& p : opts_.peers) {
      auto slot = std::make_unique<Slot>();
      slot->id = p.id;
      RaftNode::Options nopts;
      nopts.external_io = false;
      nopts.shard_id = 1;
      nopts.self_id = p.id;
      nopts.peers = opts_.peers;
      nopts.election_timeout_ms = opts_.election_timeout_ms;
      nopts.heartbeat_ms = opts_.heartbeat_ms;
      nopts.enable_pre_vote = opts_.enable_pre_vote;
      nopts.check_quorum = opts_.check_quorum;
      nopts.enable_lease_read = opts_.enable_lease_read;
      nopts.leader_lease_ms = opts_.leader_lease_ms;
      nopts.coalesce_replication = false;
      nopts.persist_sync = openkv::raft::SyncMode::kNone;
      nopts.compact_threshold = opts_.compact_threshold;

      openkv::raft::SnapshotProvider provider;
      openkv::raft::SnapshotApplier applier;
      if (opts_.with_snap) {
        Slot* raw = slot.get();
        provider = [raw](uint64_t* li, uint64_t* lt) {
          *li = raw->snap_index;
          *lt = raw->snap_term;
          return raw->snap_blob;
        };
        applier = [raw](uint64_t li, uint64_t lt, std::string_view data) {
          ++raw->snap_apply_calls;
          if (raw->snap_fail_next) {
            raw->snap_fail_next = false;
            return false;
          }
          raw->snap_index = li;
          raw->snap_term = lt;
          raw->snap_blob.assign(data);
          return true;
        };
      }

      Slot* raw = slot.get();
      auto on_apply = [raw](uint64_t, const std::string& data) { raw->applied.push_back(data); };
      auto send = [this, id = p.id](const std::string& to, const std::string& type,
                                    const std::string& payload) { deliver(id, to, type, payload); };
      slot->node = std::make_unique<RaftNode>(std::move(nopts), std::move(on_apply), std::move(send),
                                              std::move(provider), std::move(applier));
      nodes_[p.id] = std::move(slot);
    }
  }

  RaftNode& at(const std::string& id) { return *nodes_.at(id)->node; }
  const std::vector<std::string>& applied(const std::string& id) const {
    return nodes_.at(id)->applied;
  }

  void isolate(const std::string& id) { isolated_.insert(id); }
  void heal(const std::string& id) { isolated_.erase(id); }
  void drop_type(const std::string& type) { drop_types_.insert(type); }
  void allow_type(const std::string& type) { drop_types_.erase(type); }
  void set_drop_replies(bool v) { drop_replies_ = v; }
  void set_delay_ms(int ms) { delay_ms_ = ms; }
  void set_duplicate_delivery(bool v) { duplicate_ = v; }
  void heal_all() { isolated_.clear(); }
  bool any_isolated() const { return !isolated_.empty(); }
  void clear_faults() {
    drop_types_.clear();
    drop_replies_ = false;
    delay_ms_ = 0;
    duplicate_ = false;
    hold_to_.clear();
    held_.clear();
  }
  void hold_to(const std::string& id) { hold_to_ = id; }
  void release_held_reversed() {
    auto held = std::move(held_);
    held_.clear();
    hold_to_.clear();
    for (auto it = held.rbegin(); it != held.rend(); ++it) {
      deliver_now(it->from, it->to, it->type, it->payload);
    }
  }
  int dropped() const { return dropped_; }

  void tick_all(int n = 1) {
    for (int i = 0; i < n; ++i) {
      for (auto& [_, s] : nodes_) s->node->tick();
    }
  }

  void tick_ids(const std::vector<std::string>& ids, int n = 1) {
    for (int i = 0; i < n; ++i) {
      for (const auto& id : ids) nodes_.at(id)->node->tick();
    }
  }

  std::string wait_one_leader(int max_ticks = 200) {
    for (int i = 0; i < max_ticks; ++i) {
      tick_all(1);
      auto leaders = leader_ids();
      if (leaders.size() == 1) return leaders[0];
    }
    return {};
  }

  std::vector<std::string> leader_ids() const {
    std::vector<std::string> out;
    for (const auto& [id, s] : nodes_) {
      if (s->node->is_leader()) out.push_back(id);
    }
    return out;
  }

  bool wait_applied(const std::string& id, const std::string& data, int max_ticks = 80) {
    for (int i = 0; i < max_ticks; ++i) {
      for (const auto& d : nodes_.at(id)->applied) {
        if (d == data) return true;
      }
      tick_all(1);
    }
    for (const auto& d : nodes_.at(id)->applied) {
      if (d == data) return true;
    }
    return false;
  }

  bool has_applied(const std::string& id, const std::string& data) const {
    for (const auto& d : applied(id)) {
      if (d == data) return true;
    }
    return false;
  }

  struct Slot {
    std::string id;
    std::unique_ptr<RaftNode> node;
    std::vector<std::string> applied;
    uint64_t snap_index{0};
    uint64_t snap_term{0};
    std::string snap_blob;
    int snap_apply_calls{0};
    bool snap_fail_next{false};
  };

 private:
  struct Held {
    std::string from;
    std::string to;
    std::string type;
    std::string payload;
  };

  bool blocked(const std::string& from, const std::string& to) const {
    return isolated_.count(from) != 0 || isolated_.count(to) != 0;
  }

  void deliver(const std::string& from, const std::string& to, const std::string& type,
               const std::string& payload) {
    if (!hold_to_.empty() && to == hold_to_ && type == "AppendEntries") {
      held_.push_back(Held{from, to, type, payload});
      return;
    }
    if (delay_ms_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
    }
    deliver_now(from, to, type, payload);
    if (duplicate_ && (type == "AppendEntries" || type == "RequestVote" || type == "RequestPreVote")) {
      deliver_now(from, to, type, payload);
    }
  }

  void deliver_now(const std::string& from, const std::string& to, const std::string& type,
                   const std::string& payload) {
    if (blocked(from, to) || drop_types_.count(type)) {
      ++dropped_;
      if (type == "AppendEntries" || type == "InstallSnapshot") {
        auto sit = nodes_.find(from);
        if (sit != nodes_.end()) sit->second->node->on_rpc_transport_fail(to);
      }
      return;
    }
    auto dit = nodes_.find(to);
    auto sit = nodes_.find(from);
    if (dit == nodes_.end() || sit == nodes_.end()) return;

    RaftNode& dst = *dit->second->node;
    RaftNode& src = *sit->second->node;
    const auto parts = split_pipe(payload);

    try {
      if (type == "RequestVote" || type == "RequestPreVote") {
        if (parts.size() < 3) return;
        const uint64_t term = std::stoull(parts[0]);
        const uint64_t last_idx = std::stoull(parts[1]);
        const uint64_t last_term = std::stoull(parts[2]);
        bool granted = false;
        if (type == "RequestPreVote") {
          dst.on_request_pre_vote(from, term, last_idx, last_term, &granted);
          if (!drop_replies_) src.on_request_pre_vote_reply(to, dst.soft_state().term, granted);
        } else {
          dst.on_request_vote(from, term, last_idx, last_term, &granted);
          if (!drop_replies_) src.on_request_vote_reply(to, dst.soft_state().term, granted);
        }
        return;
      }
      if (type == "AppendEntries") {
        if (parts.size() < 5) return;
        const uint64_t term = std::stoull(parts[0]);
        const uint64_t prev_idx = std::stoull(parts[1]);
        const uint64_t prev_term = std::stoull(parts[2]);
        const uint64_t leader_commit = std::stoull(parts[3]);
        const uint64_t nent = std::stoull(parts[4]);
        std::vector<LogEntry> entries;
        size_t off = 5;
        for (uint64_t i = 0; i < nent; ++i) {
          if (off + 2 >= parts.size()) return;
          LogEntry e;
          e.term = std::stoull(parts[off]);
          e.index = std::stoull(parts[off + 1]);
          if (!openkv::from_hex(parts[off + 2], &e.data)) return;
          entries.push_back(std::move(e));
          off += 3;
        }
        bool success = false;
        uint64_t match = 0;
        dst.on_append_entries(from, term, prev_idx, prev_term, entries, leader_commit, &success,
                              &match);
        if (drop_replies_) {
          src.on_rpc_transport_fail(to);
          ++dropped_;
          return;
        }
        src.on_append_entries_reply(to, dst.soft_state().term, success, match);
        return;
      }
      if (type == "InstallSnapshot") {
        if (parts.size() < 4) return;
        const uint64_t term = std::stoull(parts[0]);
        const uint64_t last_idx = std::stoull(parts[1]);
        const uint64_t last_term = std::stoull(parts[2]);
        std::string data;
        if (!openkv::from_hex(parts[3], &data)) return;
        bool success = false;
        dst.on_install_snapshot(from, term, last_idx, last_term, data, &success);
        if (drop_replies_) {
          src.on_rpc_transport_fail(to);
          ++dropped_;
          return;
        }
        src.on_install_snapshot_reply(to, dst.soft_state().term, success, last_idx);
        return;
      }
    } catch (...) {
      ++dropped_;
    }
  }

  ClusterOpts opts_;
  std::unordered_map<std::string, std::unique_ptr<Slot>> nodes_;
  std::unordered_set<std::string> isolated_;
  std::unordered_set<std::string> drop_types_;
  bool drop_replies_{false};
  bool duplicate_{false};
  int delay_ms_{0};
  std::string hold_to_;
  std::vector<Held> held_;
  int dropped_{0};
};

}  // namespace

// ——— A 选主 ———

TEST(RaftFaultScenario, A1_ThreeNodeElectsAndReplicates) {
  RaftFaultCluster c;
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  ASSERT_TRUE(c.at(leader).propose("hello"));
  EXPECT_TRUE(c.wait_applied("a", "hello"));
  EXPECT_TRUE(c.wait_applied("b", "hello"));
  EXPECT_TRUE(c.wait_applied("c", "hello"));
}

TEST(RaftFaultScenario, A2_NeverTwoLeadersStable) {
  RaftFaultCluster c;
  for (int round = 0; round < 50; ++round) {
    c.tick_all(1);
    EXPECT_LE(c.leader_ids().size(), 1u);
  }
  ASSERT_FALSE(c.wait_one_leader(80).empty());
  for (int round = 0; round < 30; ++round) {
    c.tick_all(1);
    EXPECT_EQ(c.leader_ids().size(), 1u);
  }
}

TEST(RaftFaultScenario, A3_MinorityPartitionCannotElect) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  c.isolate("a");
  c.at("a").force_election();
  c.tick_ids({"a"}, 30);
  EXPECT_FALSE(c.at("a").is_leader());
  std::string maj;
  for (int i = 0; i < 200; ++i) {
    c.tick_ids({"b", "c"}, 1);
    if (c.at("b").is_leader()) {
      maj = "b";
      break;
    }
    if (c.at("c").is_leader()) {
      maj = "c";
      break;
    }
  }
  ASSERT_FALSE(maj.empty());
  ASSERT_TRUE(c.at(maj).propose("only-maj"));
  EXPECT_TRUE(c.wait_applied(maj, "only-maj"));
}

TEST(RaftFaultScenario, A4_PreVoteDoesNotBumpTerm) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const uint64_t term_before = c.at("b").soft_state().term;
  bool granted = false;
  c.at("b").on_request_pre_vote("a", term_before + 1, 0, 0, &granted);
  EXPECT_TRUE(granted);
  EXPECT_EQ(c.at("b").soft_state().term, term_before);
}

TEST(RaftFaultScenario, A5_RejectVoteWhileLeaderFresh) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  // Follower 刚听到 Leader：拒 PreVote；同任期 Vote 也拒（更高 term 的 Vote 会先 step down）
  std::string follower = c.at("a").is_leader() ? "b" : "a";
  EXPECT_FALSE(c.at(follower).soft_state().leader_id.empty());
  bool granted = true;
  const uint64_t t = c.at(follower).soft_state().term;
  c.at(follower).on_request_pre_vote("z", t + 1, 100, 1, &granted);
  EXPECT_FALSE(granted);
  granted = true;
  c.at(follower).on_request_vote("z", t, 100, 1, &granted);
  EXPECT_FALSE(granted);
}

TEST(RaftFaultScenario, A6_AfterTimeoutCanGrantPreVote) {
  ClusterOpts o;
  o.check_quorum = false;
  o.election_timeout_ms = 80;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  std::string follower = c.at("a").is_leader() ? "b" : "a";
  // 只 tick follower 直到选举超时清 leader_id
  for (int i = 0; i < 80; ++i) {
    c.at(follower).tick();
    if (c.at(follower).soft_state().leader_id.empty()) break;
  }
  EXPECT_TRUE(c.at(follower).soft_state().leader_id.empty());
  bool granted = false;
  c.at(follower).on_request_pre_vote("z", c.at(follower).soft_state().term + 1, 0, 0, &granted);
  EXPECT_TRUE(granted);
}

TEST(RaftFaultScenario, A7_ElectWithoutPreVote) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
}

TEST(RaftFaultScenario, A8_HigherTermStepsDownLeader) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  const uint64_t term = c.at(leader).soft_state().term;
  bool ok = false;
  uint64_t match = 0;
  c.at(leader).on_append_entries("b", term + 5, 0, 0, {}, 0, &ok, &match);
  EXPECT_FALSE(c.at(leader).is_leader());
  EXPECT_EQ(c.at(leader).soft_state().term, term + 5);
  EXPECT_EQ(c.at(leader).soft_state().role, Role::kFollower);
}

TEST(RaftFaultScenario, A9_AbdicateAndForceElection) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  ASSERT_TRUE(c.at(leader).abdicate());
  EXPECT_FALSE(c.at(leader).is_leader());
  // 另一节点强制选主
  std::string other = leader == "a" ? "b" : "a";
  ASSERT_TRUE(c.at(other).force_election());
  // 喂票
  for (const char* id : {"a", "b", "c"}) {
    if (id == other) continue;
    bool g = false;
    c.at(id).on_request_vote(other, c.at(other).soft_state().term, c.at(other).last_index(),
                             c.at(other).soft_state().term, &g);
    c.at(other).on_request_vote_reply(id, c.at(other).soft_state().term, g);
  }
  EXPECT_TRUE(c.at(other).is_leader() || c.wait_one_leader(100) == other ||
              !c.wait_one_leader(100).empty());
}

TEST(RaftFaultScenario, A10_LearnerCannotForceElection) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  o.peers = {{"a", "a", false}, {"b", "b", false}, {"l", "l", true}};
  RaftFaultCluster c(o);
  EXPECT_TRUE(c.at("l").self_is_learner());
  EXPECT_FALSE(c.at("l").force_election());
  ASSERT_FALSE(c.wait_one_leader(200).empty());
  EXPECT_FALSE(c.at("l").is_leader());
}

TEST(RaftFaultScenario, A11_SplitVoteEventuallyOneLeader) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  o.election_timeout_ms = 100;
  RaftFaultCluster c(o);
  // 多轮空转后仍应收敛到一名 Leader
  for (int i = 0; i < 300; ++i) {
    c.tick_all(1);
    if (c.leader_ids().size() == 1) break;
  }
  EXPECT_EQ(c.leader_ids().size(), 1u);
}

// ——— B 复制 ———

TEST(RaftFaultScenario, B2_IsolateFollowerMajorityStillWrites) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  c.isolate(victim);
  ASSERT_TRUE(c.at(leader).propose("maj"));
  EXPECT_TRUE(c.wait_applied(leader, "maj"));
  EXPECT_FALSE(c.has_applied(victim, "maj"));
}

TEST(RaftFaultScenario, B3_DropAppendEntriesBlocksCommitThenRecovers) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.drop_type("AppendEntries");
  ASSERT_TRUE(c.at(leader).propose("blocked"));
  c.tick_ids({leader}, 25);
  EXPECT_FALSE(c.has_applied("a", "blocked"));
  EXPECT_FALSE(c.has_applied("b", "blocked"));
  EXPECT_FALSE(c.has_applied("c", "blocked"));
  c.allow_type("AppendEntries");
  const std::string leader2 = c.wait_one_leader(240);
  ASSERT_FALSE(leader2.empty());
  ASSERT_TRUE(c.at(leader2).propose("after"));
  EXPECT_TRUE(c.wait_applied("a", "after", 160));
  EXPECT_TRUE(c.wait_applied("b", "after", 160));
  EXPECT_TRUE(c.wait_applied("c", "after", 160));
}

TEST(RaftFaultScenario, B4_DropAeRepliesBlocksCommit) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.set_drop_replies(true);
  ASSERT_TRUE(c.at(leader).propose("noreply"));
  c.tick_ids({leader}, 20);
  EXPECT_FALSE(c.has_applied(leader, "noreply"));
  c.set_drop_replies(false);
  const std::string leader2 = c.wait_one_leader(240);
  ASSERT_FALSE(leader2.empty());
  ASSERT_TRUE(c.at(leader2).propose("ok"));
  EXPECT_TRUE(c.wait_applied("a", "ok", 160));
}

TEST(RaftFaultScenario, B5_AeConflictThenCatchUp) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  // 给受害者注入冲突日志（假 Leader、未提交）
  std::vector<LogEntry> bogus{{99, 1, "bogus"}};
  bool ok = false;
  uint64_t match = 0;
  c.at(victim).on_append_entries("evil", /*term=*/1, 0, 0, bogus, 0, &ok, &match);
  // 真 Leader 写入并愈合复制
  ASSERT_TRUE(c.at(leader).propose("real"));
  EXPECT_TRUE(c.wait_applied(leader, "real", 120));
  EXPECT_TRUE(c.wait_applied(victim, "real", 160));
  EXPECT_FALSE(c.has_applied(victim, "bogus"));
}

TEST(RaftFaultScenario, B6_HeldAeReleasedReversedStillConsistent) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  c.hold_to(victim);
  ASSERT_TRUE(c.at(leader).propose("p1"));
  ASSERT_TRUE(c.at(leader).propose("p2"));
  c.tick_ids({leader}, 5);
  c.release_held_reversed();
  c.tick_all(40);
  EXPECT_TRUE(c.wait_applied(victim, "p1", 80));
  EXPECT_TRUE(c.wait_applied(victim, "p2", 80));
}

TEST(RaftFaultScenario, B7_RejectStaleTermAe) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  // 抬高 b 的 term
  bool ok = false;
  uint64_t match = 0;
  c.at("b").on_append_entries("x", 10, 0, 0, {}, 0, &ok, &match);
  EXPECT_EQ(c.at("b").soft_state().term, 10u);
  ok = true;
  c.at("b").on_append_entries("a", /*term=*/3, 0, 0, {}, 0, &ok, &match);
  EXPECT_FALSE(ok);
}

// ——— C Quorum / 读 ———

TEST(RaftFaultScenario, C1_IsolatedLeaderCheckQuorumStepsDown) {
  RaftFaultCluster c;
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.isolate(leader);
  bool stepped = false;
  for (int i = 0; i < 120; ++i) {
    c.at(leader).tick();
    if (!c.at(leader).is_leader()) {
      stepped = true;
      break;
    }
  }
  EXPECT_TRUE(stepped);
}

TEST(RaftFaultScenario, C2_QuorumGraceKeepsNewLeader) {
  RaftFaultCluster c;
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.isolate("a");
  c.isolate("b");
  c.isolate("c");
  // 刚当选后的前几拍：即使全隔离也不应立刻让位（grace）
  bool still = true;
  for (int i = 0; i < 3; ++i) {
    c.at(leader).tick();
    if (!c.at(leader).is_leader()) still = false;
  }
  EXPECT_TRUE(still);
}

TEST(RaftFaultScenario, C3_OnePeerDownLeaderStays) {
  ClusterOpts o;
  o.check_quorum = true;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  c.isolate(victim);
  for (int i = 0; i < 40; ++i) {
    for (const char* id : {"a", "b", "c"}) {
      if (std::string(id) == victim) continue;
      c.at(id).tick();
    }
  }
  EXPECT_TRUE(c.at(leader).is_leader());
  ASSERT_TRUE(c.at(leader).propose("keep"));
  EXPECT_TRUE(c.wait_applied(leader, "keep"));
}

TEST(RaftFaultScenario, C4_ReadIndexFailsWithoutQuorumAck) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_lease_read = false;  // 关掉 lease 快路径，强制走 ReadIndex 等多数 ACK
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.isolate("a");
  c.isolate("b");
  c.isolate("c");
  uint64_t idx = 0;
  EXPECT_FALSE(c.at(leader).read_index(&idx, /*timeout_ms=*/40));
}

TEST(RaftFaultScenario, C5_LeaseReadOnLeader) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_lease_read = true;
  o.leader_lease_ms = 5000;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  // 心跳续 lease
  c.tick_all(5);
  uint64_t idx = 0;
  EXPECT_TRUE(c.at(leader).read_index(&idx, 100));
}

// ——— D 愈合 ———

TEST(RaftFaultScenario, D1_HealPartitionFollowerCatchesUp) {
  ClusterOpts o;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  c.isolate(victim);
  ASSERT_TRUE(c.at(leader).propose("before-heal"));
  c.tick_ids(leader == "a" ? std::vector<std::string>{"a", "b"} : std::vector<std::string>{"b", "c"},
             40);
  ASSERT_TRUE(c.has_applied(leader, "before-heal"));
  c.heal(victim);
  EXPECT_TRUE(c.wait_applied(victim, "before-heal", 160));
}

TEST(RaftFaultScenario, D2_LeaderChangeDuringPartitionThenHeal) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string old = c.wait_one_leader();
  ASSERT_FALSE(old.empty());
  c.isolate(old);
  std::string neu;
  for (int i = 0; i < 250; ++i) {
    for (const char* id : {"a", "b", "c"}) {
      if (std::string(id) == old) continue;
      c.at(id).tick();
    }
    auto ls = c.leader_ids();
    for (const auto& L : ls) {
      if (L != old) {
        neu = L;
        break;
      }
    }
    if (!neu.empty()) break;
  }
  ASSERT_FALSE(neu.empty());
  ASSERT_TRUE(c.at(neu).propose("from-new"));
  c.tick_ids({neu, neu == "a" ? "b" : "a"}, 40);
  ASSERT_TRUE(c.has_applied(neu, "from-new"));
  c.heal(old);
  EXPECT_TRUE(c.wait_applied(old, "from-new", 200));
}

TEST(RaftFaultScenario, D3_DropClearsInflightSoHealCanSend) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.drop_type("AppendEntries");
  ASSERT_TRUE(c.at(leader).propose("x"));
  c.tick_ids({leader}, 10);
  EXPECT_GT(c.dropped(), 0);
  c.allow_type("AppendEntries");
  ASSERT_TRUE(c.at(leader).propose("y"));
  EXPECT_TRUE(c.wait_applied("b", "y", 120));
}

TEST(RaftFaultScenario, D4_InstallSnapshotToLaggingFollower) {
  // 单节点接收快照（与 RaftPersist 对齐）；三节点发 Snap 依赖 snap_provider 与 compact
  bool ok = false;
  std::string got;
  RaftNode::Options opts;
  opts.external_io = false;
  opts.self_id = "n1";
  opts.peers = {{"n0", "n0", false}, {"n1", "n1", false}};
  opts.check_quorum = false;
  auto applier = [&](uint64_t, uint64_t, std::string_view d) {
    got.assign(d);
    return true;
  };
  RaftNode n(opts, nullptr, [](auto&&...) {}, nullptr, applier);
  n.on_install_snapshot("n0", 1, 50, 1, "snap-blob", &ok);
  EXPECT_TRUE(ok);
  EXPECT_EQ(got, "snap-blob");
  EXPECT_EQ(n.soft_state().commit_index, 50u);
}

TEST(RaftFaultScenario, D5_InstallSnapshotFailThenRetry) {
  int calls = 0;
  RaftNode::Options opts;
  opts.external_io = false;
  opts.self_id = "n1";
  opts.peers = {{"n0", "n0", false}, {"n1", "n1", false}};
  opts.check_quorum = false;
  auto applier = [&](uint64_t, uint64_t, std::string_view) {
    ++calls;
    return calls >= 2;
  };
  RaftNode n(opts, nullptr, [](auto&&...) {}, nullptr, applier);
  bool ok = true;
  n.on_install_snapshot("n0", 1, 10, 1, "x", &ok);
  EXPECT_FALSE(ok);
  ok = false;
  n.on_install_snapshot("n0", 1, 10, 1, "x", &ok);
  EXPECT_TRUE(ok);
  EXPECT_EQ(calls, 2);
}

// ——— E Joint ———

TEST(RaftFaultScenario, E1_PromoteEnterLeaveJoint) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  o.peers = {{"a", "a", false}};
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  ConfChange add;
  add.type = ConfChange::kAddLearner;
  add.peer = {"b", "b", true};
  ASSERT_TRUE(c.at("a").propose_conf_change(add));
  c.tick_all(10);
  ConfChange prom;
  prom.type = ConfChange::kPromoteLearner;
  prom.peer = {"b", "b", false};
  ASSERT_TRUE(c.at("a").propose_conf_change(prom));
  c.tick_all(5);
  ASSERT_TRUE(c.at("a").in_joint());
  c.at("a").on_append_entries_reply("b", c.at("a").soft_state().term, true, c.at("a").last_index());
  c.tick_all(40);
  EXPECT_FALSE(c.at("a").in_joint());
}

TEST(RaftFaultScenario, E2_RejectConfWhileJoint) {
  ClusterOpts o;
  o.check_quorum = false;
  o.peers = {{"a", "a", false}};
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  ConfChange add;
  add.type = ConfChange::kAddLearner;
  add.peer = {"b", "b", true};
  ASSERT_TRUE(c.at("a").propose_conf_change(add));
  c.tick_all(10);
  ConfChange prom;
  prom.type = ConfChange::kPromoteLearner;
  prom.peer = {"b", "b", false};
  ASSERT_TRUE(c.at("a").propose_conf_change(prom));
  c.tick_all(3);
  ASSERT_TRUE(c.at("a").in_joint());
  EXPECT_FALSE(c.at("a").propose_conf_change(prom));
  c.at("a").on_append_entries_reply("b", c.at("a").soft_state().term, true, c.at("a").last_index());
  c.tick_all(40);
  EXPECT_FALSE(c.at("a").in_joint());
}

TEST(RaftFaultScenario, E3_JointStaysWhenMajorityPartitioned) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  ConfChange rm;
  rm.type = ConfChange::kRemoveNode;
  rm.peer = {"c", "c", false};
  ASSERT_TRUE(c.at(leader).propose_conf_change(rm));
  // propose 路径已 commit/apply Enter；Leave 要等后续 tick 才 propose
  ASSERT_TRUE(c.at(leader).in_joint());
  c.drop_type("AppendEntries");
  std::string other = leader == "a" ? "b" : "a";
  c.isolate(other);
  c.isolate("c");
  c.tick_ids({leader}, 50);
  EXPECT_TRUE(c.at(leader).in_joint());
}

TEST(RaftFaultScenario, E4_RemoveNodeJointWithVictimIsolated) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  c.isolate("c");
  ConfChange rm;
  rm.type = ConfChange::kRemoveNode;
  rm.peer = {"c", "c", false};
  ASSERT_TRUE(c.at(leader).propose_conf_change(rm));
  // 喂 b 的复制水位以便 Leave
  for (int i = 0; i < 50; ++i) {
    c.tick_ids({leader, "a", "b"}, 1);
    if (!c.at(leader).in_joint()) break;
    c.at(leader).on_append_entries_reply("b", c.at(leader).soft_state().term, true,
                                         c.at(leader).last_index());
  }
  // 即使 c 隔离，a+b 作为 C_new 多数也可 Leave
  EXPECT_FALSE(c.at(leader).in_joint());
  bool has_c = false;
  for (const auto& p : c.at(leader).peers()) {
    if (p.id == "c" && !p.learner) has_c = true;
  }
  EXPECT_FALSE(has_c);
}

TEST(RaftFaultScenario, E5_AddLearnerUnderFollowerIsolate) {
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string victim = leader == "a" ? "c" : "a";
  c.isolate(victim);
  ConfChange add;
  add.type = ConfChange::kAddLearner;
  add.peer = {"nL", "nL", true};
  ASSERT_TRUE(c.at(leader).propose_conf_change(add));
  c.tick_ids({leader, leader == "b" ? "a" : "b"}, 20);
  bool has = false;
  for (const auto& p : c.at(leader).peers()) {
    if (p.id == "nL" && p.learner) has = true;
  }
  EXPECT_TRUE(has);
}

// ——— F 持久化 ———

TEST(RaftFaultScenario, F1_RestartRecoversHardState) {
  const auto dir = fs::temp_directory_path() /
                   ("okv_fault_f1_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<openkv::raft::FileRaftEngine>(dir.string(),
                                                               openkv::raft::SyncMode::kNone);
  {
    RaftNode::Options opts;
    opts.external_io = false;
    opts.self_id = "n0";
    opts.peers = {{"n0", "n0", false}};
    opts.raft_engine = engine;
    opts.raft_group_id = 1;
    opts.check_quorum = false;
    opts.persist_dir = dir.string();
    std::vector<std::string> applied;
    RaftNode n(opts, [&](uint64_t, const std::string& d) { applied.push_back(d); },
               [](auto&&...) {});
    for (int i = 0; i < 40 && !n.is_leader(); ++i) n.tick();
    ASSERT_TRUE(n.is_leader());
    ASSERT_TRUE(n.propose("persisted"));
    EXPECT_FALSE(applied.empty());
  }
  {
    RaftNode::Options opts;
    opts.external_io = false;
    opts.self_id = "n0";
    opts.peers = {{"n0", "n0", false}};
    opts.raft_engine = engine;
    opts.raft_group_id = 1;
    opts.check_quorum = false;
    opts.persist_dir = dir.string();
    std::vector<std::string> applied;
    RaftNode n(opts, [&](uint64_t, const std::string& d) { applied.push_back(d); },
               [](auto&&...) {});
    EXPECT_GE(n.soft_state().commit_index, 1u);
    n.recover_apply();
  }
  fs::remove_all(dir);
}

TEST(RaftFaultScenario, F2_UncommittedLostAfterLeaderCrash) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  ASSERT_TRUE(c.at(leader).propose("committed"));
  EXPECT_TRUE(c.wait_applied("a", "committed", 80));
  EXPECT_TRUE(c.wait_applied("b", "committed", 80));

  // 丢掉 AE：下一条无法多数提交
  c.drop_type("AppendEntries");
  ASSERT_TRUE(c.at(leader).propose("uncommitted"));
  c.tick_ids({leader}, 10);
  EXPECT_FALSE(c.has_applied("b", "uncommitted"));

  // 「撞毁」Leader：隔离它，其余选主并提交新值（覆盖未提交）
  c.isolate(leader);
  c.allow_type("AppendEntries");
  std::string neu;
  for (int i = 0; i < 250; ++i) {
    for (const char* id : {"a", "b", "c"}) {
      if (std::string(id) == leader) continue;
      c.at(id).tick();
    }
    for (const auto& L : c.leader_ids()) {
      if (L != leader) {
        neu = L;
        break;
      }
    }
    if (!neu.empty()) break;
  }
  ASSERT_FALSE(neu.empty());
  ASSERT_TRUE(c.at(neu).propose("after-crash"));
  EXPECT_TRUE(c.wait_applied(neu, "after-crash", 120));
  // 已提交的仍在
  EXPECT_TRUE(c.has_applied(neu, "committed"));
}

TEST(RaftFaultScenario, F3_PersistNoneIgnoresMustSync) {
  const auto dir = fs::temp_directory_path() /
                   ("okv_fault_f3_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<openkv::raft::FileRaftEngine>(dir.string(),
                                                               openkv::raft::SyncMode::kNone);
  RaftNode::Options opts;
  opts.external_io = true;
  opts.self_id = "n0";
  opts.peers = {{"n0", "n0", false}};
  opts.raft_engine = engine;
  opts.raft_group_id = 1;
  opts.check_quorum = false;
  opts.persist_sync = openkv::raft::SyncMode::kNone;
  RaftNode n(opts, nullptr, nullptr);
  for (int i = 0; i < 40 && !n.is_leader(); ++i) n.tick();
  ASSERT_TRUE(n.is_leader());
  uint64_t idx = 0;
  ASSERT_TRUE(n.propose("x", &idx));
  ASSERT_TRUE(n.has_ready());
  auto rd = n.take_ready();
  const auto t0 = std::chrono::steady_clock::now();
  n.persist_ready(rd);
  const auto us =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  EXPECT_LT(us, 50'000);
  fs::remove_all(dir);
}

// ——— G 注入器（配置级，复验） ———

TEST(RaftFaultScenario, G1_FaultInjectDefaultsDisabled) {
  openkv::raft::RaftOutboundFault f;
  EXPECT_FALSE(f.enabled());
}

TEST(RaftFaultScenario, G2_FaultInjectDropAeOnly) {
  openkv::raft::RaftOutboundFault f;
  f.drop_ae_pct = 100;
  EXPECT_TRUE(f.should_drop("AppendEntries", 0));
  EXPECT_FALSE(f.should_drop("RequestVote", 0));
}

// ——— H 本机 Raft「集群」故障（内存多节点，不依赖 OpenKV 进程） ———

TEST(RaftFaultScenario, H1_LoseTwoReplicasBlocksCommit) {
  // 近似 it-partition：RF=3 丢 2 副本 → 写无法提交
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  for (const char* id : {"a", "b", "c"}) {
    if (std::string(id) != leader) c.isolate(id);
  }
  ASSERT_TRUE(c.at(leader).propose("no-quorum"));
  c.tick_ids({leader}, 30);
  EXPECT_FALSE(c.has_applied(leader, "no-quorum"));
  // 恢复 1 个 Follower → 多数派可写
  std::string peer = leader == "a" ? "b" : "a";
  c.heal(peer);
  c.tick_ids({leader, peer}, 40);
  ASSERT_TRUE(c.at(leader).propose("quorum-ok"));
  EXPECT_TRUE(c.wait_applied(leader, "quorum-ok", 80));
  EXPECT_TRUE(c.wait_applied(peer, "quorum-ok", 80));
}

TEST(RaftFaultScenario, H2_EngineDirReadonlyRejectsPersist) {
  // 近似磁盘只读：引擎目录不可写时 consume/落盘失败（不崩进程）
  const auto dir = fs::temp_directory_path() /
                   ("okv_fault_h2_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<openkv::raft::FileRaftEngine>(dir.string(),
                                                               openkv::raft::SyncMode::kNone);
  RaftNode::Options opts;
  opts.external_io = false;
  opts.self_id = "n0";
  opts.peers = {{"n0", "n0", false}};
  opts.raft_engine = engine;
  opts.raft_group_id = 1;
  opts.check_quorum = false;
  opts.persist_dir = dir.string();
  RaftNode n(opts, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  for (int i = 0; i < 40 && !n.is_leader(); ++i) n.tick();
  ASSERT_TRUE(n.is_leader());
  fs::permissions(dir, fs::perms::owner_read | fs::perms::owner_exec |
                           fs::perms::group_read | fs::perms::group_exec |
                           fs::perms::others_read | fs::perms::others_exec,
                  fs::perm_options::replace);
  // 再 propose：应不崩；落盘可能失败，进程仍存活
  (void)n.propose("after-ro");
  for (int i = 0; i < 5; ++i) n.tick();
  SUCCEED();
  fs::permissions(dir, fs::perms::all, fs::perm_options::replace);
  fs::remove_all(dir);
}

TEST(RaftFaultScenario, H3_DeliveryDelayStillElectsAndReplicates) {
  // 近似网卡抖动：每条 RPC 人为延迟，选主与复制仍收敛
  ClusterOpts o;
  o.check_quorum = false;
  o.enable_pre_vote = false;
  RaftFaultCluster c(o);
  c.set_delay_ms(2);
  const auto t0 = std::chrono::steady_clock::now();
  const std::string leader = c.wait_one_leader(300);
  ASSERT_FALSE(leader.empty());
  ASSERT_TRUE(c.at(leader).propose("delayed"));
  EXPECT_TRUE(c.wait_applied("a", "delayed", 200));
  EXPECT_TRUE(c.wait_applied("b", "delayed", 200));
  EXPECT_TRUE(c.wait_applied("c", "delayed", 200));
  const auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  EXPECT_GE(ms, 2);
}

TEST(RaftFaultScenario, H4_LocalMatrixSmoke) {
  // 本机矩阵冒烟：丢 1 可写 + 丢 2 不可写（串在同一集群生命周期）
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string leader = c.wait_one_leader();
  ASSERT_FALSE(leader.empty());
  std::string one = leader == "a" ? "c" : "a";
  c.isolate(one);
  ASSERT_TRUE(c.at(leader).propose("lose-one"));
  EXPECT_TRUE(c.wait_applied(leader, "lose-one", 80));
  for (const char* id : {"a", "b", "c"}) {
    if (std::string(id) == leader || std::string(id) == one) continue;
    c.isolate(id);
  }
  ASSERT_TRUE(c.at(leader).propose("lose-two"));
  c.tick_ids({leader}, 20);
  EXPECT_FALSE(c.has_applied(leader, "lose-two"));
}

// ——— X 意外 / 组合 / 多轮混沌 ———

TEST(RaftFaultScenario, X1_FlapIsolateFollowerManyRounds) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  for (int r = 0; r < 25; ++r) {
    c.isolate("c");
    c.tick_all(2);
    EXPECT_LE(c.leader_ids().size(), 1u) << "round=" << r;
    c.heal("c");
    c.tick_all(3);
    EXPECT_LE(c.leader_ids().size(), 1u) << "round=" << r;
  }
  const std::string L = c.wait_one_leader(120);
  ASSERT_FALSE(L.empty());
  ASSERT_TRUE(c.at(L).propose("after-flap"));
  EXPECT_TRUE(c.wait_applied("a", "after-flap", 100));
  EXPECT_TRUE(c.wait_applied("b", "after-flap", 100));
  EXPECT_TRUE(c.wait_applied("c", "after-flap", 100));
}

TEST(RaftFaultScenario, X2_OscillateDropAeManyRounds) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  for (int r = 0; r < 20; ++r) {
    if (r % 2 == 0) {
      c.drop_type("AppendEntries");
    } else {
      c.allow_type("AppendEntries");
    }
    c.tick_all(4);
    EXPECT_LE(c.leader_ids().size(), 1u) << "round=" << r;
  }
  c.allow_type("AppendEntries");
  const std::string L = c.wait_one_leader(200);
  ASSERT_FALSE(L.empty());
  ASSERT_TRUE(c.at(L).propose("ae-osc"));
  EXPECT_TRUE(c.wait_applied("a", "ae-osc", 160));
  EXPECT_TRUE(c.wait_applied("b", "ae-osc", 160));
  EXPECT_TRUE(c.wait_applied("c", "ae-osc", 160));
}

TEST(RaftFaultScenario, X3_DualForceElectionStorm) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  for (int r = 0; r < 15; ++r) {
    c.at("a").force_election();
    c.at("b").force_election();
    c.tick_all(5);
    EXPECT_LE(c.leader_ids().size(), 1u) << "round=" << r;
  }
  ASSERT_FALSE(c.wait_one_leader(200).empty());
}

TEST(RaftFaultScenario, X4_AbdicateLoopRecover) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  for (int r = 0; r < 10; ++r) {
    const std::string L = c.wait_one_leader(150);
    ASSERT_FALSE(L.empty()) << "round=" << r;
    ASSERT_TRUE(c.at(L).abdicate());
    c.tick_all(8);
    EXPECT_LE(c.leader_ids().size(), 1u);
  }
  ASSERT_FALSE(c.wait_one_leader(200).empty());
}

TEST(RaftFaultScenario, X5_DelayPlusIsolateCombined) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  c.set_delay_ms(1);
  const std::string L = c.wait_one_leader(300);
  ASSERT_FALSE(L.empty());
  c.isolate("c");
  ASSERT_TRUE(c.at(L).propose("slow-maj"));
  EXPECT_TRUE(c.wait_applied(L, "slow-maj", 120));
  c.heal("c");
  EXPECT_TRUE(c.wait_applied("c", "slow-maj", 200));
}

TEST(RaftFaultScenario, X6_DuplicateRpcDelivery) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  c.set_duplicate_delivery(true);
  const std::string L = c.wait_one_leader(250);
  ASSERT_FALSE(L.empty());
  ASSERT_TRUE(c.at(L).propose("dup"));
  EXPECT_TRUE(c.wait_applied("a", "dup", 120));
  EXPECT_TRUE(c.wait_applied("b", "dup", 120));
  EXPECT_TRUE(c.wait_applied("c", "dup", 120));
  // 不应因重复投递出现双主
  EXPECT_EQ(c.leader_ids().size(), 1u);
}

TEST(RaftFaultScenario, X7_OldLeaderHealedDoesNotStayDual) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string old = c.wait_one_leader();
  ASSERT_FALSE(old.empty());
  c.isolate(old);
  std::string neu;
  for (int i = 0; i < 250; ++i) {
    for (const char* id : {"a", "b", "c"}) {
      if (std::string(id) == old) continue;
      c.at(id).tick();
    }
    for (const auto& x : c.leader_ids()) {
      if (x != old) {
        neu = x;
        break;
      }
    }
    if (!neu.empty()) break;
  }
  ASSERT_FALSE(neu.empty());
  ASSERT_TRUE(c.at(neu).propose("new-era"));
  c.tick_ids({neu, neu == "a" ? "b" : "a"}, 30);
  c.heal(old);
  for (int i = 0; i < 80; ++i) {
    c.tick_all(1);
    EXPECT_LE(c.leader_ids().size(), 1u) << "tick=" << i;
  }
  EXPECT_TRUE(c.wait_applied(old, "new-era", 120));
}

TEST(RaftFaultScenario, X8_DropVotesOnlyAeStillWorks) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string L = c.wait_one_leader();
  ASSERT_FALSE(L.empty());
  c.drop_type("RequestVote");
  c.drop_type("RequestPreVote");
  ASSERT_TRUE(c.at(L).propose("ae-only"));
  EXPECT_TRUE(c.wait_applied("a", "ae-only", 100));
  EXPECT_TRUE(c.wait_applied("b", "ae-only", 100));
  EXPECT_TRUE(c.wait_applied("c", "ae-only", 100));
  EXPECT_EQ(c.leader_ids().size(), 1u);
}

TEST(RaftFaultScenario, X9_AllIsolatedThenFullHeal) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader().empty());
  c.isolate("a");
  c.isolate("b");
  c.isolate("c");
  c.tick_all(20);
  c.heal_all();
  const std::string L = c.wait_one_leader(250);
  ASSERT_FALSE(L.empty());
  ASSERT_TRUE(c.at(L).propose("reunited"));
  EXPECT_TRUE(c.wait_applied("a", "reunited", 160));
  EXPECT_TRUE(c.wait_applied("b", "reunited", 160));
  EXPECT_TRUE(c.wait_applied("c", "reunited", 160));
}

TEST(RaftFaultScenario, X10_RandomChaosManyRounds) {
  // 分区期间旧主可与新区主短暂并存；只在「无隔离」时要求 ≤1，愈合后必须收敛可写。
  ClusterOpts o;
  o.enable_pre_vote = true;
  o.check_quorum = true;
  o.election_timeout_ms = 100;
  o.heartbeat_ms = 30;
  RaftFaultCluster c(o);
  std::mt19937 rng{42};
  std::uniform_int_distribution<int> act(0, 9);
  std::uniform_int_distribution<int> node(0, 2);
  const char* ids[3] = {"a", "b", "c"};

  ASSERT_FALSE(c.wait_one_leader(200).empty());
  for (int r = 0; r < 80; ++r) {
    switch (act(rng)) {
      case 0:
        c.isolate(ids[node(rng)]);
        break;
      case 1:
        c.heal(ids[node(rng)]);
        break;
      case 2:
        c.heal_all();
        break;
      case 3:
        c.drop_type("AppendEntries");
        break;
      case 4:
        c.allow_type("AppendEntries");
        break;
      case 5:
        c.set_drop_replies(act(rng) % 2 == 0);
        break;
      case 6:
        c.set_delay_ms(act(rng) % 2);
        break;
      case 7:
        c.at(ids[node(rng)]).force_election();
        break;
      case 8: {
        auto ls = c.leader_ids();
        if (ls.size() == 1) (void)c.at(ls[0]).propose("chaos-" + std::to_string(r));
        break;
      }
      default:
        c.clear_faults();
        c.heal_all();
        break;
    }
    c.tick_all(3);
    if (!c.any_isolated()) {
      EXPECT_LE(c.leader_ids().size(), 1u) << "chaos round=" << r;
    }
    if (r % 20 == 19) {
      c.clear_faults();
      c.heal_all();
      c.tick_all(40);
      EXPECT_LE(c.leader_ids().size(), 1u) << "heal checkpoint r=" << r;
    }
  }
  c.clear_faults();
  c.heal_all();
  const std::string L = c.wait_one_leader(300);
  ASSERT_FALSE(L.empty());
  ASSERT_TRUE(c.at(L).propose("chaos-final"));
  EXPECT_TRUE(c.wait_applied("a", "chaos-final", 200));
  EXPECT_TRUE(c.wait_applied("b", "chaos-final", 200));
  EXPECT_TRUE(c.wait_applied("c", "chaos-final", 200));
}

TEST(RaftFaultScenario, X11_RandomChaosDifferentSeeds) {
  // 多种子各跑一短轮，抓种子相关意外
  for (uint32_t seed : {1u, 7u, 99u, 12345u, 0xC0FFEEu}) {
    ClusterOpts o;
    o.enable_pre_vote = true;
    o.check_quorum = true;
    RaftFaultCluster c(o);
    std::mt19937 rng{seed};
    std::uniform_int_distribution<int> act(0, 6);
    const char* ids[3] = {"a", "b", "c"};
    ASSERT_FALSE(c.wait_one_leader(200).empty()) << "seed=" << seed;
    for (int r = 0; r < 30; ++r) {
      switch (act(rng)) {
        case 0:
          c.isolate(ids[rng() % 3]);
          break;
        case 1:
          c.heal_all();
          break;
        case 2:
          c.drop_type("AppendEntries");
          break;
        case 3:
          c.allow_type("AppendEntries");
          break;
        case 4:
          c.at(ids[rng() % 3]).force_election();
          break;
        default: {
          auto ls = c.leader_ids();
          if (ls.size() == 1) (void)c.at(ls[0]).abdicate();
          break;
        }
      }
      c.tick_all(3);
      if (!c.any_isolated()) {
        EXPECT_LE(c.leader_ids().size(), 1u) << "seed=" << seed << " r=" << r;
      }
    }
    c.clear_faults();
    c.heal_all();
    EXPECT_FALSE(c.wait_one_leader(250).empty()) << "seed=" << seed;
  }
}

TEST(RaftFaultScenario, X12_DropRepliesThenRecoverMajority) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string L = c.wait_one_leader();
  ASSERT_FALSE(L.empty());
  c.set_drop_replies(true);
  ASSERT_TRUE(c.at(L).propose("no-ack"));
  c.tick_ids({L}, 20);
  EXPECT_FALSE(c.has_applied(L, "no-ack"));
  // 回包恢复后要么原条目追上，要么新主可再写（与 B4 同口径）
  c.set_drop_replies(false);
  const std::string L2 = c.wait_one_leader(240);
  ASSERT_FALSE(L2.empty());
  if (!c.wait_applied("a", "no-ack", 80)) {
    ASSERT_TRUE(c.at(L2).propose("after-ack"));
    EXPECT_TRUE(c.wait_applied("a", "after-ack", 160));
    EXPECT_TRUE(c.wait_applied("b", "after-ack", 160));
    EXPECT_TRUE(c.wait_applied("c", "after-ack", 160));
  } else {
    EXPECT_TRUE(c.wait_applied("b", "no-ack", 120));
    EXPECT_TRUE(c.wait_applied("c", "no-ack", 120));
  }
}

TEST(RaftFaultScenario, X13_IsolateLeaderThenMinorityCannotCommit) {
  ClusterOpts o;
  o.enable_pre_vote = false;
  o.check_quorum = false;
  RaftFaultCluster c(o);
  const std::string L = c.wait_one_leader();
  ASSERT_FALSE(L.empty());
  c.isolate(L);
  ASSERT_TRUE(c.at(L).propose("orphan"));
  c.tick_ids({L}, 40);
  EXPECT_FALSE(c.has_applied(L, "orphan"));
  c.heal(L);
  // 愈合后由真正多数派收敛；旧提案未必已提交，再写一笔确认可写
  const std::string neu = c.wait_one_leader(250);
  ASSERT_FALSE(neu.empty());
  ASSERT_TRUE(c.at(neu).propose("after-orphan"));
  EXPECT_TRUE(c.wait_applied("a", "after-orphan", 160));
  EXPECT_TRUE(c.wait_applied("b", "after-orphan", 160));
  EXPECT_TRUE(c.wait_applied("c", "after-orphan", 160));
}

TEST(RaftFaultScenario, X14_PreVoteOnChaosShort) {
  ClusterOpts o;
  o.enable_pre_vote = true;
  o.check_quorum = true;
  RaftFaultCluster c(o);
  ASSERT_FALSE(c.wait_one_leader(200).empty());
  for (int r = 0; r < 20; ++r) {
    c.isolate("b");
    c.at("c").force_election();
    c.tick_all(4);
    c.heal("b");
    c.tick_all(6);
    if (!c.any_isolated()) {
      EXPECT_LE(c.leader_ids().size(), 1u) << "r=" << r;
    }
  }
  c.heal_all();
  ASSERT_FALSE(c.wait_one_leader(200).empty());
}

TEST(RaftFaultScenario, X15_MultiSeedHealAlwaysWritable) {
  for (uint32_t seed : {3u, 11u, 77u, 888u, 2026u}) {
    ClusterOpts o;
    o.enable_pre_vote = true;
    o.check_quorum = true;
    RaftFaultCluster c(o);
    std::mt19937 rng{seed};
    ASSERT_FALSE(c.wait_one_leader(200).empty()) << seed;
    for (int r = 0; r < 25; ++r) {
      const int a = static_cast<int>(rng() % 5);
      if (a == 0) c.isolate("a");
      else if (a == 1) c.isolate("b");
      else if (a == 2) c.drop_type("AppendEntries");
      else if (a == 3) c.at("c").force_election();
      else {
        c.clear_faults();
        c.heal_all();
      }
      c.tick_all(4);
    }
    c.clear_faults();
    c.heal_all();
    const std::string L = c.wait_one_leader(300);
    ASSERT_FALSE(L.empty()) << seed;
    const std::string payload = "seed-" + std::to_string(seed);
    ASSERT_TRUE(c.at(L).propose(payload));
    EXPECT_TRUE(c.wait_applied("a", payload, 200)) << seed;
    EXPECT_TRUE(c.wait_applied("b", payload, 200)) << seed;
    EXPECT_TRUE(c.wait_applied("c", payload, 200)) << seed;
  }
}
