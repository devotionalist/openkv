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

// @file raw_node.cpp
// RawNode：RaftNode(external_io) 薄封装，对齐 raft-rs Ready/Step/Advance。

#include "opskv/raft/raw_node.hpp"

#include "opskv/raft/raft_node.hpp"

namespace opskv::raft {

struct RawNode::Impl {
  RaftNode node;
  explicit Impl(RaftNode::Options o)
      : node(std::move(o), nullptr, nullptr, nullptr, nullptr) {}
};

RawNode::RawNode(Options opts) {
  RaftNode::Options o;
  o.shard_id = opts.shard_id;
  o.self_id = std::move(opts.self_id);
  o.peers = std::move(opts.peers);
  o.election_timeout_ms = opts.election_timeout_ms;
  o.heartbeat_ms = opts.heartbeat_ms;
  o.compact_threshold = opts.compact_threshold;
  o.snapshot_min_interval_ms = opts.snapshot_min_interval_ms;
  o.enable_pre_vote = opts.enable_pre_vote;
  o.check_quorum = opts.check_quorum;
  o.max_inflight_msgs = opts.max_inflight_msgs;
  o.max_ae_entries = opts.max_ae_entries;
  o.max_ae_bytes = opts.max_ae_bytes;
  o.coalesce_replication = opts.coalesce_replication;
  o.leader_lease_ms = opts.leader_lease_ms;
  o.enable_lease_read = opts.enable_lease_read;
  o.external_io = true;
  o.enable_async_apply = false;
  impl_ = std::make_unique<Impl>(std::move(o));
}

RawNode::~RawNode() = default;
RawNode::RawNode(RawNode&&) noexcept = default;
RawNode& RawNode::operator=(RawNode&&) noexcept = default;

SoftState RawNode::soft_state() const { return impl_->node.soft_state(); }
bool RawNode::is_leader() const { return impl_->node.is_leader(); }
bool RawNode::in_joint() const { return impl_->node.in_joint(); }
bool RawNode::self_is_learner() const { return impl_->node.self_is_learner(); }
uint64_t RawNode::last_index() const { return impl_->node.last_index(); }
uint64_t RawNode::match_index_of(const std::string& peer_id) const {
  return impl_->node.match_index_of(peer_id);
}
const std::vector<Peer>& RawNode::peers() const { return impl_->node.peers(); }
uint32_t RawNode::shard_id() const { return impl_->node.shard_id(); }
std::string RawNode::leader_hint() const { return impl_->node.soft_state().leader_id; }

void RawNode::tick() { impl_->node.tick(); }

bool RawNode::propose(std::string data, uint64_t* assigned_index) {
  return impl_->node.propose(std::move(data), assigned_index);
}

bool RawNode::propose_conf_change(const ConfChange& cc) {
  return impl_->node.propose_conf_change(cc);
}

bool RawNode::read_index(uint64_t* read_commit, uint32_t timeout_ms) {
  return impl_->node.read_index(read_commit, timeout_ms);
}

bool RawNode::abdicate() { return impl_->node.abdicate(); }
bool RawNode::force_election() { return impl_->node.force_election(); }

void RawNode::step_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                                  uint64_t prev_term, const std::vector<LogEntry>& entries,
                                  uint64_t leader_commit, bool* success, uint64_t* match_index) {
  impl_->node.on_append_entries(from, term, prev_idx, prev_term, entries, leader_commit, success,
                                match_index);
}

void RawNode::step_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                uint64_t last_term, bool* granted) {
  impl_->node.on_request_vote(from, term, last_idx, last_term, granted);
}

void RawNode::step_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                    uint64_t last_term, bool* granted) {
  impl_->node.on_request_pre_vote(from, term, last_idx, last_term, granted);
}

void RawNode::step_append_entries_reply(const std::string& from, uint64_t term, bool success,
                                        uint64_t match_index) {
  impl_->node.on_append_entries_reply(from, term, success, match_index);
}

void RawNode::step_request_vote_reply(const std::string& from, uint64_t term, bool granted) {
  impl_->node.on_request_vote_reply(from, term, granted);
}

void RawNode::step_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted) {
  impl_->node.on_request_pre_vote_reply(from, term, granted);
}

void RawNode::step_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                                    uint64_t last_term, std::string_view data, bool* success) {
  impl_->node.on_install_snapshot(from, term, last_index, last_term, data, success);
}

void RawNode::step_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                          uint64_t match_index) {
  impl_->node.on_install_snapshot_reply(from, term, success, match_index);
}

void RawNode::step_transport_fail(const std::string& peer_id) {
  impl_->node.on_rpc_transport_fail(peer_id);
}

void RawNode::add_peer(const Peer& p) { impl_->node.add_peer(p); }
void RawNode::remove_peer(const std::string& id) { impl_->node.remove_peer(id); }

void RawNode::bootstrap_from_disk(uint64_t term, std::string voted_for, uint64_t commit,
                                  uint64_t last_applied, const std::vector<LogEntry>& entries,
                                  uint64_t first_index, uint64_t snap_term) {
  PersistedHardState hs;
  hs.term = term;
  hs.voted_for = std::move(voted_for);
  hs.commit_index = commit;
  hs.last_applied = last_applied;
  hs.first_index = first_index;
  hs.snapshot_term = snap_term;
  impl_->node.load_hard_state(hs, entries);
}

bool RawNode::has_ready() const { return impl_->node.has_ready(); }
Ready RawNode::ready() { return impl_->node.take_ready(); }
void RawNode::advance(const Ready& rd) { impl_->node.advance(rd); }
bool RawNode::consume_activity_flag() { return impl_->node.consume_activity_flag(); }

RaftNode* RawNode::raft_node() { return &impl_->node; }
const RaftNode* RawNode::raft_node() const { return &impl_->node; }

}  // namespace opskv::raft
