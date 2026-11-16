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

// @file raft_node.cpp
// 门面编排：propose/tick；Ready→ReadyPump，apply→Apply，线性读→LinearRead，
// 复制→Replication，落盘→LogPersist。

#include "openkv/raft/raft_node.hpp"

#include "openkv/logging.hpp"

#include <algorithm>
#include <random>
#include <sstream>

namespace openkv::raft {

RaftNode::RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
                   SnapshotProvider snap_provider, SnapshotApplier snap_applier)
    : opts_(std::move(opts)),
      on_apply_(std::move(on_apply)),
      send_(std::move(send)),
      snap_provider_(std::move(snap_provider)),
      snap_applier_(std::move(snap_applier)),
      election_(*this),
      membership_(*this),
      replication_(*this),
      log_persist_(*this),
      apply_(*this),
      linear_read_(*this),
      ready_pump_(*this) {
  if (opts_.max_inflight_msgs == 0) opts_.max_inflight_msgs = 1;
  if (opts_.max_ae_entries == 0) opts_.max_ae_entries = 64;
  if (opts_.max_ae_bytes == 0) opts_.max_ae_bytes = 256 * 1024;
  // external_io：协议不发网/不回调 apply；落盘经 Store persist_fn→persist_ready。
  std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(opts_.election_timeout_ms / 100,
                                          opts_.election_timeout_ms / 50);
  election_.election_timeout_ticks_ = std::max(5, dist(rng));
  for (const auto& p : opts_.peers) {
    progress_.next_index_[p.id] = 1;
    progress_.match_index_[p.id] = 0;
  }
  if (opts_.persist_batch_n == 0) opts_.persist_batch_n = 1;
  group_id_ = opts_.raft_group_id != 0 ? opts_.raft_group_id : opts_.shard_id;
  if (opts_.raft_engine) {
    engine_ = opts_.raft_engine;
  } else if (!opts_.persist_dir.empty()) {
    owned_file_engine_ = std::make_shared<FileRaftEngine>(opts_.persist_dir, opts_.persist_sync);
    engine_ = owned_file_engine_;
  }
  if (engine_) load_from_disk();
  // compact 后日志窗口不含历史：必须先恢复本地 FSM 快照再 recover_apply。
  apply_.restore_local_fsm_snapshot();
}

void RaftNode::load_from_disk() {
  if (!engine_) return;
  PersistedHardState hs;
  std::vector<LogEntry> entries;
  if (!load_group_from_engine(*engine_, group_id_, &hs, &entries)) return;
  term_ = hs.term;
  voted_for_ = hs.voted_for;
  commit_index_ = hs.commit_index;
  last_applied_ = hs.last_applied;
  apply_scheduled_ = hs.last_applied;
  log_.load(hs.first_index, hs.snapshot_term, std::move(entries));
  log_persist_.stable_first_index_ = log_.first_index();
  log_persist_.stable_last_index_ = log_.last_index();
  role_ = Role::kFollower;
  leader_id_.clear();
  SKV_INFO << "shard=" << opts_.shard_id << " group=" << group_id_ << " recovered term=" << term_
           << " commit=" << commit_index_ << " applied=" << last_applied_
           << " last_index=" << log_.last_index();
}

void RaftNode::recover_apply() {
  // on_apply 里再进 recover_apply 会重入 mu_ / 自等 last_applied → 直接跳过。
  if (in_on_apply_.load(std::memory_order_relaxed)) return;
  std::lock_guard<std::mutex> lock(mu_);
  apply_.apply_committed();
  persist_locked();
}

SoftState RaftNode::soft_state() const {
  std::lock_guard<std::mutex> lock(mu_);
  SoftState st = ready_pump_.soft_state_locked();
  if (role_ == Role::kLeader) {
    uint64_t min_match = log_.last_index();
    bool any = false;
    for (const auto& p : opts_.peers) {
      if (p.id == opts_.self_id || p.learner) continue;
      auto it = progress_.match_index_.find(p.id);
      uint64_t m = it == progress_.match_index_.end() ? 0 : it->second;
      // 从未建连成功的 peer（match=0 且 quiet 已爆）不拖 lag，避免僵尸副本误导运维
      auto qit = election_.peer_quiet_ticks_.find(p.id);
      if (m == 0 && qit != election_.peer_quiet_ticks_.end() &&
          qit->second >= election_.election_timeout_ticks_ * 2) {
        continue;
      }
      any = true;
      if (m < min_match) min_match = m;
    }
    if (any) {
      st.replication_lag = log_.last_index() > min_match ? log_.last_index() - min_match : 0;
    }
  }
  return st;
}

bool RaftNode::is_leader() const {
  std::lock_guard<std::mutex> lock(mu_);
  return role_ == Role::kLeader;
}

bool RaftNode::in_joint() const {
  std::lock_guard<std::mutex> lock(mu_);
  return membership_.in_joint_;
}

bool RaftNode::conf_change_busy() const {
  std::lock_guard<std::mutex> lock(mu_);
  return membership_.in_joint_ || membership_.auto_leave_pending_;
}

std::string RaftNode::leader_address() const {
  std::lock_guard<std::mutex> lock(mu_);
  if (leader_id_.empty()) return {};
  for (const auto& p : opts_.peers) {
    if (p.id == leader_id_) return p.address;
  }
  return {};
}

bool RaftNode::self_is_learner() const {
  for (const auto& p : opts_.peers) {
    if (p.id == opts_.self_id) return p.learner;
  }
  return false;
}

uint64_t RaftNode::match_index_of(const std::string& peer_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) return 0;
  if (peer_id == opts_.self_id) return commit_index_;
  auto it = progress_.match_index_.find(peer_id);
  return it == progress_.match_index_.end() ? 0 : it->second;
}

uint64_t RaftNode::last_index() const {
  std::lock_guard<std::mutex> lock(mu_);
  return log_.last_index();
}

bool RaftNode::abdicate() {
  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) return false;
  election_.become_follower(term_, {});
  persist_locked();
  return true;
}

bool RaftNode::force_election() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (self_is_learner()) return false;
    if (role_ == Role::kLeader) return true;
    election_.election_elapsed_ = 0;
    election_.in_pre_vote_ = false;
    election_.pre_votes_.clear();
    election_.become_candidate();
    election_.start_election();
    persist_locked();
    mark_activity();
  }
  ready_pump_.flush_transport();
  return true;
}

bool RaftNode::consume_activity_flag() {
  std::lock_guard<std::mutex> lock(mu_);
  bool v = activity_flag_;
  activity_flag_ = false;
  return v;
}

void RaftNode::mark_activity() { activity_flag_ = true; }

bool RaftNode::read_index(uint64_t* read_commit, uint32_t timeout_ms) {
  return linear_read_.read_index(read_commit, timeout_ms);
}

bool RaftNode::wait_applied(uint64_t index, uint32_t timeout_ms,
                            const std::function<void()>& tick) {
  return apply_.wait_applied(index, timeout_ms, tick);
}

void RaftNode::load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries) {
  std::lock_guard<std::mutex> lock(mu_);
  term_ = hs.term;
  voted_for_ = hs.voted_for;
  commit_index_ = hs.commit_index;
  last_applied_ = hs.last_applied;
  apply_scheduled_ = hs.last_applied;
  log_.load(hs.first_index, hs.snapshot_term, std::vector<LogEntry>(entries));
  log_persist_.stable_first_index_ = log_.first_index();
  log_persist_.stable_last_index_ = log_.last_index();
  role_ = Role::kFollower;
  leader_id_.clear();
  ready_io_.clear_batch();
  ready_pump_.outbound_.clear();
}

// 提业务日志：true 只表示已 append 拿到 index，不等于已落盘/提交/apply。详见头文件。
bool RaftNode::propose(std::string data, uint64_t* assigned_index) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::kLeader) return false;

    const uint64_t idx = log_.append(term_, std::move(data));
    if (opts_.external_io) {
      if (auto e = log_.entry(idx)) ready_io_.ready_unstable_.push_back(*e);
    }
    progress_.match_index_[opts_.self_id] = idx;
    if (assigned_index) *assigned_index = idx;

    persist_locked();
    replication_.need_bcast_ = true;
    // external_io 出站只走 Ready：本拍必须入队 AE，不能等 tick。
    // 嵌入路径才吃 coalesce_replication。
    if (opts_.external_io || !opts_.coalesce_replication || membership_.majority() <= 1) {
      replication_.maybe_broadcast();
    }
    replication_.maybe_commit();
    apply_.apply_committed();
    persist_locked();
  }
  ready_pump_.flush_transport();
  return true;
}

bool RaftNode::propose_conf_change(const ConfChange& cc) {
  ConfChange out = cc;
  if (cc.type == ConfChange::kPromoteLearner || cc.type == ConfChange::kRemoveNode) {
    std::lock_guard<std::mutex> lock(mu_);
    if (membership_.in_joint_ || membership_.auto_leave_pending_) return false;
    out.type = ConfChange::kEnterJoint;
    out.joint_change = cc.type;
  } else if (cc.type == ConfChange::kEnterJoint || cc.type == ConfChange::kLeaveJoint) {
    std::lock_guard<std::mutex> lock(mu_);
    if (cc.type == ConfChange::kEnterJoint &&
        (membership_.in_joint_ || membership_.auto_leave_pending_)) {
      return false;
    }
  }
  std::string payload;
  if (!membership_.encode_conf(out, &payload)) return false;
  return propose(std::move(payload));
}

// 周期时钟：Leader 发心跳/查多数派；Follower 超时则选主。
void RaftNode::tick() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    // 先 apply（ConfChange 加 peer / auto_leave），再心跳，避免新 learner 空等一整拍
    apply_.apply_committed();
    if (role_ == Role::kLeader) {
      membership_.maybe_auto_leave_joint();
      ++election_.heartbeat_elapsed_;
      const bool hb =
          election_.heartbeat_elapsed_ * 100 >= static_cast<int>(opts_.heartbeat_ms);
      if (hb || replication_.need_bcast_) {
        if (hb) election_.heartbeat_elapsed_ = 0;
        replication_.broadcast_heartbeat();
        replication_.need_bcast_ = false;
      }
      for (auto& [id, quiet] : election_.peer_quiet_ticks_) {
        // 快照在途 / 本轮发不出 AE：不涨 quiet（否则本机管道拥塞会被当成网络分区）
        if (progress_.snap_inflight_.count(id) || progress_.peer_send_blocked_.count(id)) continue;
        ++quiet;
      }
      election_.maybe_check_quorum();
    } else if (!self_is_learner()) {
      ++election_.election_elapsed_;
      if (election_.election_elapsed_ >= election_.election_timeout_ticks_) {
        election_.election_elapsed_ = 0;
        // 超时后必须丢掉陈旧 leader_id。否则 on_request_pre_vote 因
        //「仍有 leader 且 elapsed 刚清零」互拒 PreVote，选主永久卡死（it-scale-live）。
        leader_id_.clear();
        if (opts_.enable_pre_vote) {
          election_.start_pre_vote();
        } else {
          election_.become_candidate();
          election_.start_election();
        }
        persist_locked();
        mark_activity();
      }
    }
    log_persist_.persist_maybe_flush(true);  // tick 边界强制 group flush
  }
  ready_pump_.flush_transport();
}

void RaftNode::persist_ready(const Ready& rd) { log_persist_.persist_ready(rd); }

void RaftNode::on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                                 uint64_t prev_term, const std::vector<LogEntry>& entries,
                                 uint64_t leader_commit, bool* success, uint64_t* match_index) {
  std::lock_guard<std::mutex> lock(mu_);
  replication_.on_append_entries(from, term, prev_idx, prev_term, entries, leader_commit, success,
                                 match_index);
}

void RaftNode::on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                                       uint64_t match_index) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    replication_.on_append_entries_reply_locked(from, term, success, match_index);
  }
  ready_pump_.flush_transport();
  ready_pump_.maybe_notify_ready();
}

void RaftNode::on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                                   uint64_t last_term, std::string_view data, bool* success) {
  std::lock_guard<std::mutex> lock(mu_);
  replication_.on_install_snapshot(from, term, last_index, last_term, data, success);
}

void RaftNode::on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                         uint64_t match_index) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    replication_.on_install_snapshot_reply_locked(from, term, success, match_index);
  }
  ready_pump_.flush_transport();
  ready_pump_.maybe_notify_ready();
}

void RaftNode::on_rpc_transport_fail(const std::string& peer_id) {
  std::lock_guard<std::mutex> lock(mu_);
  replication_.dec_inflight(peer_id);
}

void RaftNode::on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                               uint64_t last_term, bool* granted) {
  std::lock_guard<std::mutex> lock(mu_);
  election_.on_request_vote(from, term, last_idx, last_term, granted);
}

void RaftNode::on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term, bool* granted) {
  std::lock_guard<std::mutex> lock(mu_);
  election_.on_request_pre_vote(from, term, last_idx, last_term, granted);
}

void RaftNode::on_request_vote_reply(const std::string& from, uint64_t term, bool granted) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    election_.on_request_vote_reply(from, term, granted);
  }
  ready_pump_.flush_transport();
  ready_pump_.maybe_notify_ready();
}

void RaftNode::on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    election_.on_request_pre_vote_reply(from, term, granted);
  }
  ready_pump_.flush_transport();
  ready_pump_.maybe_notify_ready();
}

void RaftNode::add_peer(const Peer& p) {
  std::lock_guard<std::mutex> lock(mu_);
  membership_.add_peer(p);
}

void RaftNode::remove_peer(const std::string& id) {
  std::lock_guard<std::mutex> lock(mu_);
  membership_.remove_peer(id);
}

}  // namespace openkv::raft
