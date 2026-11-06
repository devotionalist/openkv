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
// RaftNode：复制/选举 + 持久化 + InstallSnapshot + Joint + 异步 Apply + inflight。

#include "opskv/raft/raft_node.hpp"
#include "opskv/codec.hpp"

#include "opskv/logging.hpp"

#include <algorithm>
#include <chrono>
#include <random>
#include <sstream>
#include <thread>

namespace opskv::raft {
namespace {

using ::opskv::to_hex;

// 快照 blob 前缀：SKVC1\n<conf_csv>\n<kv>。conf 与 ADMIN_ENSURE 同形（id@addr[*N],...）。
constexpr std::string_view kSnapConfMagic = "SKVC1\n";

std::string encode_peers_csv(const std::vector<Peer>& peers) {
  std::ostringstream oss;
  for (size_t i = 0; i < peers.size(); ++i) {
    if (i) oss << ',';
    oss << peers[i].id << '@' << peers[i].address;
    if (peers[i].learner) oss << "*N";
  }
  return oss.str();
}

std::string wrap_snap_conf(const std::string& conf_csv, std::string kv) {
  if (conf_csv.empty()) return kv;
  return std::string(kSnapConfMagic) + conf_csv + "\n" + std::move(kv);
}

bool parse_peers_csv(std::string_view csv, std::vector<Peer>* out) {
  if (!out) return false;
  out->clear();
  size_t i = 0;
  while (i < csv.size()) {
    size_t j = csv.find(',', i);
    if (j == std::string_view::npos) j = csv.size();
    auto item = csv.substr(i, j - i);
    i = j + 1;
    if (item.empty()) continue;
    bool learner = false;
    if (item.size() >= 2 && item.substr(item.size() - 2) == "*N") {
      learner = true;
      item.remove_suffix(2);
    }
    const auto at = item.find('@');
    if (at == std::string_view::npos || at == 0) continue;
    Peer p;
    p.id = std::string(item.substr(0, at));
    p.address = std::string(item.substr(at + 1));
    p.learner = learner;
    out->push_back(std::move(p));
  }
  return !out->empty();
}

// 若带 SKVC1 前缀则剥出 conf 与 kv；否则整段当 kv。
void peel_snap_conf(std::string_view data, std::vector<Peer>* conf, std::string_view* kv) {
  if (conf) conf->clear();
  if (!data.starts_with(kSnapConfMagic)) {
    if (kv) *kv = data;
    return;
  }
  const auto rest = data.substr(kSnapConfMagic.size());
  const auto nl = rest.find('\n');
  if (nl == std::string_view::npos) {
    if (kv) *kv = data;
    return;
  }
  if (conf) parse_peers_csv(rest.substr(0, nl), conf);
  if (kv) *kv = rest.substr(nl + 1);
}

}  // namespace

RaftNode::RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
                   SnapshotProvider snap_provider, SnapshotApplier snap_applier)
    : opts_(std::move(opts)),
      on_apply_(std::move(on_apply)),
      send_(std::move(send)),
      snap_provider_(std::move(snap_provider)),
      snap_applier_(std::move(snap_applier)) {
  if (opts_.max_inflight_msgs == 0) opts_.max_inflight_msgs = 1;
  if (opts_.max_ae_entries == 0) opts_.max_ae_entries = 64;
  if (opts_.max_ae_bytes == 0) opts_.max_ae_bytes = 256 * 1024;
  if (opts_.apply_queue_capacity == 0) opts_.apply_queue_capacity = 1024;
  if (opts_.external_io) {
    // RawNode：协议不发网/不回调 apply；落盘经 Store persist_fn→persist_ready。
    // 保留 persist_dir 以便启动 load + Ready 外提刷盘。
    opts_.enable_async_apply = false;
  }
  std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(opts_.election_timeout_ms / 100,
                                          opts_.election_timeout_ms / 50);
  election_timeout_ticks_ = std::max(5, dist(rng));
  for (const auto& p : opts_.peers) {
    next_index_[p.id] = 1;
    match_index_[p.id] = 0;
  }
  if (opts_.persist_batch_n == 0) opts_.persist_batch_n = 1;
  group_id_ = opts_.raft_group_id != 0 ? opts_.raft_group_id : opts_.shard_id;
  if (opts_.raft_engine) {
    engine_ = opts_.raft_engine;
  } else if (!opts_.persist_dir.empty()) {
    owned_file_engine_ = std::make_shared<FileRaftEngine>(opts_.persist_dir, opts_.persist_sync);
    // 兼容旧 hard.state/log.entries
    if (!owned_file_engine_->get_raft_state(group_id_)) {
      owned_file_engine_->migrate_legacy_persist_dir(opts_.persist_dir, group_id_);
    }
    engine_ = owned_file_engine_;
  }
  if (engine_) load_from_disk();
  start_apply_worker();
}

RaftNode::~RaftNode() { stop_apply_worker(); }

void RaftNode::start_apply_worker() {
  if (!opts_.enable_async_apply) return;
  apply_running_ = true;
  apply_thread_ = std::thread([this] { apply_worker_loop(); });
}

void RaftNode::stop_apply_worker() {
  if (!apply_running_.exchange(false)) return;
  apply_cv_.notify_all();
  if (apply_thread_.joinable()) apply_thread_.join();
}

void RaftNode::apply_worker_loop() {
  while (apply_running_) {
    ApplyItem item;
    {
      std::unique_lock<std::mutex> lk(apply_mu_);
      apply_cv_.wait(lk, [&] { return !apply_q_.empty() || !apply_running_; });
      if (!apply_running_ && apply_q_.empty()) break;
      if (apply_q_.empty()) continue;
      item = std::move(apply_q_.front());
      apply_q_.pop_front();
    }
    if (on_apply_) {
      in_on_apply_.store(true, std::memory_order_relaxed);
      try {
        on_apply_(item.index, item.data);
      } catch (...) {
        in_on_apply_.store(false, std::memory_order_relaxed);
        throw;
      }
      in_on_apply_.store(false, std::memory_order_relaxed);
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (item.index > last_applied_) last_applied_ = item.index;
      maybe_compact();
      persist_locked();
    }
    apply_cv_.notify_all();
  }
}

bool RaftNode::enqueue_apply(uint64_t index, std::string data) {
  std::lock_guard<std::mutex> lk(apply_mu_);
  if (apply_q_.size() >= opts_.apply_queue_capacity) return false;
  apply_q_.push_back(ApplyItem{index, std::move(data)});
  apply_cv_.notify_one();
  return true;
}

void RaftNode::wait_apply_caught_up_to(uint64_t index) {
  // 调用方已持 mu_；需释放后等 apply，再重新获取 — 简化：轮询 apply 队列空且 last_applied
  for (;;) {
    bool done = false;
    {
      std::lock_guard<std::mutex> lk(apply_mu_);
      done = apply_q_.empty() && last_applied_ >= index;
    }
    if (done) return;
    // 短暂释放 mu_ 让 worker 推进
    mu_.unlock();
    {
      std::unique_lock<std::mutex> lk(apply_mu_);
      apply_cv_.wait_for(lk, std::chrono::milliseconds(1), [&] {
        return apply_q_.empty() || !apply_running_;
      });
    }
    mu_.lock();
    if (last_applied_ >= index) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      if (apply_q_.empty()) return;
    }
  }
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
  stable_first_index_ = log_.first_index();
  stable_last_index_ = log_.last_index();
  role_ = Role::kFollower;
  leader_id_.clear();
  SKV_INFO << "shard=" << opts_.shard_id << " group=" << group_id_ << " recovered term=" << term_
           << " commit=" << commit_index_ << " applied=" << last_applied_
           << " last_index=" << log_.last_index();
}

void RaftNode::persist_locked() {
  if (opts_.external_io) {
    ready_hs_dirty_ = true;
    ready_hs_.term = term_;
    ready_hs_.vote = voted_for_;
    ready_hs_.commit = commit_index_;
    return;
  }
  persist_dirty_ = true;
  ++persist_pending_;
  persist_maybe_flush(false);
}

void RaftNode::persist_maybe_flush(bool force) {
  if (opts_.external_io) return;
  if (!engine_) return;
  if (!persist_dirty_) return;
  if (!force && persist_pending_ < opts_.persist_batch_n) return;
  const bool sync = opts_.persist_sync != SyncMode::kNone;
  engine_flush_locked(sync);
  persist_dirty_ = false;
  persist_pending_ = 0;
}

void RaftNode::engine_flush_locked(bool sync) {
  if (!engine_) return;
  const uint64_t first = log_.first_index();
  const uint64_t last = log_.last_index();

  auto batch = engine_->make_log_batch();

  // compact：first 前移 → 删盘上旧前缀
  if (stable_first_index_ != 0 && first > stable_first_index_) {
    batch->cut_logs(group_id_, stable_first_index_, first);
  }

  // 冲突截断：内存 last < 已稳定 last → 删尾部
  if (stable_last_index_ != 0 && last < stable_last_index_) {
    batch->cut_logs(group_id_, last + 1, stable_last_index_ + 1);
  }

  // 追加 (stable_last, last]；冷启动 stable=0 则写 [first, last]
  uint64_t from = 0;
  if (stable_last_index_ == 0) {
    from = first;
  } else if (last > stable_last_index_) {
    from = stable_last_index_ + 1;
    if (from < first) from = first;
  }
  std::vector<LogEntry> delta;
  if (from != 0 && last >= from) {
    // 若 from..last 与盘上重叠（同 index 重写），先 cut 再 append
    if (stable_last_index_ != 0 && from <= stable_last_index_) {
      batch->cut_logs(group_id_, from, stable_last_index_ + 1);
    }
    for (uint64_t i = from; i <= last; ++i) {
      if (auto e = log_.entry(i)) delta.push_back(*e);
    }
    if (!delta.empty()) batch->append(group_id_, std::move(delta));
  }

  PersistedHardState hs;
  hs.term = term_;
  hs.voted_for = voted_for_;
  hs.commit_index = commit_index_;
  hs.last_applied = last_applied_;
  hs.first_index = first;
  hs.snapshot_term = log_.snapshot_term();
  batch->put_raft_state(group_id_, RaftLocalState::from_persisted(hs, last));
  engine_->consume(*batch, sync);

  stable_first_index_ = first;
  stable_last_index_ = last;
}

void RaftNode::recover_apply() {
  // apply worker 内 on_apply→ensure_region→recover_apply 会 wait 自己推进 last_applied → 死锁
  if (in_on_apply_.load(std::memory_order_relaxed)) return;
  std::lock_guard<std::mutex> lock(mu_);
  apply_committed();
  persist_locked();
}

SoftState RaftNode::soft_state() const {
  std::lock_guard<std::mutex> lock(mu_);
  SoftState st;
  st.role = role_;
  st.leader_id = leader_id_;
  st.term = term_;
  st.commit_index = commit_index_;
  st.last_applied = last_applied_;
  st.election_count = election_count_;
  st.snapshot_send_count = snapshot_send_count_;
  st.in_joint = in_joint_;
  st.conf_change_busy = in_joint_ || auto_leave_pending_;
  if (role_ == Role::kLeader) {
    uint64_t min_match = log_.last_index();
    bool any = false;
    for (const auto& p : opts_.peers) {
      if (p.id == opts_.self_id || p.learner) continue;
      auto it = match_index_.find(p.id);
      uint64_t m = it == match_index_.end() ? 0 : it->second;
      // 从未建连成功的 peer（match=0 且 quiet 已爆）不拖 lag，避免僵尸副本误导运维
      auto qit = peer_quiet_ticks_.find(p.id);
      if (m == 0 && qit != peer_quiet_ticks_.end() &&
          qit->second >= election_timeout_ticks_ * 2) {
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
  return in_joint_;
}

bool RaftNode::conf_change_busy() const {
  std::lock_guard<std::mutex> lock(mu_);
  return in_joint_ || auto_leave_pending_;
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
  auto it = match_index_.find(peer_id);
  return it == match_index_.end() ? 0 : it->second;
}

uint64_t RaftNode::last_index() const {
  std::lock_guard<std::mutex> lock(mu_);
  return log_.last_index();
}

bool RaftNode::abdicate() {
  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::kLeader) return false;
  become_follower(term_, {});
  persist_locked();
  return true;
}

bool RaftNode::force_election() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (self_is_learner()) return false;
    if (role_ == Role::kLeader) return true;
    election_elapsed_ = 0;
    in_pre_vote_ = false;
    pre_votes_.clear();
    become_candidate();
    start_election();
    persist_locked();
    mark_activity();
  }
  flush_transport();
  return true;
}

bool RaftNode::consume_activity_flag() {
  std::lock_guard<std::mutex> lock(mu_);
  bool v = activity_flag_;
  activity_flag_ = false;
  return v;
}

void RaftNode::mark_activity() { activity_flag_ = true; }

void RaftNode::note_peer_progress(const std::string& peer_id) {
  peer_quiet_ticks_[peer_id] = 0;
}

bool RaftNode::lease_valid_locked() const {
  if (!opts_.enable_lease_read || opts_.leader_lease_ms == 0) return false;
  if (role_ != Role::kLeader) return false;
  return std::chrono::steady_clock::now() < lease_expire_;
}

void RaftNode::note_read_index_ack(const std::string& peer_id) {
  if (pending_read_gen_ == 0) return;
  read_index_acks_.insert(peer_id);
  // self 计入
  read_index_acks_.insert(opts_.self_id);
  uint32_t need = in_joint_ ? std::max(majority_of(joint_old_voters_), majority_of(joint_new_voters_))
                            : majority_of(voter_ids());
  if (read_index_acks_.size() >= need) {
    pending_read_gen_ = 0;
    read_cv_.notify_all();
  }
}

bool RaftNode::read_index(uint64_t* read_commit, uint32_t timeout_ms) {
  if (!read_commit) return false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::kLeader) return false;
    *read_commit = commit_index_;
    if (majority() <= 1 || lease_valid_locked()) {
      return true;
    }
    pending_read_commit_ = commit_index_;
    pending_read_gen_ = ++read_index_gen_;
    read_index_acks_.clear();
    read_index_acks_.insert(opts_.self_id);
    broadcast_heartbeat();
    need_bcast_ = false;
  }
  flush_transport();
  {
    std::unique_lock<std::mutex> lock(mu_);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (pending_read_gen_ != 0) {
      if (read_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
        pending_read_gen_ = 0;
        return false;
      }
    }
    if (role_ != Role::kLeader) return false;
    *read_commit = pending_read_commit_;
    lease_expire_ = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(opts_.leader_lease_ms);
  }
  return true;
}

void RaftNode::dec_inflight(const std::string& peer_id) {
  auto it = inflight_.find(peer_id);
  if (it != inflight_.end() && it->second > 0) --it->second;
  snap_inflight_.erase(peer_id);
  probe_inflight_.erase(peer_id);
}

void RaftNode::on_rpc_transport_fail(const std::string& peer_id) {
  std::lock_guard<std::mutex> lock(mu_);
  dec_inflight(peer_id);
}

bool RaftNode::try_inc_inflight(const std::string& peer_id) {
  auto& n = inflight_[peer_id];
  if (n >= opts_.max_inflight_msgs) return false;
  ++n;
  return true;
}

void RaftNode::maybe_check_quorum() {
  if (!opts_.check_quorum || role_ != Role::kLeader) return;
  // Joint 内必须撑到 LeaveJoint 双多数提交；step-down 会清 match、反复 propose LeaveJoint
  // 却永远 commit 不了（it-scale-live REMOVE 后卡 joint 根因）。
  if (in_joint_) return;
  // 新 Leader：先发心跳并等待 ACK，避免刚上任就被误判让位（TCP 排队时尤甚）。
  if (leader_quorum_grace_ticks_ > 0) {
    --leader_quorum_grace_ticks_;
    return;
  }
  const auto voters = voter_ids();
  if (majority_of(voters) <= 1) return;
  // 宽松窗口：3×election_timeout（本机也常因单 worker 快照头阻塞误判）。
  const int quiet_limit = election_timeout_ticks_ * 3;
  auto active_in = [&](const std::vector<std::string>& vs) -> uint32_t {
    uint32_t active = 0;
    for (const auto& id : vs) {
      if (id == opts_.self_id) {
        ++active;
        continue;
      }
      // 快照在途 / 管道满发不出：不算「对端无响应」
      if (snap_inflight_.count(id) || peer_send_blocked_.count(id)) {
        ++active;
        continue;
      }
      auto it = peer_quiet_ticks_.find(id);
      // 探针在途且 quiet 仍低：保护慢网；quiet 已爆说明发出后长期无 ACK → 失联
      if (probe_inflight_.count(id) &&
          (it == peer_quiet_ticks_.end() || it->second < quiet_limit)) {
        ++active;
        continue;
      }
      // 尚未建 quiet 槽（刚 Promote 的 voter）视为活跃，避免误判 step-down
      if (it == peer_quiet_ticks_.end() || it->second < quiet_limit) ++active;
    }
    return active;
  };
  const bool ok = active_in(voters) >= majority_of(voters);
  if (!ok) {
    SKV_WARN << "shard=" << opts_.shard_id << " check_quorum step down term=" << term_;
    become_follower(term_, {});
    persist_locked();
    mark_activity();
  }
}

std::vector<std::string> RaftNode::voter_ids() const {
  std::vector<std::string> out;
  for (const auto& p : opts_.peers) {
    if (!p.learner) out.push_back(p.id);
  }
  return out;
}

uint32_t RaftNode::majority_of(const std::vector<std::string>& voters) const {
  size_t n = voters.empty() ? 1 : voters.size();
  return static_cast<uint32_t>(n / 2 + 1);
}

uint32_t RaftNode::majority() const {
  if (in_joint_) {
    return std::max(majority_of(joint_old_voters_), majority_of(joint_new_voters_));
  }
  return majority_of(voter_ids());
}

bool RaftNode::quorate_at(const std::vector<std::string>& voters, uint64_t index) const {
  uint32_t ok = 0;
  for (const auto& id : voters) {
    uint64_t m = 0;
    if (id == opts_.self_id) {
      m = match_index_.count(opts_.self_id) ? match_index_.at(opts_.self_id) : log_.last_index();
    } else {
      auto it = match_index_.find(id);
      m = it == match_index_.end() ? 0 : it->second;
    }
    if (m >= index) ++ok;
  }
  return ok >= majority_of(voters);
}

bool RaftNode::votes_won(const std::unordered_map<std::string, bool>& votes) const {
  auto count_yes = [&](const std::vector<std::string>& vs) {
    uint32_t yes = 0;
    for (const auto& id : vs) {
      auto it = votes.find(id);
      if (it != votes.end() && it->second) ++yes;
    }
    return yes;
  };
  if (!in_joint_) {
    return count_yes(voter_ids()) >= majority_of(voter_ids());
  }
  return count_yes(joint_old_voters_) >= majority_of(joint_old_voters_) &&
         count_yes(joint_new_voters_) >= majority_of(joint_new_voters_);
}

void RaftNode::enqueue(std::string peer_id, std::string type, std::string payload) {
  outbound_.push_back(Outbound{std::move(peer_id), std::move(type), std::move(payload)});
}

void RaftNode::flush_transport() {
  if (opts_.external_io) return;  // 消息留在 outbound_，由 take_ready 外提
  std::vector<Outbound> batch;
  {
    std::lock_guard<std::mutex> lock(mu_);
    batch.swap(outbound_);
  }
  for (auto& m : batch) {
    if (send_) send_(m.peer_id, m.type, m.payload);
  }
}

bool RaftNode::has_ready() const {
  std::lock_guard<std::mutex> lock(mu_);
  return ready_hs_dirty_ || !ready_unstable_.empty() || !ready_committed_.empty() ||
         !outbound_.empty() || !ready_snaps_.empty();
}

Ready RaftNode::take_ready() {
  std::lock_guard<std::mutex> lock(mu_);
  Ready rd;
  if (ready_hs_dirty_) {
    rd.hard_state = ready_hs_;
    rd.must_sync = true;
    ready_hs_dirty_ = false;
  }
  rd.entries = std::move(ready_unstable_);
  ready_unstable_.clear();
  rd.committed_entries = std::move(ready_committed_);
  ready_committed_.clear();
  for (auto& m : outbound_) {
    rd.messages.push_back(Message{std::move(m.peer_id), std::move(m.type), std::move(m.payload)});
  }
  outbound_.clear();
  if (!ready_snaps_.empty()) {
    rd.snapshot = ready_snaps_.front();
    ready_snaps_.erase(ready_snaps_.begin());
  }
  SoftState st;
  st.role = role_;
  st.leader_id = leader_id_;
  st.term = term_;
  st.commit_index = commit_index_;
  st.last_applied = last_applied_;
  st.election_count = election_count_;
  st.snapshot_send_count = snapshot_send_count_;
  st.in_joint = in_joint_;
  st.conf_change_busy = in_joint_ || auto_leave_pending_;
  rd.soft_state = st;
  return rd;
}

void RaftNode::advance(const Ready& rd) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!opts_.external_io) return;
  // 普通条目 last_applied 由 report_applied（ApplyFsm）推进，避免 propose 误以为已落 KV。
  (void)rd;
}

void RaftNode::persist_ready(const Ready& rd) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!opts_.external_io || !engine_) return;
  const bool sync =
      rd.must_sync || opts_.persist_sync != SyncMode::kNone || ready_hs_dirty_;
  engine_flush_locked(sync);
  ready_hs_dirty_ = false;
  persist_dirty_ = false;
  persist_pending_ = 0;
}

void RaftNode::report_applied(uint64_t index) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!opts_.external_io) return;
  if (index > last_applied_) last_applied_ = index;
  maybe_compact();
  persist_locked();
}

void RaftNode::load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries) {
  std::lock_guard<std::mutex> lock(mu_);
  term_ = hs.term;
  voted_for_ = hs.voted_for;
  commit_index_ = hs.commit_index;
  last_applied_ = hs.last_applied;
  apply_scheduled_ = hs.last_applied;
  log_.load(hs.first_index, hs.snapshot_term, std::vector<LogEntry>(entries));
  stable_first_index_ = log_.first_index();
  stable_last_index_ = log_.last_index();
  role_ = Role::kFollower;
  leader_id_.clear();
  ready_hs_dirty_ = false;
  ready_unstable_.clear();
  ready_committed_.clear();
  ready_snaps_.clear();
  outbound_.clear();
}

bool RaftNode::encode_conf(const ConfChange& cc, std::string* out) const {
  out->clear();
  out->push_back(static_cast<char>(0xFF));
  if (cc.type == ConfChange::kEnterJoint) {
    out->push_back(static_cast<char>(ConfChange::kEnterJoint));
    out->push_back(static_cast<char>(cc.joint_change));
  } else {
    out->push_back(static_cast<char>(cc.type));
  }
  out->append(cc.peer.id);
  out->push_back('\0');
  out->append(cc.peer.address);
  return true;
}

bool RaftNode::decode_conf(std::string_view data, ConfChange* cc) {
  if (data.size() < 3 || static_cast<unsigned char>(data[0]) != 0xFF) return false;
  auto type = static_cast<ConfChange::Type>(data[1]);
  size_t id_off = 2;
  if (type == ConfChange::kEnterJoint) {
    if (data.size() < 4) return false;
    cc->type = ConfChange::kEnterJoint;
    cc->joint_change = static_cast<ConfChange::Type>(data[2]);
    id_off = 3;
  } else {
    cc->type = type;
    cc->joint_change = type;
  }
  auto nul = data.find('\0', id_off);
  if (nul == std::string_view::npos) return false;
  cc->peer.id = std::string(data.substr(id_off, nul - id_off));
  cc->peer.address = std::string(data.substr(nul + 1));
  cc->peer.learner = (cc->type == ConfChange::kAddLearner ||
                      (cc->type == ConfChange::kEnterJoint &&
                       cc->joint_change == ConfChange::kAddLearner));
  return true;
}

bool RaftNode::propose(std::string data, uint64_t* assigned_index) {
  uint64_t idx = 0;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::kLeader) return false;
    if (opts_.enable_async_apply) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      if (apply_q_.size() >= opts_.apply_queue_capacity) {
        SKV_WARN << "shard=" << opts_.shard_id << " propose backpressure apply_q="
                 << apply_q_.size();
        return false;
      }
    }
    idx = log_.append(term_, std::move(data));
    if (opts_.external_io) {
      if (auto e = log_.entry(idx)) ready_unstable_.push_back(*e);
    }
    match_index_[opts_.self_id] = idx;
    if (assigned_index) *assigned_index = idx;
    persist_locked();
    need_bcast_ = true;
    if (!opts_.coalesce_replication || majority() <= 1) {
      maybe_broadcast();
    }
    maybe_commit();
    apply_committed();
    persist_locked();
  }
  flush_transport();
  // 单节点 / 已提交：等待异步 apply 追上，保持 propose 语义对调用方可见
  if (opts_.enable_async_apply && !opts_.external_io) {
    for (int i = 0; i < 200; ++i) {
      if (soft_state().last_applied >= idx) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  return true;
}

bool RaftNode::propose_conf_change(const ConfChange& cc) {
  ConfChange out = cc;
  if (cc.type == ConfChange::kPromoteLearner || cc.type == ConfChange::kRemoveNode) {
    std::lock_guard<std::mutex> lock(mu_);
    if (in_joint_ || auto_leave_pending_) return false;
    out.type = ConfChange::kEnterJoint;
    out.joint_change = cc.type;
  } else if (cc.type == ConfChange::kEnterJoint || cc.type == ConfChange::kLeaveJoint) {
    std::lock_guard<std::mutex> lock(mu_);
    if (cc.type == ConfChange::kEnterJoint && (in_joint_ || auto_leave_pending_)) return false;
  }
  std::string payload;
  if (!encode_conf(out, &payload)) return false;
  return propose(std::move(payload));
}

void RaftNode::maybe_broadcast() {
  // 仅 Leader 且显式标记 need_bcast_ 时发送；避免「空 AE 成功 → 再广播」正反馈打满 transport。
  if (role_ != Role::kLeader || !need_bcast_) return;
  broadcast_heartbeat();
  need_bcast_ = false;
}

void RaftNode::tick() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    // 先 apply（ConfChange 加 peer / auto_leave），再心跳，避免新 learner 空等一整拍
    apply_committed();
    if (role_ == Role::kLeader) {
      maybe_auto_leave_joint();
      ++heartbeat_elapsed_;
      const bool hb =
          heartbeat_elapsed_ * 100 >= static_cast<int>(opts_.heartbeat_ms);
      if (hb || need_bcast_) {
        if (hb) heartbeat_elapsed_ = 0;
        broadcast_heartbeat();
        need_bcast_ = false;
      }
      for (auto& [id, quiet] : peer_quiet_ticks_) {
        // 快照在途 / 本轮发不出 AE：不涨 quiet（否则本机管道拥塞会被当成网络分区）
        if (snap_inflight_.count(id) || peer_send_blocked_.count(id)) continue;
        ++quiet;
      }
      maybe_check_quorum();
    } else if (!self_is_learner()) {
      ++election_elapsed_;
      if (election_elapsed_ >= election_timeout_ticks_) {
        election_elapsed_ = 0;
        // 超时后必须丢掉陈旧 leader_id。否则 on_request_pre_vote 因
        //「仍有 leader 且 elapsed 刚清零」互拒 PreVote，选主永久卡死（it-scale-live）。
        leader_id_.clear();
        if (opts_.enable_pre_vote) {
          start_pre_vote();
        } else {
          become_candidate();
          start_election();
        }
        persist_locked();
        mark_activity();
      }
    }
    persist_maybe_flush(true);  // tick 边界强制 group flush
  }
  flush_transport();
}

void RaftNode::maybe_auto_leave_joint() {
  if (!auto_leave_pending_ || !in_joint_ || role_ != Role::kLeader) return;
  // 已有本任期未提交的 LeaveJoint：只等双多数提交，勿重复 append（换主后旧任期条目需新任期条目才能提交）。
  for (uint64_t i = commit_index_ + 1; i <= log_.last_index(); ++i) {
    auto e = log_.entry(i);
    if (!e || e->term != term_) continue;
    ConfChange cc;
    if (decode_conf(e->data, &cc) && cc.type == ConfChange::kLeaveJoint) {
      leave_joint_inflight_ = true;
      need_bcast_ = true;
      return;
    }
  }
  if (leave_joint_inflight_) {
    // 仅有旧任期 LeaveJoint：允许再 propose 一条本任期条目以推进 commit。
    leave_joint_inflight_ = false;
  }
  leave_joint_inflight_ = true;
  // auto_leave_pending_ 保留到 LeaveJoint apply，换主时可重新武装。
  ConfChange leave;
  leave.type = ConfChange::kLeaveJoint;
  std::string payload;
  encode_conf(leave, &payload);
  auto idx = log_.append(term_, std::move(payload));
  if (opts_.external_io) {
    if (auto e = log_.entry(idx)) ready_unstable_.push_back(*e);
  }
  match_index_[opts_.self_id] = idx;
  need_bcast_ = true;
  persist_locked();
  SKV_INFO << "shard=" << opts_.shard_id << " auto LeaveJoint proposed idx=" << idx;
}

void RaftNode::become_follower(uint64_t term, const std::string& leader) {
  role_ = Role::kFollower;
  in_pre_vote_ = false;
  pre_votes_.clear();
  if (term > term_) {
    term_ = term;
    voted_for_.clear();
  } else {
    term_ = term;
  }
  leader_id_ = leader;
  election_elapsed_ = 0;
  leader_quorum_grace_ticks_ = 0;
  inflight_.clear();
  snap_inflight_.clear();
}

void RaftNode::become_candidate() {
  role_ = Role::kCandidate;
  in_pre_vote_ = false;
  pre_votes_.clear();
  ++term_;
  voted_for_ = opts_.self_id;
  leader_id_.clear();
  votes_.clear();
  votes_[opts_.self_id] = true;
  mark_activity();
}

void RaftNode::become_leader() {
  role_ = Role::kLeader;
  in_pre_vote_ = false;
  pre_votes_.clear();
  leader_id_ = opts_.self_id;
  heartbeat_elapsed_ = 0;
  peer_quiet_ticks_.clear();
  inflight_.clear();
  snap_inflight_.clear();
  probe_inflight_.clear();
  peer_send_blocked_.clear();
  for (const auto& p : opts_.peers) {
    next_index_[p.id] = log_.last_index() + 1;
    match_index_[p.id] = 0;
    if (!p.learner && p.id != opts_.self_id) peer_quiet_ticks_[p.id] = 0;
  }
  match_index_[opts_.self_id] = log_.last_index();
  ++election_count_;
  // 至少撑过一轮选举超时，等心跳 ACK 回来再启用 check_quorum。
  leader_quorum_grace_ticks_ = election_timeout_ticks_ + 2;
  lease_expire_ = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(opts_.leader_lease_ms);
  // ConfChange 中途换主：Follower apply EnterJoint 时不会置 auto_leave；新 Leader 必须重武装。
  if (in_joint_) {
    auto_leave_pending_ = true;
    leave_joint_inflight_ = false;
    // Joint 退出前拉长 grace，给 LeaveJoint 复制窗口（即便 check_quorum 在 joint 内已跳过）。
    leader_quorum_grace_ticks_ = election_timeout_ticks_ * 4 + 2;
    SKV_INFO << "shard=" << opts_.shard_id << " become leader in_joint → re-arm LeaveJoint";
  }
  SKV_INFO << "shard=" << opts_.shard_id << " become leader term=" << term_
           << " last_index=" << log_.last_index();
  mark_activity();
  broadcast_heartbeat();
  need_bcast_ = false;
  if (in_joint_) {
    maybe_auto_leave_joint();
    if (need_bcast_) {
      broadcast_heartbeat();
      need_bcast_ = false;
    }
  }
}

void RaftNode::step_down_if_stale(uint64_t term) {
  if (term > term_) become_follower(term);
}

void RaftNode::start_pre_vote() {
  in_pre_vote_ = true;
  pre_votes_.clear();
  pre_votes_[opts_.self_id] = true;
  const uint64_t future_term = term_ + 1;
  SKV_INFO << "shard=" << opts_.shard_id << " start pre-vote future_term=" << future_term;
  mark_activity();
  for (const auto& p : opts_.peers) {
    if (p.id == opts_.self_id || p.learner) continue;
    std::string payload = std::to_string(future_term) + "|" + std::to_string(log_.last_index()) +
                          "|" + std::to_string(log_.last_term());
    enqueue(p.id, "RequestPreVote", std::move(payload));
  }
  if (votes_won(pre_votes_)) {
    become_candidate();
    start_election();
  }
}

void RaftNode::start_election() {
  SKV_INFO << "shard=" << opts_.shard_id << " start election term=" << term_;
  mark_activity();
  for (const auto& p : opts_.peers) {
    if (p.id == opts_.self_id || p.learner) continue;
    std::string payload = std::to_string(term_) + "|" + std::to_string(log_.last_index()) + "|" +
                          std::to_string(log_.last_term());
    enqueue(p.id, "RequestVote", std::move(payload));
  }
  if (votes_won(votes_)) become_leader();
}

void RaftNode::broadcast_heartbeat() {
  peer_send_blocked_.clear();
  for (const auto& p : opts_.peers) {
    if (p.id == opts_.self_id) continue;

    if (next_index_[p.id] < log_.first_index()) {
      if (!opts_.external_io && !snap_provider_) {
        SKV_WARN << "shard=" << opts_.shard_id << " peer=" << p.id
                 << " behind compact window but no snap_provider; skip InstallSnapshot";
        continue;
      }
      if (snap_inflight_.count(p.id)) continue;
      if (opts_.snapshot_min_interval_ms > 0) {
        const auto now = std::chrono::steady_clock::now();
        auto it = last_snap_sent_.find(p.id);
        if (it != last_snap_sent_.end() &&
            now - it->second < std::chrono::milliseconds(opts_.snapshot_min_interval_ms)) {
          continue;
        }
      }
      if (!try_inc_inflight(p.id)) {
        peer_send_blocked_.insert(p.id);
        continue;
      }
      snap_inflight_.insert(p.id);
      uint64_t last_idx = last_applied_ > 0 ? last_applied_ : (log_.first_index() - 1);
      uint64_t last_term = log_.snapshot_term();
      if (auto e = log_.entry(last_idx)) last_term = e->term;
      const std::string conf_csv = encode_peers_csv(opts_.peers);
      if (opts_.external_io) {
        SnapshotMeta meta;
        meta.index = last_idx;
        meta.term = last_term;
        meta.to = p.id;
        meta.conf_state = conf_csv;
        ready_snaps_.push_back(meta);
        // 空 blob：外层 SnapWorker 填充后发送（附带 conf_state）
        std::ostringstream oss;
        oss << term_ << '|' << last_idx << '|' << last_term << '|';
        enqueue(p.id, "InstallSnapshot", oss.str());
      } else {
        std::string data = wrap_snap_conf(conf_csv, snap_provider_(&last_idx, &last_term));
        std::ostringstream oss;
        oss << term_ << '|' << last_idx << '|' << last_term << '|' << to_hex(data);
        enqueue(p.id, "InstallSnapshot", oss.str());
        SKV_INFO << "shard=" << opts_.shard_id << " send snapshot to=" << p.id
                 << " last_idx=" << last_idx << " bytes=" << data.size()
                 << " conf_peers=" << opts_.peers.size();
      }
      last_snap_sent_[p.id] = std::chrono::steady_clock::now();
      ++snapshot_send_count_;
      continue;
    }

    uint64_t prev = next_index_[p.id] > 0 ? next_index_[p.id] - 1 : 0;
    uint64_t prev_term = 0;
    if (prev == log_.first_index() - 1) {
      prev_term = log_.snapshot_term();
    } else if (auto e = log_.entry(prev)) {
      prev_term = e->term;
    }
    auto ents =
        log_.entries_from(next_index_[p.id], opts_.max_ae_entries, opts_.max_ae_bytes);
    const bool probe = ents.empty();
    if (probe) {
      // 空心跳不占复制管道额度，避免落后 peer 的 snap/AE 堵死保活 → 误 check_quorum
      if (probe_inflight_.count(p.id)) continue;
      probe_inflight_.insert(p.id);
    } else if (!try_inc_inflight(p.id)) {
      peer_send_blocked_.insert(p.id);
      continue;
    }
    std::ostringstream oss;
    oss << term_ << '|' << prev << '|' << prev_term << '|' << commit_index_ << '|' << ents.size();
    for (const auto& e : ents) {
      oss << '|' << e.term << '|' << e.index << '|' << to_hex(e.data);
    }
    enqueue(p.id, "AppendEntries", oss.str());
  }
}

void RaftNode::maybe_commit() {
  if (role_ != Role::kLeader) return;
  uint64_t lo = commit_index_ + 1;
  uint64_t hi = log_.last_index();
  uint64_t best = commit_index_;
  for (uint64_t cand = hi; cand >= lo; --cand) {
    bool ok;
    if (in_joint_) {
      ok = quorate_at(joint_old_voters_, cand) && quorate_at(joint_new_voters_, cand);
    } else {
      ok = quorate_at(voter_ids(), cand);
    }
    if (!ok) continue;
    if (auto e = log_.entry(cand); e && e->term == term_) {
      best = cand;
      break;
    }
  }
  if (best > commit_index_) commit_index_ = best;
}

void RaftNode::apply_conf_entry(const LogEntry& e) {
  ConfChange cc;
  if (!decode_conf(e.data, &cc)) return;

  if (cc.type == ConfChange::kLeaveJoint) {
    if (in_joint_) {
      // 丢掉不在 C_new 中的 voter
      std::unordered_set<std::string> keep(joint_new_voters_.begin(), joint_new_voters_.end());
      for (const auto& p : opts_.peers) {
        if (!p.learner && !keep.count(p.id)) {
          // defer erase
        }
      }
      opts_.peers.erase(std::remove_if(opts_.peers.begin(), opts_.peers.end(),
                                       [&](const Peer& p) {
                                         if (p.learner) return false;
                                         return !keep.count(p.id);
                                       }),
                        opts_.peers.end());
      in_joint_ = false;
      joint_old_voters_.clear();
      joint_new_voters_.clear();
      auto_leave_pending_ = false;
      leave_joint_inflight_ = false;
      SKV_INFO << "shard=" << opts_.shard_id << " left joint config voters="
               << voter_ids().size();
    } else {
      leave_joint_inflight_ = false;
      auto_leave_pending_ = false;
    }
    return;
  }

  if (cc.type == ConfChange::kEnterJoint) {
    joint_old_voters_ = voter_ids();
    ConfChange inner;
    inner.type = cc.joint_change;
    inner.peer = cc.peer;
    if (inner.type == ConfChange::kPromoteLearner) {
      for (auto& p : opts_.peers) {
        if (p.id == inner.peer.id) {
          p.learner = false;
          if (role_ == Role::kLeader && p.id != opts_.self_id) {
            peer_quiet_ticks_[p.id] = 0;
          }
        }
      }
    } else if (inner.type == ConfChange::kRemoveNode) {
      // 仍留在 peers，LeaveJoint 时删除；先从 new 中去掉
    } else if (inner.type == ConfChange::kAddNode || inner.type == ConfChange::kAddLearner) {
      Peer peer = inner.peer;
      peer.learner = (inner.type == ConfChange::kAddLearner);
      add_peer(peer);
    }
    joint_new_voters_.clear();
    for (const auto& p : opts_.peers) {
      if (p.learner) continue;
      if (inner.type == ConfChange::kRemoveNode && p.id == inner.peer.id) continue;
      joint_new_voters_.push_back(p.id);
    }
    if (inner.type == ConfChange::kPromoteLearner) {
      bool found = false;
      for (const auto& id : joint_new_voters_) {
        if (id == inner.peer.id) found = true;
      }
      if (!found) joint_new_voters_.push_back(inner.peer.id);
    }
    in_joint_ = true;
    if (role_ == Role::kLeader) auto_leave_pending_ = true;
    SKV_INFO << "shard=" << opts_.shard_id << " enter joint old=" << joint_old_voters_.size()
             << " new=" << joint_new_voters_.size();
    return;
  }

  // 单步（learner 增删 / 兼容旧 AddNode）
  Peer peer = cc.peer;
  peer.learner = (cc.type == ConfChange::kAddLearner);
  if (cc.type == ConfChange::kAddNode || cc.type == ConfChange::kAddLearner) {
    add_peer(peer);
  } else if (cc.type == ConfChange::kRemoveNode) {
    remove_peer(peer.id);
  } else if (cc.type == ConfChange::kPromoteLearner) {
    for (auto& p : opts_.peers) {
      if (p.id == peer.id) {
        p.learner = false;
        if (role_ == Role::kLeader && p.id != opts_.self_id) {
          peer_quiet_ticks_[p.id] = 0;
        }
      }
    }
  }
}

void RaftNode::apply_committed() {
  while (apply_scheduled_ < commit_index_) {
    const uint64_t idx = apply_scheduled_ + 1;
    if (idx < log_.first_index()) {
      apply_scheduled_ = idx;
      if (idx > last_applied_) last_applied_ = idx;
      continue;
    }
    auto e = log_.entry(idx);
    if (!e) {
      apply_scheduled_ = idx;
      continue;
    }
    const bool is_conf =
        !e->data.empty() && static_cast<unsigned char>(e->data[0]) == 0xFF;
    if (opts_.external_io) {
      // ConfChange 仍改协议配置；普通条目经 Ready 外提，last_applied 在 advance 时推进。
      if (is_conf) apply_conf_entry(*e);
      ready_committed_.push_back(*e);
      apply_scheduled_ = idx;
      if (is_conf) last_applied_ = idx;
      continue;
    }
    if (is_conf) {
      if (opts_.enable_async_apply && idx > 0) {
        wait_apply_caught_up_to(idx - 1);
      }
      apply_conf_entry(*e);
      apply_scheduled_ = idx;
      last_applied_ = idx;
      continue;
    }
    if (opts_.enable_async_apply) {
      if (!enqueue_apply(idx, e->data)) {
        // 背压：停止调度，commit 仍前进
        break;
      }
      apply_scheduled_ = idx;
      // last_applied_ 由 worker 推进
    } else {
      if (on_apply_) on_apply_(idx, e->data);
      apply_scheduled_ = idx;
      last_applied_ = idx;
    }
  }
  if (!opts_.enable_async_apply && !opts_.external_io) maybe_compact();
}

void RaftNode::maybe_compact() {
  if (opts_.compact_threshold == 0) return;
  if (!snap_provider_) return;
  if (log_.size() <= opts_.compact_threshold) return;
  if (last_applied_ < log_.first_index()) return;
  const uint64_t old_first = log_.first_index();
  uint64_t term = log_.snapshot_term();
  if (auto e = log_.entry(last_applied_)) term = e->term;
  log_.compact_to(last_applied_, term);
  // 前缀删除交给 engine_flush_locked 的 cut；避免与 flush 双删竞态。
  (void)old_first;
  persist_locked();
  SKV_INFO << "shard=" << opts_.shard_id << " compacted to index=" << last_applied_
           << " first=" << log_.first_index();
}

void RaftNode::on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                                 uint64_t prev_term, const std::vector<LogEntry>& entries,
                                 uint64_t leader_commit, bool* success, uint64_t* match_index) {
  std::lock_guard<std::mutex> lock(mu_);
  *success = false;
  *match_index = 0;
  if (term < term_) return;
  step_down_if_stale(term);
  become_follower(term_, from);
  election_elapsed_ = 0;
  in_pre_vote_ = false;
  if (!log_.append_entries(prev_idx, prev_term, entries)) {
    persist_locked();
    return;
  }
  if (opts_.external_io && !entries.empty()) {
    for (const auto& e : entries) {
      if (auto got = log_.entry(e.index)) ready_unstable_.push_back(*got);
    }
  }
  *success = true;
  *match_index = prev_idx + entries.size();
  if (leader_commit > commit_index_) {
    commit_index_ = std::min(leader_commit, log_.last_index());
  }
  apply_committed();
  persist_locked();
}

void RaftNode::on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                               uint64_t last_term, bool* granted) {
  std::lock_guard<std::mutex> lock(mu_);
  *granted = false;
  if (term < term_) return;
  step_down_if_stale(term);
  bool up_to_date = (last_term > log_.last_term()) ||
                    (last_term == log_.last_term() && last_idx >= log_.last_index());
  if ((voted_for_.empty() || voted_for_ == from) && up_to_date) {
    voted_for_ = from;
    *granted = true;
    election_elapsed_ = 0;
    persist_locked();
  }
}

void RaftNode::on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term, bool* granted) {
  std::lock_guard<std::mutex> lock(mu_);
  *granted = false;
  (void)from;
  if (term < term_) return;
  if (!leader_id_.empty() && election_elapsed_ < election_timeout_ticks_) {
    return;
  }
  bool up_to_date = (last_term > log_.last_term()) ||
                    (last_term == log_.last_term() && last_idx >= log_.last_index());
  *granted = up_to_date;
}

void RaftNode::on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                                       uint64_t match_index) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    step_down_if_stale(term);
    if (role_ != Role::kLeader || term != term_) return;
    dec_inflight(from);
    if (success) {
      note_peer_progress(from);
      match_index_[from] = match_index;
      next_index_[from] = match_index + 1;
      // 刷新 lease（多数派有进展）
      lease_expire_ = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(opts_.leader_lease_ms);
      note_read_index_ack(from);
      maybe_commit();
      apply_committed();
      persist_locked();
      // 仅当该 peer 仍有未复制条目时继续追赶；已追平则交给周期性 heartbeat。
      if (next_index_[from] <= log_.last_index()) {
        need_bcast_ = true;
        maybe_broadcast();
      }
    } else {
      // 对端可达（仅日志不匹配）：计入活跃，避免 check_quorum 误 step-down
      note_peer_progress(from);
      if (next_index_[from] > 0) --next_index_[from];
      broadcast_heartbeat();
      need_bcast_ = false;
    }
  }
  flush_transport();
}

void RaftNode::on_request_vote_reply(const std::string& from, uint64_t term, bool granted) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    step_down_if_stale(term);
    if (role_ != Role::kCandidate || term != term_) return;
    votes_[from] = granted;
    if (votes_won(votes_)) {
      become_leader();
      persist_locked();
    }
  }
  flush_transport();
}

void RaftNode::on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!in_pre_vote_) return;
    if (term > term_) {
      become_follower(term);
      persist_locked();
      return;
    }
    pre_votes_[from] = granted;
    if (votes_won(pre_votes_)) {
      SKV_INFO << "shard=" << opts_.shard_id << " pre-vote won; starting real election";
      become_candidate();
      start_election();
      persist_locked();
    }
  }
  flush_transport();
}

void RaftNode::on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                                   uint64_t last_term, std::string_view data, bool* success) {
  std::lock_guard<std::mutex> lock(mu_);
  *success = false;
  if (term < term_) {
    // 空日志 learner：允许用快照对齐（term 落后常见于入队延迟），并采纳对方 term
    if (!(self_is_learner() && log_.last_index() + 1 <= log_.first_index() &&
          last_index >= log_.first_index())) {
      SKV_WARN << "shard=" << opts_.shard_id << " reject snapshot term=" << term
               << " local_term=" << term_ << " from=" << from << " last_idx=" << last_index;
      return;
    }
    term_ = term;
    voted_for_.clear();
  }
  step_down_if_stale(term);
  become_follower(term_, from);
  election_elapsed_ = 0;

  std::vector<Peer> snap_conf;
  std::string_view kv = data;
  peel_snap_conf(data, &snap_conf, &kv);

  if (snap_applier_) {
    if (!snap_applier_(last_index, last_term, kv)) {
      SKV_WARN << "shard=" << opts_.shard_id << " snap_applier failed from=" << from
               << " last_idx=" << last_index << " bytes=" << kv.size();
      return;
    }
  } else if (!kv.empty() && !opts_.external_io) {
    SKV_WARN << "shard=" << opts_.shard_id << " InstallSnapshot with data but no snap_applier";
    return;
  }
  // 恢复成员表：快照窗口内的 ConfChange 不会再 apply，必须靠 ConfState。
  if (!snap_conf.empty()) {
    opts_.peers = std::move(snap_conf);
    for (const auto& p : opts_.peers) {
      if (opts_.on_peer_addr && !p.address.empty()) opts_.on_peer_addr(p.id, p.address);
    }
    SKV_INFO << "shard=" << opts_.shard_id << " snapshot restored conf peers="
             << opts_.peers.size();
  }
  // external_io：外层已安装状态机；此处只对齐日志窗口。
  // 排空异步 apply，避免与快照冲突
  if (opts_.enable_async_apply) wait_apply_caught_up_to(last_applied_);
  log_.compact_to(last_index, last_term);
  if (commit_index_ < last_index) commit_index_ = last_index;
  last_applied_ = last_index;
  apply_scheduled_ = last_index;
  persist_locked();
  *success = true;
  SKV_INFO << "shard=" << opts_.shard_id << " installed snapshot last_idx=" << last_index
           << " bytes=" << kv.size();
}

void RaftNode::on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                         uint64_t match_index) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    // 无论 term/角色是否匹配，先清 snap_inflight，避免追平永久卡住。
    dec_inflight(from);
    step_down_if_stale(term);
    if (role_ != Role::kLeader || term != term_) {
      SKV_WARN << "shard=" << opts_.shard_id << " InstallSnapshot reply ignored from=" << from
               << " success=" << success << " match=" << match_index << " reply_term=" << term
               << " self_term=" << term_ << " role=" << static_cast<int>(role_);
      return;
    }
    if (success) {
      note_peer_progress(from);
      match_index_[from] = match_index;
      next_index_[from] = match_index + 1;
      SKV_INFO << "shard=" << opts_.shard_id << " InstallSnapshot reply ok from=" << from
               << " match=" << match_index;
      maybe_commit();
      apply_committed();
      persist_locked();
      need_bcast_ = true;
    } else {
      // 安装失败（对端 term/状态）：保持 next 在快照窗口，下轮继续发
      note_peer_progress(from);
      next_index_[from] = 0;
      need_bcast_ = true;
    }
  }
  flush_transport();
}

void RaftNode::add_peer(const Peer& p) {
  for (auto& existing : opts_.peers) {
    if (existing.id != p.id) continue;
    if (!p.address.empty()) existing.address = p.address;
    // 重试 ADD_LEARNER：若仍未追上，重置为快照窗口并催复制
    if (role_ == Role::kLeader) {
      auto mit = match_index_.find(p.id);
      if (mit == match_index_.end() || mit->second == 0) {
        next_index_[p.id] = 0;
        match_index_[p.id] = 0;
        need_bcast_ = true;
      }
    }
    if (opts_.on_peer_addr && !p.address.empty()) {
      opts_.on_peer_addr(p.id, p.address);
    }
    return;
  }
  opts_.peers.push_back(p);
  // 新 peer 空日志：一律先走快照（next < first），避免 AE 探测过慢 / compact 窗口不匹配
  next_index_[p.id] = 0;
  match_index_[p.id] = 0;
  if (role_ == Role::kLeader) need_bcast_ = true;
  if (opts_.on_peer_addr && !p.address.empty()) {
    opts_.on_peer_addr(p.id, p.address);
  }
}

void RaftNode::remove_peer(const std::string& id) {
  opts_.peers.erase(std::remove_if(opts_.peers.begin(), opts_.peers.end(),
                                   [&](const Peer& p) { return p.id == id; }),
                    opts_.peers.end());
  next_index_.erase(id);
  match_index_.erase(id);
  inflight_.erase(id);
  snap_inflight_.erase(id);
  probe_inflight_.erase(id);
  peer_send_blocked_.erase(id);
  peer_quiet_ticks_.erase(id);
}

}  // namespace opskv::raft
