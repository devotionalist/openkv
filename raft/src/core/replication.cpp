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

// @file replication.cpp
// 心跳/AE、commit、InstallSnapshot 入站出站。

#include "openkv/raft/core/replication.hpp"

#include <chrono>
#include <sstream>

#include "openkv/logging.hpp"
#include "openkv/raft/core/apply.hpp"
#include "openkv/raft/core/election.hpp"
#include "openkv/raft/core/linear_read.hpp"
#include "openkv/raft/core/log_persist.hpp"
#include "openkv/raft/core/membership.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/ready_pump.hpp"
#include "openkv/raft/util/snap_stream.hpp"

namespace openkv::raft {
namespace {

std::string encode_peers_csv(const std::vector<Peer>& peers) {
  std::ostringstream oss;
  for (size_t i = 0; i < peers.size(); ++i) {
    if (i) {
      oss << ',';
    }
    oss << peers[i].id << '@' << peers[i].address;
    if (peers[i].learner) {
      oss << "*N";
    }
  }
  return oss.str();
}

std::string wrap_snap_conf(const std::string& conf_csv, std::string kv) {
  if (conf_csv.empty()) {
    return kv;
  }
  return std::string(kMagicSnapConf) + conf_csv + "\n" + std::move(kv);
}

bool parse_peers_csv(std::string_view csv, std::vector<Peer>* out) {
  if (!out) {
    return false;
  }
  out->clear();
  size_t i = 0;
  while (i < csv.size()) {
    size_t j = csv.find(',', i);
    if (j == std::string_view::npos) {
      j = csv.size();
    }
    auto item = csv.substr(i, j - i);
    i = j + 1;
    if (item.empty()) {
      continue;
    }
    bool learner = false;
    if (item.size() >= 2 && item.substr(item.size() - 2) == "*N") {
      learner = true;
      item.remove_suffix(2);
    }
    const auto at = item.find('@');
    if (at == std::string_view::npos || at == 0) {
      continue;
    }
    Peer p;
    p.id = std::string(item.substr(0, at));
    p.address = std::string(item.substr(at + 1));
    p.learner = learner;
    out->push_back(std::move(p));
  }
  return !out->empty();
}

void peel_snap_conf(std::string_view data, std::vector<Peer>* conf, std::string_view* kv) {
  if (conf) {
    conf->clear();
  }
  if (!data.starts_with(kMagicSnapConf)) {
    if (kv) {
      *kv = data;
    }
    return;
  }
  const auto rest = data.substr(kMagicSnapConf.size());
  const auto nl = rest.find('\n');
  if (nl == std::string_view::npos) {
    if (kv) {
      *kv = data;
    }
    return;
  }
  if (conf) {
    parse_peers_csv(rest.substr(0, nl), conf);
  }
  if (kv) {
    *kv = rest.substr(nl + 1);
  }
}

}  // namespace

void Replication::dec_inflight(const std::string& peer_id) {
  // 传输失败：AE / probe / snap 名额一并放下，允许重发。
  auto it = n_.progress_.inflight_.find(peer_id);
  if (it != n_.progress_.inflight_.end() && it->second > 0) {
    --it->second;
  }
  n_.progress_.snap_inflight_.erase(peer_id);
  n_.progress_.probe_inflight_.erase(peer_id);
}

void Replication::dec_ae_inflight(const std::string& peer_id) {
  // AE/probe 应答：只还 AE 名额，绝不能清 snap_inflight_。
  // 否则「先 probe 再发快照」时，迟到的 AE 应答会误放开快照在途标记，
  // 导致重复 InstallSnapshot、worker 堵死、陈旧 match 把 next_index 打回窗口外。
  const bool was_probe = n_.progress_.probe_inflight_.erase(peer_id) > 0;
  if (was_probe) {
    return;
  }
  if (n_.progress_.snap_inflight_.count(peer_id)) {
    return;
  }
  auto it = n_.progress_.inflight_.find(peer_id);
  if (it != n_.progress_.inflight_.end() && it->second > 0) {
    --it->second;
  }
}

void Replication::dec_snap_inflight(const std::string& peer_id) {
  auto it = n_.progress_.inflight_.find(peer_id);
  if (it != n_.progress_.inflight_.end() && it->second > 0) {
    --it->second;
  }
  n_.progress_.snap_inflight_.erase(peer_id);
}

bool Replication::try_inc_inflight(const std::string& peer_id) {
  auto& cnt = n_.progress_.inflight_[peer_id];
  if (cnt >= n_.opts_.max_inflight_msgs) {
    return false;
  }
  ++cnt;
  return true;
}

// Leader 且被标了 need_replicate_ 时，立刻推一轮 AE/快照（不等下一个 heartbeat tick）。
void Replication::maybe_replicate() {
  if (n_.role_ != Role::kLeader || !need_replicate_) {
    return;
  }
  broadcast_replicate();
  need_replicate_ = false;
}

// Leader：在多数派已复制的前提下抬 commit_index（Joint 要双多数）。
void Replication::maybe_commit() {
  if (n_.role_ != Role::kLeader) {
    return;
  }
  uint64_t lo = n_.commit_index_ + 1;
  uint64_t hi = n_.log_.last_index();
  uint64_t best = n_.commit_index_;
  for (uint64_t cand = hi; cand >= lo; --cand) {
    bool ok;
    if (n_.membership_.in_joint_) {
      ok = n_.membership_.quorate_at(n_.membership_.joint_old_voters_, cand) &&
           n_.membership_.quorate_at(n_.membership_.joint_new_voters_, cand);
    } else {
      ok = n_.membership_.quorate_at(n_.membership_.voter_ids(), cand);
    }
    if (!ok) {
      continue;
    }
    auto e = n_.log_.entry(cand);
    if (e && e->term == n_.term_) {
      best = std::max(best, cand);  // 更新当前提交索引
      break;
    }
  }
  // 更新当前提交索引
  if (best > n_.commit_index_) {
    n_.commit_index_ = best;
  }
}

// 对每个 peer 推一轮（只入队；发网交给 Ready/Store）。
// 掉窗→快照；有条目→AE；否则空 AE（probe/心跳）。
void Replication::broadcast_replicate() {
  // —— 0. 开场：上一轮因 inflight 满被挡的，本轮重试 ——
  n_.progress_.peer_send_blocked_.clear();

  for (const auto& p : n_.opts_.peers) {
    // —— 1. 跳过自己 ——
    if (p.id == n_.opts_.self_id) {
      continue;
    }

    // —— 2. 分支 A：next 掉出日志窗 → InstallSnapshot ——
    if (n_.progress_.next_index_[p.id] < n_.log_.first_index()) {
      // 2.1 嵌入模式且没有 snap_provider，发不了快照
      if (!n_.opts_.external_io && !n_.snap_provider_) {
        SKV_WARN << "shard=" << n_.opts_.shard_id << " peer=" << p.id
                 << " behind compact window but no snap_provider; skip InstallSnapshot";
        continue;
      }
      // 2.2 快照已在途，勿重复发
      if (n_.progress_.snap_inflight_.count(p.id)) {
        continue;
      }
      // 2.3 距上次发快照太近，限流
      if (n_.opts_.snapshot_min_interval_ms > 0) {
        const auto now = std::chrono::steady_clock::now();
        auto it = last_snap_sent_.find(p.id);
        if (it != last_snap_sent_.end() &&
            now - it->second < std::chrono::milliseconds(n_.opts_.snapshot_min_interval_ms)) {
          continue;
        }
      }
      // 2.4 在途配额满 → 本轮挡住，下一轮再试
      if (!try_inc_inflight(p.id)) {
        n_.progress_.peer_send_blocked_.insert(p.id);
        continue;
      }

      // 2.5 记下快照在途，拼 last_idx/term + 成员表
      // 在途 probe 与快照无关；清掉以免迟到 AE 应答误碰名额（见 dec_ae_inflight）。
      n_.progress_.probe_inflight_.erase(p.id);
      n_.progress_.snap_inflight_.insert(p.id);
      uint64_t last_idx = n_.last_applied_ > 0 ? n_.last_applied_ : (n_.log_.first_index() - 1);
      uint64_t last_term = n_.log_.snapshot_term();
      if (auto e = n_.log_.entry(last_idx)) {
        last_term = e->term;
      }
      const std::string conf_csv = encode_peers_csv(n_.opts_.peers);

      // 2.6 入队：external_io 只塞 meta 壳（blob 给 SnapWorker）；否则当场打包
      if (n_.opts_.external_io) {
        SnapshotMeta meta;
        meta.index = last_idx;
        meta.term = last_term;
        meta.to = p.id;
        meta.conf_state = conf_csv;
        n_.ready_io_.ready_snaps_.push_back(meta);
        InstallSnapshotReq req;
        req.term = n_.term_;
        req.last_index = last_idx;
        req.last_term = last_term;
        n_.ready_pump_.enqueue(p.id, MsgType::kInstallSnapshot, std::move(req));
      } else {
        std::string data = wrap_snap_conf(conf_csv, n_.snap_provider_(&last_idx, &last_term));
        InstallSnapshotReq req;
        req.term = n_.term_;
        req.last_index = last_idx;
        req.last_term = last_term;
        req.data = std::move(data);
        SKV_INFO << "shard=" << n_.opts_.shard_id << " send snapshot to=" << p.id
                 << " last_idx=" << last_idx << " bytes=" << req.data.size()
                 << " conf_peers=" << n_.opts_.peers.size();
        n_.ready_pump_.enqueue(p.id, MsgType::kInstallSnapshot, std::move(req));
      }
      // 2.7 记发送时间与计数；此人本轮不再走 AE
      last_snap_sent_[p.id] = std::chrono::steady_clock::now();
      ++snapshot_send_count_;
      continue;
    }

    // —— 3. 分支 B：仍在日志窗 → AppendEntries ——
    // 3.1 对齐点 prev = next-1（及其 term；边界用 snapshot_term）
    uint64_t prev = n_.progress_.next_index_[p.id] > 0 ? n_.progress_.next_index_[p.id] - 1 : 0;
    uint64_t prev_term = 0;
    if (prev == n_.log_.first_index() - 1) {
      prev_term = n_.log_.snapshot_term();
    } else if (auto e = n_.log_.entry(prev)) {
      prev_term = e->term;
    }
    // 3.2 从 next 取一批条目（条数/字节上限）
    auto ents = n_.log_.entries_from(n_.progress_.next_index_[p.id], n_.opts_.max_ae_entries,
                                     n_.opts_.max_ae_bytes);
    // 3.3 空批次 = probe/心跳；有条目则占 inflight
    const bool probe = ents.empty();
    if (probe) {
      if (n_.progress_.probe_inflight_.count(p.id)) {
        continue;
      }
      n_.progress_.probe_inflight_.insert(p.id);
    } else if (!try_inc_inflight(p.id)) {
      n_.progress_.peer_send_blocked_.insert(p.id);
      continue;
    }
    // 3.4 组结构体并入队 AppendEntries（entries 二进制直传，不拼 hex）
    AppendEntriesReq req;
    req.term = n_.term_;
    req.prev_log_index = prev;
    req.prev_log_term = prev_term;
    req.leader_commit = n_.commit_index_;
    req.entries = std::move(ents);
    n_.ready_pump_.enqueue(p.id, MsgType::kAppendEntries, std::move(req));
  }
  // —— 4. 结束：真正发网在 flush_transport / Store take_ready ——
}

void Replication::on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                                    uint64_t prev_term, const std::vector<LogEntry>& entries,
                                    uint64_t leader_commit, bool* success, uint64_t* match_index) {
  *success = false;
  *match_index = 0;
  if (term < n_.term_) {
    return;
  }
  n_.election_.step_down_if_stale(term);
  n_.election_.become_follower(n_.term_, from);
  n_.election_.election_elapsed_ = 0;
  n_.election_.in_pre_vote_ = false;
  if (!n_.log_.append_entries(prev_idx, prev_term, entries)) {
    n_.log_persist_.persist_locked();
    return;
  }
  if (n_.opts_.external_io && !entries.empty()) {
    for (const auto& e : entries) {
      if (auto got = n_.log_.entry(e.index)) {
        n_.ready_io_.ready_unstable_.push_back(*got);
      }
    }
  }
  *success = true;
  *match_index = prev_idx + entries.size();
  if (leader_commit > n_.commit_index_) {
    n_.commit_index_ = std::min(leader_commit, n_.log_.last_index());
  }
  n_.apply_.apply_committed();
  n_.log_persist_.persist_locked();
}

void Replication::on_append_entries_reply_locked(const std::string& from, uint64_t term,
                                                 bool success, uint64_t match_index) {
  n_.election_.step_down_if_stale(term);
  if (n_.role_ != Role::kLeader || term != n_.term_) {
    return;
  }
  dec_ae_inflight(from);
  if (success) {
    n_.election_.note_peer_progress(from);
    n_.progress_.match_index_[from] = match_index;
    n_.progress_.next_index_[from] = match_index + 1;
    n_.linear_read_.renew_lease_locked();
    n_.linear_read_.note_read_index_ack(from);
    maybe_commit();
    n_.apply_.apply_committed();
    n_.log_persist_.persist_locked();
    if (n_.progress_.next_index_[from] <= n_.log_.last_index()) {
      need_replicate_ = true;
      maybe_replicate();
    }
  } else {
    n_.election_.note_peer_progress(from);
    if (n_.progress_.next_index_[from] > 0) {
      --n_.progress_.next_index_[from];
    }
    broadcast_replicate();
    need_replicate_ = false;
  }
}

void Replication::on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                                      uint64_t last_term, std::string_view data, bool* success) {
  *success = false;
  if (term < n_.term_) {
    if (!(n_.self_is_learner() && n_.log_.last_index() + 1 <= n_.log_.first_index() &&
          last_index >= n_.log_.first_index())) {
      SKV_WARN << "shard=" << n_.opts_.shard_id << " reject snapshot term=" << term
               << " local_term=" << n_.term_ << " from=" << from << " last_idx=" << last_index;
      return;
    }
    n_.term_ = term;
    n_.voted_for_.clear();
  }
  n_.election_.step_down_if_stale(term);
  n_.election_.become_follower(n_.term_, from);
  n_.election_.election_elapsed_ = 0;

  std::vector<Peer> snap_conf;
  std::string_view kv = data;
  peel_snap_conf(data, &snap_conf, &kv);

  if (n_.snap_applier_) {
    if (!n_.snap_applier_(last_index, last_term, kv)) {
      SKV_WARN << "shard=" << n_.opts_.shard_id << " snap_applier failed from=" << from
               << " last_idx=" << last_index << " bytes=" << kv.size();
      return;
    }
  } else if (!kv.empty() && !n_.opts_.external_io) {
    SKV_WARN << "shard=" << n_.opts_.shard_id << " InstallSnapshot with data but no snap_applier";
    return;
  }
  if (!snap_conf.empty()) {
    n_.opts_.peers = std::move(snap_conf);
    for (const auto& p : n_.opts_.peers) {
      if (n_.opts_.on_peer_addr && !p.address.empty()) {
        n_.opts_.on_peer_addr(p.id, p.address);
      }
    }
    SKV_INFO << "shard=" << n_.opts_.shard_id
             << " snapshot restored conf peers=" << n_.opts_.peers.size();
  }
  n_.log_.compact_to(last_index, last_term);
  if (n_.commit_index_ < last_index) {
    n_.commit_index_ = last_index;
  }
  n_.last_applied_ = last_index;
  n_.apply_scheduled_ = last_index;
  n_.log_persist_.persist_locked();
  *success = true;
  SKV_INFO << "shard=" << n_.opts_.shard_id << " installed snapshot last_idx=" << last_index
           << " bytes=" << kv.size();
}

void Replication::on_install_snapshot_reply_locked(const std::string& from, uint64_t term,
                                                   bool success, uint64_t match_index) {
  dec_snap_inflight(from);
  n_.election_.step_down_if_stale(term);
  if (n_.role_ != Role::kLeader || term != n_.term_) {
    SKV_WARN << "shard=" << n_.opts_.shard_id << " InstallSnapshot reply ignored from=" << from
             << " success=" << success << " match=" << match_index << " reply_term=" << term
             << " self_term=" << n_.term_ << " role=" << static_cast<int>(n_.role_);
    return;
  }
  if (success) {
    n_.election_.note_peer_progress(from);
    // 迟到的旧快照应答不得把 next/match 打回更低，否则立刻又掉出 compact 窗。
    const uint64_t cur_match = n_.progress_.match_index_[from];
    if (match_index < cur_match) {
      SKV_WARN << "shard=" << n_.opts_.shard_id << " InstallSnapshot stale reply from=" << from
               << " match=" << match_index << " cur_match=" << cur_match << "; keep progress";
      need_replicate_ = true;
      return;
    }
    n_.progress_.match_index_[from] = match_index;
    n_.progress_.next_index_[from] = match_index + 1;
    SKV_INFO << "shard=" << n_.opts_.shard_id << " InstallSnapshot reply ok from=" << from
             << " match=" << match_index;
    maybe_commit();
    n_.apply_.apply_committed();
    n_.log_persist_.persist_locked();
    need_replicate_ = true;
  } else {
    n_.election_.note_peer_progress(from);
    n_.progress_.next_index_[from] = 0;
    need_replicate_ = true;
  }
}

}  // namespace openkv::raft
