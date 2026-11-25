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

// @file membership.cpp
// Joint / ConfChange / 多数派 / add_remove peer。

#include "openkv/raft/core/membership.hpp"

#include <algorithm>
#include <unordered_set>

#include "openkv/logging.hpp"
#include "openkv/raft/core/election.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/replication.hpp"

namespace openkv::raft {

std::vector<std::string> Membership::voter_ids() const {
  std::vector<std::string> out;
  for (const auto& p : n_.opts_.peers) {
    if (!p.learner) {
      out.push_back(p.id);
    }
  }
  return out;
}

uint32_t Membership::majority_of(const std::vector<std::string>& voters) const {
  size_t n = voters.empty() ? 1 : voters.size();
  return static_cast<uint32_t>(n / 2 + 1);
}

uint32_t Membership::majority() const {
  if (in_joint_) {
    return std::max(majority_of(joint_old_voters_), majority_of(joint_new_voters_));
  }
  return majority_of(voter_ids());
}

bool Membership::quorate_at(const std::vector<std::string>& voters, uint64_t index) const {
  uint32_t ok = 0;
  for (const auto& id : voters) {
    uint64_t m = 0;
    if (id == n_.opts_.self_id) {
      m = n_.progress_.match_index_.count(n_.opts_.self_id)
              ? n_.progress_.match_index_.at(n_.opts_.self_id)
              : n_.log_.last_index();
    } else {
      auto it = n_.progress_.match_index_.find(id);
      m = it == n_.progress_.match_index_.end() ? 0 : it->second;
    }
    if (m >= index) {
      ++ok;
    }
  }
  return ok >= majority_of(voters);
}

bool Membership::encode_conf(const ConfChange& cc, std::string* out) const {
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

bool Membership::decode_conf(std::string_view data, ConfChange* cc) {
  if (data.size() < 3 || static_cast<unsigned char>(data[0]) != 0xFF) {
    return false;
  }
  auto type = static_cast<ConfChange::Type>(data[1]);
  size_t id_off = 2;
  if (type == ConfChange::kEnterJoint) {
    if (data.size() < 4) {
      return false;
    }
    cc->type = ConfChange::kEnterJoint;
    cc->joint_change = static_cast<ConfChange::Type>(data[2]);
    id_off = 3;
  } else {
    cc->type = type;
    cc->joint_change = type;
  }
  auto nul = data.find('\0', id_off);
  if (nul == std::string_view::npos) {
    return false;
  }
  cc->peer.id = std::string(data.substr(id_off, nul - id_off));
  cc->peer.address = std::string(data.substr(nul + 1));
  cc->peer.learner =
      (cc->type == ConfChange::kAddLearner ||
       (cc->type == ConfChange::kEnterJoint && cc->joint_change == ConfChange::kAddLearner));
  return true;
}

void Membership::maybe_auto_leave_joint() {
  if (!auto_leave_pending_ || !in_joint_ || n_.role_ != Role::kLeader) {
    return;
  }
  for (uint64_t i = n_.commit_index_ + 1; i <= n_.log_.last_index(); ++i) {
    auto e = n_.log_.entry(i);
    if (!e || e->term != n_.term_) {
      continue;
    }
    ConfChange cc;
    if (decode_conf(e->data, &cc) && cc.type == ConfChange::kLeaveJoint) {
      leave_joint_inflight_ = true;
      n_.replication_.need_replicate_ = true;
      return;
    }
  }
  if (leave_joint_inflight_) {
    leave_joint_inflight_ = false;
  }
  leave_joint_inflight_ = true;
  ConfChange leave;
  leave.type = ConfChange::kLeaveJoint;
  std::string payload;
  encode_conf(leave, &payload);
  auto idx = n_.log_.append(n_.term_, std::move(payload));
  if (n_.opts_.external_io) {
    if (auto e = n_.log_.entry(idx)) {
      n_.ready_io_.ready_unstable_.push_back(*e);
    }
  }
  n_.progress_.match_index_[n_.opts_.self_id] = idx;
  n_.replication_.need_replicate_ = true;
  n_.persist_locked();
  SKV_INFO << "shard=" << n_.opts_.shard_id << " auto LeaveJoint proposed idx=" << idx;
}

void Membership::apply_conf_entry(const LogEntry& e) {
  ConfChange cc;
  if (!decode_conf(e.data, &cc)) {
    return;
  }
  if (cc.type == ConfChange::kLeaveJoint) {
    if (in_joint_) {
      std::unordered_set<std::string> keep(joint_new_voters_.begin(), joint_new_voters_.end());
      n_.opts_.peers.erase(std::remove_if(n_.opts_.peers.begin(), n_.opts_.peers.end(),
                                          [&](const Peer& p) {
                                            if (p.learner) {
                                              return false;
                                            }
                                            return !keep.count(p.id);
                                          }),
                           n_.opts_.peers.end());
      in_joint_ = false;
      joint_old_voters_.clear();
      joint_new_voters_.clear();
      auto_leave_pending_ = false;
      leave_joint_inflight_ = false;
      SKV_INFO << "shard=" << n_.opts_.shard_id
               << " left joint config voters=" << voter_ids().size();
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
      for (auto& p : n_.opts_.peers) {
        if (p.id == inner.peer.id) {
          p.learner = false;
          if (n_.role_ == Role::kLeader && p.id != n_.opts_.self_id) {
            n_.election_.peer_quiet_ticks_[p.id] = 0;
          }
        }
      }
    } else if (inner.type == ConfChange::kRemoveNode) {
      // 仍留在 peers，LeaveJoint 时删除
    } else if (inner.type == ConfChange::kAddNode || inner.type == ConfChange::kAddLearner) {
      Peer peer = inner.peer;
      peer.learner = (inner.type == ConfChange::kAddLearner);
      add_peer(peer);
    }
    joint_new_voters_.clear();
    for (const auto& p : n_.opts_.peers) {
      if (p.learner) {
        continue;
      }
      if (inner.type == ConfChange::kRemoveNode && p.id == inner.peer.id) {
        continue;
      }
      joint_new_voters_.push_back(p.id);
    }
    if (inner.type == ConfChange::kPromoteLearner) {
      bool found = false;
      for (const auto& id : joint_new_voters_) {
        if (id == inner.peer.id) {
          found = true;
        }
      }
      if (!found) {
        joint_new_voters_.push_back(inner.peer.id);
      }
    }
    in_joint_ = true;
    if (n_.role_ == Role::kLeader) {
      auto_leave_pending_ = true;
    }
    SKV_INFO << "shard=" << n_.opts_.shard_id << " enter joint old=" << joint_old_voters_.size()
             << " new=" << joint_new_voters_.size();
    return;
  }

  Peer peer = cc.peer;
  peer.learner = (cc.type == ConfChange::kAddLearner);
  if (cc.type == ConfChange::kAddNode || cc.type == ConfChange::kAddLearner) {
    add_peer(peer);
  } else if (cc.type == ConfChange::kRemoveNode) {
    remove_peer(peer.id);
  } else if (cc.type == ConfChange::kPromoteLearner) {
    for (auto& p : n_.opts_.peers) {
      if (p.id == peer.id) {
        p.learner = false;
        if (n_.role_ == Role::kLeader && p.id != n_.opts_.self_id) {
          n_.election_.peer_quiet_ticks_[p.id] = 0;
        }
      }
    }
  }
}

void Membership::add_peer(const Peer& p) {
  for (auto& existing : n_.opts_.peers) {
    if (existing.id != p.id) {
      continue;
    }
    if (!p.address.empty()) {
      existing.address = p.address;
    }
    if (n_.role_ == Role::kLeader) {
      auto mit = n_.progress_.match_index_.find(p.id);
      if (mit == n_.progress_.match_index_.end() || mit->second == 0) {
        n_.progress_.next_index_[p.id] = 0;
        n_.progress_.match_index_[p.id] = 0;
        n_.replication_.need_replicate_ = true;
      }
    }
    if (n_.opts_.on_peer_addr && !p.address.empty()) {
      n_.opts_.on_peer_addr(p.id, p.address);
    }
    return;
  }
  n_.opts_.peers.push_back(p);
  n_.progress_.next_index_[p.id] = 0;
  n_.progress_.match_index_[p.id] = 0;
  if (n_.role_ == Role::kLeader) {
    n_.replication_.need_replicate_ = true;
  }
  if (n_.opts_.on_peer_addr && !p.address.empty()) {
    n_.opts_.on_peer_addr(p.id, p.address);
  }
}

void Membership::remove_peer(const std::string& id) {
  n_.opts_.peers.erase(std::remove_if(n_.opts_.peers.begin(), n_.opts_.peers.end(),
                                      [&](const Peer& p) { return p.id == id; }),
                       n_.opts_.peers.end());
  n_.progress_.next_index_.erase(id);
  n_.progress_.match_index_.erase(id);
  n_.progress_.inflight_.erase(id);
  n_.progress_.snap_inflight_.erase(id);
  n_.progress_.probe_inflight_.erase(id);
  n_.progress_.peer_send_blocked_.erase(id);
  n_.election_.peer_quiet_ticks_.erase(id);
}

}  // namespace openkv::raft
