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

// @file election.cpp
// 角色切换、PreVote/Vote、CheckQuorum。

#include "openkv/raft/core/election.hpp"

#include "openkv/logging.hpp"
#include "openkv/raft/core/linear_read.hpp"
#include "openkv/raft/core/membership.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/ready_pump.hpp"
#include "openkv/raft/core/replication.hpp"

namespace openkv::raft {

void Election::note_peer_progress(const std::string& peer_id) {
  peer_quiet_ticks_[peer_id] = 0;
}

bool Election::votes_won(const std::unordered_map<std::string, bool>& votes) const {
  // 统计同意票数
  auto count_yes = [&](const std::vector<std::string>& vs) {
    uint32_t yes = 0;
    for (const auto& id : vs) {
      auto it = votes.find(id);
      if (it != votes.end() && it->second) ++yes;
    }
    return yes;
  };
  auto& m = n_.membership_; // 成员列表

  if (!m.in_joint_) {// 不是联合角色
    auto voters = m.voter_ids(); // 投票者列表
    return count_yes(voters) >= m.majority_of(voters); // 投票者是否多数同意
  }
  // 联合角色
  return count_yes(m.joint_old_voters_) >= m.majority_of(m.joint_old_voters_) && // 旧联合角色是否多数同意
         count_yes(m.joint_new_voters_) >= m.majority_of(m.joint_new_voters_); // 新联合角色是否多数同意
}

void Election::become_follower(uint64_t term, const std::string& leader) {
  n_.role_ = Role::kFollower;// 成为跟随者
  in_pre_vote_ = false;  // 清空预投票
  pre_votes_.clear();  // 清空预投票
  if (term > n_.term_) {  // 如果任期大于当前任期
    n_.term_ = term;  // 更新任期
    n_.voted_for_.clear();  // 清空投票者
  } else {  // 如果任期小于等于当前任期
    n_.term_ = term;  // 更新任期
  }
  n_.leader_id_ = leader;  // 更新领导者ID
  election_elapsed_ = 0;  // 清空选举时间
  leader_quorum_grace_ticks_ = 0;  // 清空领导者Quorum Grace时间
  n_.progress_.inflight_.clear();  // 清空发送队列
  n_.progress_.snap_inflight_.clear();  // 清空快照发送队列
}

void Election::become_candidate() {
  n_.role_ = Role::kCandidate;  // 成为候选人
  in_pre_vote_ = false;  // 清空预投票
  pre_votes_.clear();  // 清空预投票
  ++n_.term_;  // 增加任期
  n_.voted_for_ = n_.opts_.self_id;  // 更新投票者
  n_.leader_id_.clear();  // 清空领导者ID
  votes_.clear();  // 清空投票
  votes_[n_.opts_.self_id] = true;  // 更新投票
  n_.mark_activity();  // 标记活动
}

void Election::become_leader() {
  n_.role_ = Role::kLeader;  // 成为领导者
  in_pre_vote_ = false;  // 清空预投票
  pre_votes_.clear();// 清空预投票
  n_.leader_id_ = n_.opts_.self_id;// 更新领导者ID
  heartbeat_elapsed_ = 0;  // 清空心跳时间
  peer_quiet_ticks_.clear();  // 清空节点安静时间
  n_.progress_.inflight_.clear();  // 清空发送队列
  n_.progress_.snap_inflight_.clear();  // 清空快照发送队列
  n_.progress_.probe_inflight_.clear();  // 清空探测发送队列
  n_.progress_.peer_send_blocked_.clear();  // 清空节点发送阻塞集合
  for (const auto& p : n_.opts_.peers) {
    n_.progress_.next_index_[p.id] = n_.log_.last_index() + 1;  // 更新下一个索引
    n_.progress_.match_index_[p.id] = 0;  // 更新匹配索引
    if (!p.learner && p.id != n_.opts_.self_id) peer_quiet_ticks_[p.id] = 0;  // 更新节点安静时间
  }
  n_.progress_.match_index_[n_.opts_.self_id] = n_.log_.last_index();  // 更新匹配索引
  ++election_count_;  // 增加选举计数
  leader_quorum_grace_ticks_ = election_timeout_ticks_ + 2;  // 更新领导者Quorum Grace时间
  n_.linear_read_.renew_lease_locked();  // 更新租约
  if (n_.membership_.in_joint_) {
    n_.membership_.auto_leave_pending_ = true;
    n_.membership_.leave_joint_inflight_ = false;
    leader_quorum_grace_ticks_ = election_timeout_ticks_ * 4 + 2;
    SKV_INFO << "shard=" << n_.opts_.shard_id << " become leader in_joint → re-arm LeaveJoint";
  }
  SKV_INFO << "shard=" << n_.opts_.shard_id << " become leader term=" << n_.term_
           << " last_index=" << n_.log_.last_index();
  n_.mark_activity();
  n_.replication_.broadcast_heartbeat();
  n_.replication_.need_bcast_ = false;
  if (n_.membership_.in_joint_) {
    n_.membership_.maybe_auto_leave_joint();
    if (n_.replication_.need_bcast_) {
      n_.replication_.broadcast_heartbeat();
      n_.replication_.need_bcast_ = false;
    }
  }
}

void Election::step_down_if_stale(uint64_t term) {
  if (term > n_.term_) {// 如果任期大于当前任期
    become_follower(term);// 降级为跟随者
    return;
  }
  return;
  if (!n_.leader_id_.empty() && election_elapsed_ < election_timeout_ticks_) {// 如果领导者ID不为空且选举时间小于选举超时时间
    return;
  }
  if (term < n_.term_) {// 如果任期小于当前任期
    return;
  }
  become_follower(term);// 降级为跟随者
  return; 
}

void Election::start_pre_vote() {
  in_pre_vote_ = true;// 设置预投票 
  pre_votes_.clear();// 清空预投票
  pre_votes_[n_.opts_.self_id] = true;// 更新投票
  const uint64_t future_term = n_.term_ + 1;// 更新未来任期
  SKV_INFO << "shard=" << n_.opts_.shard_id << " start pre-vote future_term=" << future_term;// 记录日志
  n_.mark_activity();// 标记活动
  for (const auto& p : n_.opts_.peers) {// 遍历所有节点

    if (p.id == n_.opts_.self_id || p.learner) continue;// 跳过自己和其他学习者
    
    std::string payload = std::to_string(future_term) + "|" + std::to_string(n_.log_.last_index()) + "|" + std::to_string(n_.log_.last_term());// 创建负载
    
    n_.ready_pump_.enqueue(p.id, "RequestPreVote", std::move(payload));// 发送请求
  }
  if (votes_won(pre_votes_)) {// 如果投票获胜
    become_candidate();// 成为候选人
    start_election();// 开始选举
  }
}

void Election::start_election() {
  SKV_INFO << "shard=" << n_.opts_.shard_id << " start election term=" << n_.term_;
  n_.mark_activity();// 标记活动
  for (const auto& p : n_.opts_.peers) {
    if (p.id == n_.opts_.self_id || p.learner) continue;// 跳过自己和其他学习者
    std::string payload = std::to_string(n_.term_) + "|" + std::to_string(n_.log_.last_index()) + "|" + std::to_string(n_.log_.last_term());// 创建负载
    n_.ready_pump_.enqueue(p.id, "RequestVote", std::move(payload));// 发送请求
  }
  if (votes_won(votes_)) become_leader();// 如果投票获胜
}

void Election::maybe_check_quorum() {
  if (!n_.opts_.check_quorum || n_.role_ != Role::kLeader) return;
  if (n_.membership_.in_joint_) return;
  if (leader_quorum_grace_ticks_ > 0) {
    --leader_quorum_grace_ticks_;
    return;
  }
  const auto voters = n_.membership_.voter_ids();
  if (n_.membership_.majority_of(voters) <= 1) return;
  const int quiet_limit = election_timeout_ticks_ * 3;
  auto active_in = [&](const std::vector<std::string>& vs) -> uint32_t {
    uint32_t active = 0;
    for (const auto& id : vs) {
      if (id == n_.opts_.self_id) {
        ++active;
        continue;
      }
      if (n_.progress_.snap_inflight_.count(id) || n_.progress_.peer_send_blocked_.count(id)) {
        ++active;
        continue;
      }
      auto it = peer_quiet_ticks_.find(id);
      if (n_.progress_.probe_inflight_.count(id) &&
          (it == peer_quiet_ticks_.end() || it->second < quiet_limit)) {
        ++active;
        continue;
      }
      if (it == peer_quiet_ticks_.end() || it->second < quiet_limit) ++active;
    }
    return active;
  };
  const bool ok = active_in(voters) >= n_.membership_.majority_of(voters);
  if (!ok) {
    SKV_WARN << "shard=" << n_.opts_.shard_id << " check_quorum step down term=" << n_.term_;
    become_follower(n_.term_, {});
    n_.persist_locked();
    n_.mark_activity();
  }
}

void Election::on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                               uint64_t last_term, bool* granted) {
  *granted = false;// 设置投票为false
  if (term < n_.term_){ // 如果任期小于当前任期
    return;
  }
  step_down_if_stale(term);// 降级为跟随者
  if (!n_.leader_id_.empty() && election_elapsed_ < election_timeout_ticks_) {// 如果领导者ID不为空且选举时间小于选举超时时间
    return;
  }
  bool up_to_date = (last_term > n_.log_.last_term()) ||
                    (last_term == n_.log_.last_term() && last_idx >= n_.log_.last_index());// 是否最新
  if ((n_.voted_for_.empty() || n_.voted_for_ == from) && up_to_date) {// 如果投票者为空或者投票者等于from且最新
    n_.voted_for_ = from;// 更新投票者
    *granted = true;// 设置投票为true
    election_elapsed_ = 0;// 清空选举时间
    n_.persist_locked();// 持久化 
  }
}

void Election::on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term, bool* granted) {
  *granted = false;// 设置投票为false 
  (void)from;
  if (term < n_.term_) return; // 如果任期小于当前任期
  if (!n_.leader_id_.empty() && election_elapsed_ < election_timeout_ticks_) {// 如果领导者ID不为空且选举时间小于选举超时时间
    return;
  }
  bool up_to_date = (last_term > n_.log_.last_term()) || (last_term == n_.log_.last_term() && last_idx >= n_.log_.last_index());// 是否最新
  *granted = up_to_date;
}

void Election::on_request_vote_reply(const std::string& from, uint64_t term, bool granted) {
  step_down_if_stale(term);// 降级为跟随者
  if (n_.role_ != Role::kCandidate || term != n_.term_) return;// 如果角色不是候选者或者任期不等于当前任期

  votes_[from] = granted;// 更新投票

  if (votes_won(votes_)) {// 如果投票获胜
    become_leader();// 成为领导者
    n_.persist_locked();// 持久化
  }
}

void Election::on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted) {
  if (!in_pre_vote_) return;// 如果不在预投票
  if (term > n_.term_) {// 如果任期大于当前任期
    become_follower(term);// 降级为跟随者
    n_.persist_locked();// 持久化
    return;
  }
  pre_votes_[from] = granted;// 更新投票
  if (votes_won(pre_votes_)) {// 如果投票获胜
    SKV_INFO << "shard=" << n_.opts_.shard_id << " pre-vote won; starting real election";// 记录日志
    become_candidate();// 成为候选人
    start_election();// 开始选举
    n_.persist_locked();// 持久化
  }
}

}  // namespace openkv::raft
