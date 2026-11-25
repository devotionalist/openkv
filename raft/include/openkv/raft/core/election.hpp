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

// @file election.hpp
// 选举与角色切换：PreVote / Vote / CheckQuorum（无锁，由 RaftNode::mu_ 保护）。

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace openkv::raft {

class RaftNode;

/* 管「谁当家、怎么选、多数派还活着没」。 */
class Election {
 public:
  /**
   * 绑定所属 RaftNode（共享 mu_）。
   * @param node 宿主节点
   */
  explicit Election(RaftNode& node) : n_(node) {}

  /**
   * 切到 Follower；可记录已知 Leader。
   * @param term 新任期（通常 ≥ 当前）
   * @param leader 已知 Leader id；默认空
   */
  void become_follower(uint64_t term, const std::string& leader = {});
  /**
   * 切到 Candidate 并清票箱，准备拉票。
   */
  void become_candidate();
  /**
   * 当选后切到 Leader，初始化复制进度。
   */
  void become_leader();
  /**
   * 若对端 term 更大则退位成 Follower。
   * @param term 对端任期
   */
  void step_down_if_stale(uint64_t term);
  /**
   * 发起 PreVote 轮（不抬本地 term）。
   */
  void start_pre_vote();
  /**
   * 发起正式选举（抬 term、自投、广播 RequestVote）。
   */
  void start_election();
  /**
   * Leader：检查多数派近期是否还有应答，否则退位。
   */
  void maybe_check_quorum();
  /**
   * 记下某 voter 刚有成功进度（清 quiet ticks）。
   * @param peer_id 对端
   */
  void note_peer_progress(const std::string& peer_id);
  /**
   * 选票 map 是否已达多数同意（含自己；Joint 要双多数）。
   * @param votes peer → 是否同意
   * @return true 表示已赢
   */
  bool votes_won(const std::unordered_map<std::string, bool>& votes) const;
  /**
   * 处理入站 RequestVote。
   * @param from 候选者
   * @param term 候选任期
   * @param last_idx 候选日志末下标
   * @param last_term 候选日志末 term
   * @param granted 输出是否投票
   */
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);
  /**
   * 处理入站 PreVote。
   * @param from 候选者
   * @param term 拟用任期
   * @param last_idx 候选日志末下标
   * @param last_term 候选日志末 term
   * @param granted 输出是否同意预投
   */
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);
  /**
   * 处理入站 RequestVote 应答。
   * @param from 对端
   * @param term 对端任期
   * @param granted 是否同意
   */
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  /**
   * 处理入站 PreVote 应答。
   * @param from 对端
   * @param term 对端任期
   * @param granted 是否同意预投
   */
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  /**
   * 统一处理选举类应答（Vote/PreVote 共用收尾）。
   * @param from 对端
   * @param term 对端任期
   * @param granted 是否同意
   */
  void on_request_election_reply(const std::string& from, uint64_t term, bool granted);

  std::unordered_map<std::string, bool> votes_;      // 正式选举收票
  std::unordered_map<std::string, bool> pre_votes_;  // PreVote 收票
  bool in_pre_vote_{false};                          // 正在 PreVote 轮
  // Leader：距上次收到该 voter 成功响应过了多少 tick。
  std::unordered_map<std::string, int> peer_quiet_ticks_;
  int election_elapsed_{0};         // Follower 距上次听到 Leader 的 tick 数
  int heartbeat_elapsed_{0};        // Leader 距上次该发心跳的 tick 数
  int election_timeout_ticks_{10};  // 折算成「多少次 tick 算选举超时」
  // 当选后前几拍跳过 CheckQuorum，等首轮心跳 ACK 回来。
  int leader_quorum_grace_ticks_{0};
  uint64_t election_count_{0};  // 累计发起选举次数

 private:
  RaftNode& n_;  // 宿主节点
};

}  // namespace openkv::raft
