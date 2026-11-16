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

/// 管「谁当家、怎么选、多数派还活着没」。
class Election {
 public:
  explicit Election(RaftNode& node) : n_(node) {}

  /**
   * @brief    
   * 
   * @param term 任期
   * @param leader 领导者
   * @return void
   */
  void become_follower(uint64_t term, const std::string& leader = {});

  /**
   * @brief 成为候选人
   * 
   * @return void
   */   
  void become_candidate();

  /**
   * @brief 成为领导者
   * 
   * @return void
   */
  void become_leader();

  /**
   * @brief 如果任期过期，则降级为跟随者
   * 
   * @param term 任期
   * @return void
   */
  void step_down_if_stale(uint64_t term);

  /**
   * @brief 开始预投票
   * 
   * @return void
   */
  void start_pre_vote();

  /**
   * @brief 开始选举
   * 
   * @return void
   */
  void start_election();

  /**
   * @brief 检查是否满足法定人数
   * 
   * @return void
   */
  void maybe_check_quorum();

  /**
   * @brief 记录节点进度
   * 
   * @param peer_id 节点ID
   * @return void
   */
  void note_peer_progress(const std::string& peer_id);

  /**
   * @brief 选票是否已达多数同意
   * 
   * @param votes 选票
   * @return bool
   */
  /// 选票 map 是否已达多数同意（含自己；Joint 要双多数）。
  bool votes_won(const std::unordered_map<std::string, bool>& votes) const;


  /**
   * @brief 处理投票请求
   *  
   * @param from 请求来源
   * @param term 任期
   * @param last_idx 最后索引
   * @param last_term 最后任期
   * @param granted 是否授予
   * @return void
   */ 
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);

  /**
   * @brief 处理预投票请求
   * 
   * @param from 请求来源
   * @param term 任期
   * @param last_idx 最后索引
   * @param last_term 最后任期
   * @param granted 是否授予
   * @return void
   */
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);

  /**
   * @brief 处理投票回复
   * 
   * @param from 请求来源
   * @param term 任期
   * @param granted 是否授予
   * @return void
   */
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);

  /**
   * @brief 处理预投票回复
   * 
   * @param from 请求来源
   * @param term 任期
   * @param granted 是否授予
   * @return void
   */
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);

  /**
   * @brief 处理选举回复
   * 
   * @param from 请求来源
   * @param term 任期
   * @param granted 是否授予
   * @return void
   */
  void on_request_election_reply(const std::string& from, uint64_t term, bool granted);


  std::unordered_map<std::string, bool> votes_;      ///< 正式选举收票
  std::unordered_map<std::string, bool> pre_votes_;  ///< PreVote 收票
  bool in_pre_vote_{false};                          ///< 正在 PreVote 轮
  /// Leader：距上次收到该 voter 成功响应过了多少 tick。
  std::unordered_map<std::string, int> peer_quiet_ticks_;
  int election_elapsed_{0};         ///< Follower 距上次听到 Leader 的 tick 数
  int heartbeat_elapsed_{0};        ///< Leader 距上次该发心跳的 tick 数
  int election_timeout_ticks_{10};  ///< 折算成「多少次 tick 算选举超时」
  /// 当选后前几拍跳过 CheckQuorum，等首轮心跳 ACK 回来。
  int leader_quorum_grace_ticks_{0};
  uint64_t election_count_{0};

 private:
  RaftNode& n_;
};

}  // namespace openkv::raft
