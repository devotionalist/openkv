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

// @file membership.hpp
// 成员表 / Joint 配置：ConfChange apply、多数派计算、自动 LeaveJoint（无锁，由 RaftNode::mu_
// 保护）。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/raft/core/types.hpp"

namespace openkv::raft {

class RaftNode;

/// 管「谁有投票权、Joint 新旧两套、加删 peer」。
class Membership {
 public:
  explicit Membership(RaftNode& node) : n_(node) {}

  /// 有投票权的 peer id（非 Joint 时 = 全体非 learner）。
  std::vector<std::string> voter_ids() const;
  uint32_t majority_of(const std::vector<std::string>& voters) const;
  uint32_t majority() const;
  /// 指定投票人集合里是否已有多数派 match >= index。
  bool quorate_at(const std::vector<std::string>& voters, uint64_t index) const;

  bool encode_conf(const ConfChange& cc, std::string* out) const;
  static bool decode_conf(std::string_view data, ConfChange* cc);

  void apply_conf_entry(const LogEntry& e);
  /// Joint 已稳定：自动 propose LeaveJoint。
  void maybe_auto_leave_joint();
  void add_peer(const Peer& p);
  void remove_peer(const std::string& id);

  bool in_joint_{false};                       ///< 是否在 Joint（新旧双多数）
  std::vector<std::string> joint_old_voters_;  ///< Joint 旧投票人
  std::vector<std::string> joint_new_voters_;  ///< Joint 新投票人
  bool auto_leave_pending_{false};             ///< 稳定后自动离 Joint
  /// 已 propose LeaveJoint、等 apply；避免每 tick 重复 append。
  bool leave_joint_inflight_{false};

 private:
  RaftNode& n_;
};

}  // namespace openkv::raft
