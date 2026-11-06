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

// @file types.hpp
// Raft 子系统公共类型：角色、日志条目、软状态、传输与成员变更回调。
//
// 架构位置：raft_log / raft_node / multi_raft 及 storage 层 ConfChange 编解码共用。

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace opskv::raft {

/// 节点在 Raft 组内的角色；Learner 不参与投票与 quorum。
enum class Role { kFollower, kCandidate, kLeader, kLearner };

/// 集群内一个 Raft 对等体（含 learner 标记）。
struct Peer {
  std::string id;
  std::string address;
  bool learner{false};
};

/// 已持久化/复制的一条 Raft 日志记录；data 由上层状态机解释（如 KV Command）。
struct LogEntry {
  uint64_t term{0};
  uint64_t index{0};
  std::string data;
};

/// 可观测的 Raft 软状态快照（不含持久化 hard state 细节）；读时需持锁或接受短暂不一致。
struct SoftState {
  Role role{Role::kFollower};
  std::string leader_id;
  uint64_t term{0};
  uint64_t commit_index{0};
  uint64_t last_applied{0};
  /// O1：选主次数 / 发送快照次数 / 复制延迟（leader last_index - min peer match）。
  uint64_t election_count{0};
  uint64_t snapshot_send_count{0};
  uint64_t replication_lag{0};
  bool in_joint{false};
  /// in_joint 或 LeaveJoint 尚未 apply（auto_leave_pending）。
  bool conf_change_busy{false};
};

// 状态机回调：多数派提交后由 Raft Apply
/// 日志条目已 commit 且轮到 apply 时调用；须在 RaftNode 锁内或按实现约定串行 apply。
using ApplyCallback = std::function<void(uint64_t index, const std::string& data)>;
// 发送 AppendEntries / RequestVote（由上层 gRPC 实现）
/// 向指定 peer 发送 RPC；msg_type 如 "AppendEntries" / "RequestVote"，payload 由上层序列化。
using TransportSend = std::function<void(const std::string& peer_id, const std::string& msg_type,
                                         const std::string& payload)>;

/// 在线成员变更提案内容；经日志提交后在 apply 阶段生效。
struct ConfChange {
  enum Type {
    kAddNode = 0,
    kRemoveNode = 1,
    kAddLearner = 2,
    kPromoteLearner = 3,
    /// Joint：进入 (C_old, C_new)；payload 内嵌原变更类型于 peer 之外由编码层处理。
    kEnterJoint = 4,
    /// Joint：离开，配置变为纯 C_new。
    kLeaveJoint = 5,
  };
  Type type{kAddNode};
  Peer peer;
  /// EnterJoint 时携带的实际变更（Promote / Remove）；LeaveJoint 可忽略。
  Type joint_change{kAddNode};
};

}  // namespace opskv::raft
