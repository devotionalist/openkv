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
// 职责：Raft 子系统公共类型（Role / Peer / LogEntry / SoftState / ConfChange 与回调别名）。
// 架构：被 raft_log、raft_node、ready、raftstore::msg、Storage ConfChange 编解码共用。
// 协作：无独立 .cpp；纯类型头。
// 注意：LogEntry::data 语义由上层状态机解释（KV Command / ConfChange 等）。

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace openkv::raft {

// 节点在 Raft 组内的角色；Learner 不参与投票与 quorum。
enum class Role {
  kFollower,   // 跟班：收日志、可投票
  kCandidate,  // 竞选中
  kLeader,     // 当家：收写、复制日志
  kLearner,    // 只收日志、不投票、不算多数派
};

/* 集群内一个 Raft 对等体（含 learner 标记）。 */
struct Peer {
  std::string id;       // 节点ID
  std::string address;  // 节点地址
  bool learner{false};  // 是否学习者
};

/* 已持久化/复制的一条 Raft 日志记录；data 由上层状态机解释（如 KV Command）。 */
struct LogEntry {
  uint64_t term{0};   // 日志条目的任期号
  uint64_t index{0};  // 日志条目的索引
  std::string data;   // 日志条目的数据
};

/*
 * 可观测软状态快照（给 metrics / redirect / 调试）。
 * 可含 term、commit_index 的**只读拷贝**，但持久化以 HardState 为准；重启不依赖 SoftState。
 */
struct SoftState {
  Role role{Role::kFollower};       // Follower / Candidate / Leader / Learner
  std::string leader_id;            // 已知 Leader（redirect）
  uint64_t term{0};                 // 当前任期（观测用，落盘见 HardState.term）
  uint64_t commit_index{0};         // 已提交水位（观测用，落盘见 HardState.commit）
  uint64_t last_applied{0};         // 状态机已 apply 水位（一般不进 HardState）
  uint64_t election_count{0};       // 选主次数
  uint64_t snapshot_send_count{0};  // 发快照次数
  uint64_t replication_lag{0};      // Leader：last_index - min(peer match)
  bool in_joint{false};             // 是否联合配置（新旧双多数过渡期；不是选举角色）
  bool conf_change_busy{false};     // Joint 中或 LeaveJoint 未完成，勿再 Promote/Remove
};

// 状态机回调：多数派提交后由 Raft Apply
// 日志条目已 commit 且轮到 apply 时调用；须在 RaftNode 锁内或按实现约定串行 apply。
using ApplyCallback = std::function<void(uint64_t index, const std::string& data)>;
// 发送 AppendEntries / RequestVote（由上层 gRPC 实现）
// 向指定 peer 发送 RPC；msg_type 如 "AppendEntries" / "RequestVote"，payload 由上层序列化。
using TransportSend = std::function<void(const std::string& peer_id, const std::string& msg_type,
                                         const std::string& payload)>;

/*
 * 成员变更提案：走 Raft 日志，commit 后再 apply 才改配置。
 * 注意：这些不是 Leader/Follower「角色」，而是「对成员名单做什么」。
 */
struct ConfChange {
  enum Type {
    kAddNode = 0,         // 直接加一个有投票权的节点（voter）
    kRemoveNode = 1,      // 从配置里去掉一个节点（常走 Joint，见 propose_conf_change）
    kAddLearner = 2,      // 加学习者：只收日志、不投票、不算多数派
    kPromoteLearner = 3,  // 学习者转正成 voter（必走 Joint，防一步切配置双主）
    // 进入联合配置：新旧两套投票人同时说了算（双多数派才能 commit）
    kEnterJoint = 4,
    // 退出联合：只留新配置 C_new，恢复「一套人说了算」
    kLeaveJoint = 5,
  };
  Type type{kAddNode};  // 本条提案的变更种类
  Peer peer;            // 针对哪个节点（id/地址/是否 learner）
  // 进 Joint 时真正要做的事（通常是 Promote 或 Remove）；LeaveJoint 不用管
  Type joint_change{kAddNode};
};

}  // namespace openkv::raft
