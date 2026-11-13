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

// @file msg.hpp
// 职责：raftstore 邮箱消息类型（Tick / RaftMessage / SignificantMsg / ApplyMsg / SnapTask）。
// 架构：PeerMsg 进 Raft worker；ApplyControl 进 Apply worker。
// 协作：SignificantMsg::kWake 是 propose 后 group-commit 唤醒入口（批内合并 Ready）。

#pragma once

#include "openkv/raft/ready.hpp"
#include "openkv/raft/types.hpp"
#include "openkv/region.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace openkv::raftstore {

/// 定时器：Store::schedule_tick 向各 Peer 邮箱投递；触发 election/heartbeat。
struct TickMsg {};

/// 入站或回环的 Raft RPC 载荷（type 如 AppendEntries / RequestVote / …）。
struct RaftMessage {
  std::string from;
  std::string type;
  std::string payload;
};

/// 带外控制：超时转让、探测不可达、以及 propose 后的 kWake（group-commit 入口）。
struct SignificantMsg {
  /// kWake：唤醒 Peer 并驱动一次 Ready（批内多个 propose 合并）。
  enum class Type { kMsgTimeoutNow, kMsgUnreachable, kWake };
  Type type{Type::kWake};
  std::string peer_id;
};

/// Apply 完成后回告 Raft worker（推进 applied / ConfChange 收尾）。
struct ApplyRes {
  uint32_t region_id{0};
  uint64_t applied_index{0};
  bool conf_change{false};
};

/// 投递给 Apply worker 的已提交日志批；destroy=true 表示 Region 销毁清理。
struct ApplyMsg {
  uint32_t region_id{0};
  std::vector<raft::LogEntry> entries;
  bool destroy{false};
};

/// 出站快照任务；data 空则 SnapWorker 调用 Provider 现场生成。
struct SnapTask {
  uint32_t region_id{0};
  raft::SnapshotMeta meta;
  std::string data;
  uint32_t attempt{0};  // 已失败次数；用于重试上限
};

/// Raft worker 邮箱元素。
using PeerMsg = std::variant<TickMsg, RaftMessage, SignificantMsg, ApplyRes>;
/// Apply worker 邮箱元素。
using ApplyControl = std::variant<ApplyMsg, SnapTask>;

}  // namespace openkv::raftstore
