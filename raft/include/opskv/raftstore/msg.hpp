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
// Raftstore 邮箱消息（SignificantMsg / PeerMsg / ApplyMsg）。

#pragma once

#include "opskv/raft/ready.hpp"
#include "opskv/raft/types.hpp"
#include "opskv/region.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace opskv::raftstore {

struct TickMsg {};
struct RaftMessage {
  std::string from;
  std::string type;
  std::string payload;
};
struct SignificantMsg {
  enum class Type { kMsgTimeoutNow, kMsgUnreachable, kWake };
  Type type{Type::kWake};
  std::string peer_id;
};
struct ApplyRes {
  uint32_t region_id{0};
  uint64_t applied_index{0};
  bool conf_change{false};
};
struct ApplyMsg {
  uint32_t region_id{0};
  std::vector<raft::LogEntry> entries;
  bool destroy{false};
};
struct SnapTask {
  uint32_t region_id{0};
  raft::SnapshotMeta meta;
  std::string data;  // 空则由 SnapWorker 拉取
  uint32_t attempt{0};  // 已失败次数；用于 SnapWorker 重试上限
};

using PeerMsg = std::variant<TickMsg, RaftMessage, SignificantMsg, ApplyRes>;
using ApplyControl = std::variant<ApplyMsg, SnapTask>;

}  // namespace opskv::raftstore
