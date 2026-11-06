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

// @file ready.hpp
// raft-rs 同构的 Ready / Message / HardState（协议输出，不含 IO）。

#pragma once

#include "opskv/raft/types.hpp"

#include <optional>
#include <string>
#include <vector>

namespace opskv::raft {

/// 持久化 hard state（对齐 raft-rs HardState）。
struct HardState {
  uint64_t term{0};
  std::string vote;
  uint64_t commit{0};
};

/// 待发送的 Raft RPC（由外层 Transport 发出）。
struct Message {
  std::string to;
  std::string type;  // AppendEntries / RequestVote / RequestPreVote / InstallSnapshot
  std::string payload;
};

/// 快照元数据；blob 由外层 SnapshotProvider 填充后发送。
struct SnapshotMeta {
  uint64_t index{0};
  uint64_t term{0};
  std::string to;  // 目标 peer；空表示本地安装用
  /// 发送时的成员表（id@addr[*N],...）。接收端安装快照时恢复，避免 compact
  /// 窗口吞掉 ConfChange 后 Learner 从新 Leader 成员表消失。
  std::string conf_state;
};

/// raft-rs Ready：一轮协议推进后外层须处理的批次。
struct Ready {
  std::optional<HardState> hard_state;
  /// 需追加到 WAL 的新日志（含未提交）。
  std::vector<LogEntry> entries;
  /// 已提交、待 apply 的条目（ConfChange + 普通）。
  std::vector<LogEntry> committed_entries;
  std::vector<Message> messages;
  /// 需对该 peer 发起 InstallSnapshot（数据由外层拉取）。
  std::optional<SnapshotMeta> snapshot;
  bool must_sync{false};
  SoftState soft_state{};
};

}  // namespace opskv::raft
