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
// 职责：协议一轮输出（HardState / Message / Ready / SnapshotMeta），不含任何 IO。
// 架构：RaftNode(external_io) 产出 Ready；raftstore::Store 负责 persist + 发网 + apply。
// 协作：persist → RaftEngine；messages → GrpcRaftTransport / TransportFn；committed → Apply。
// 注意：must_sync 提示本批 Ready 需要 durable 后再 advance。

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "openkv/raft/core/types.hpp"

namespace openkv::raft {

// 必须落盘的协议元状态（重启靠它恢复选举/提交水位）。
// 与 SoftState 不同：SoftState 可丢、仅观测；HardState 丢了会乱投票或丢 commit。
struct HardState {
  uint64_t term{0};    ///< 当前任期
  std::string vote;    ///< 本任期投票对象（voted_for）
  uint64_t commit{0};  ///< 已提交最大 index（commit_index）
};

// 待发送的 Raft RPC（由外层 Transport 发出）。
struct Message {
  std::string to;
  std::string type;  // AppendEntries / RequestVote / RequestPreVote / InstallSnapshot
  std::string payload;
};

// 快照元数据；blob 由外层 SnapshotProvider 填充后发送。
struct SnapshotMeta {
  uint64_t index{0};       // 快照索引
  uint64_t term{0};        // 快照任期
  std::string to;          // 目标 peer；空表示本地安装用
  std::string conf_state;  // 发送时的成员表（id@addr[*N],...）。接收端安装快照时恢复，避免
                           // compact窗口吞掉 ConfChange 后 Learner 从新 Leader 成员表消失。
};

// 一轮协议输出：外层（Store）须按序 persist → 发网 → apply → advance。
struct Ready {
  std::optional<HardState> hard_state;      // 若有：本批需落盘的 HardState
  std::vector<LogEntry> entries;            // 需追加到 WAL 的新日志（unstable，可尚未 commit）。
  std::vector<LogEntry> committed_entries;  // 已提交、待 apply 的条目（ConfChange + 普通）。
  std::vector<Message> messages;            // 落盘后再发（耐久顺序）
  std::optional<SnapshotMeta> snapshot;     // 需对该 peer 发起 InstallSnapshot（数据由外层拉取）。
  bool must_sync{false};                    // 本批是否要求 sync 落盘后再 advance
  SoftState soft_state{};                   // 附带观测快照（非持久化载荷）
};

}  // namespace openkv::raft
