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

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "openkv/raft/core/ready.hpp"
#include "openkv/raft/core/types.hpp"
#include "openkv/region.hpp"

namespace openkv::raftstore {

/* 定时器：Store::schedule_tick 向各 Peer 邮箱投递；触发 election/heartbeat。 */
struct TickMsg {};

/* 入站或回环的 Raft RPC 载荷（type 如 AppendEntries / RequestVote / …）。 */
struct RaftMessage {
  std::string from;     // 来源 peer id
  std::string type;     // RPC 类型名（如 AppendEntries）
  std::string payload;  // 已序列化的请求体
};

/* 带外控制：超时转让、探测不可达、propose 后 kWake、异步落盘完成 kPersistDone。 */
struct SignificantMsg {
  enum class Type {
    kMsgTimeoutNow,   // 催对端立刻发起选举（转让）
    kMsgUnreachable,  // 报告某 peer 发不出去，触发探测/降速
    kWake,            // 唤醒 Peer 并驱动一次 Ready（批内多个 propose 合并）
    kPersistDone,     // PersistWorker 落盘完成，raft worker 发 AE / 投递 Apply / advance
  };
  Type type{Type::kWake};  // 本条控制语义
  std::string peer_id;     // 相关对端；kWake/kPersistDone 可空
};

/* Apply 完成后回告 Raft worker（推进 applied / ConfChange 收尾）。 */
struct ApplyRes {
  uint32_t region_id{0};      // 哪个 Region
  uint64_t applied_index{0};  // 状态机已应用到的 index
  bool conf_change{false};    // 本批是否含成员变更，需额外收尾
};

/* 投递给 Apply worker 的已提交日志批；destroy=true 表示 Region 销毁清理。 */
struct ApplyMsg {
  uint32_t region_id{0};                // 哪个 Region
  std::vector<raft::LogEntry> entries;  // 待 apply 的已提交条目
  bool destroy{false};                  // true=销毁清理，entries 可空
};

/* 出站快照任务；data 空则 SnapWorker 调用 Provider 现场生成。 */
struct SnapTask {
  uint32_t region_id{0};    // 哪个 Region
  raft::SnapshotMeta meta;  // 快照元数据（index/term/目标）
  std::string data;         // 预生成字节；空则现场 Provider
  uint32_t attempt{0};      // 已失败次数；用于重试上限
};

// Raft worker 邮箱元素。
using PeerMsg = std::variant<TickMsg, RaftMessage, SignificantMsg, ApplyRes>;
// Apply worker 邮箱元素。
using ApplyControl = std::variant<ApplyMsg, SnapTask>;

}  // namespace openkv::raftstore
