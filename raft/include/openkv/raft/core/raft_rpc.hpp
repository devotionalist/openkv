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

// @file raft_rpc.hpp
// 职责：节点间 Raft RPC 的内存结构体（Vote / AE / Snap）与入站回包。
// 架构：Ready::Message / RaftMessage 直接扛结构体；GrpcRaftTransport 填 proto，不再 pipe|hex。
// 协作：replication / election enqueue；Store::step；Storage/Meta gRPC 入站。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "openkv/raft/core/types.hpp"

namespace openkv::raft {

/* 出站 / 入站 RPC 种类（名字给故障注入、日志用）。 */
enum class MsgType : uint8_t {
  kAppendEntries = 0,
  kRequestVote = 1,
  kRequestPreVote = 2,
  kInstallSnapshot = 3,
};

/**
 * RPC 类型名（与历史字符串一致，便于故障注入按名过滤）。
 * @param t 消息种类
 * @return 静态 C 串
 */
inline const char* msg_type_name(MsgType t) noexcept {
  switch (t) {
    case MsgType::kAppendEntries:
      return "AppendEntries";
    case MsgType::kRequestVote:
      return "RequestVote";
    case MsgType::kRequestPreVote:
      return "RequestPreVote";
    case MsgType::kInstallSnapshot:
      return "InstallSnapshot";
  }
  return "Unknown";
}

/**
 * 从类型名解析 MsgType。
 * @param s 类型名
 * @param out 写出结果
 * @return 是否识别
 */
inline bool parse_msg_type(std::string_view s, MsgType* out) noexcept {
  if (!out) {
    return false;
  }
  if (s == "AppendEntries") {
    *out = MsgType::kAppendEntries;
    return true;
  }
  if (s == "RequestVote") {
    *out = MsgType::kRequestVote;
    return true;
  }
  if (s == "RequestPreVote") {
    *out = MsgType::kRequestPreVote;
    return true;
  }
  if (s == "InstallSnapshot") {
    *out = MsgType::kInstallSnapshot;
    return true;
  }
  return false;
}

/* RequestVote / RequestPreVote 请求体。 */
struct VoteReq {
  uint64_t term{0};            // 候选人任期（PreVote 为 future_term）
  uint64_t last_log_index{0};  // 候选人最后一条日志 index
  uint64_t last_log_term{0};   // 对应 term
};

/* AppendEntries 请求体（entries 空 = 心跳 / probe）。 */
struct AppendEntriesReq {
  uint64_t term{0};                          // Leader 任期
  uint64_t prev_log_index{0};                // 对齐点 index
  uint64_t prev_log_term{0};                 // 对齐点 term
  uint64_t leader_commit{0};                 // Leader 已提交水位
  std::vector<LogEntry> entries;             // 本批日志；二进制 data，不经 hex
};

/* InstallSnapshot 请求体；data 空表示 external_io 壳，等 SnapWorker 填 blob。 */
struct InstallSnapshotReq {
  uint64_t term{0};        // Leader 任期
  uint64_t last_index{0};  // 快照覆盖到的 index
  uint64_t last_term{0};   // 对应 term
  std::string data;        // 快照二进制（可含 ConfState 前缀）
};

/* 出站 / 入站统一载荷；monostate = 仅唤醒、无业务体。 */
using RaftReq = std::variant<std::monostate, VoteReq, AppendEntriesReq, InstallSnapshotReq>;

/*
 * 入站 step 回包：直接填 gRPC Response，不再拼「OK AE …」行。
 * Meta TCP 调试口若还要文本，可另做 to_line，生产路径不走。
 */
struct RaftReply {
  enum class Kind : uint8_t { kError, kVote, kPreVote, kAppend, kSnap };

  Kind kind{Kind::kError};  // 回包种类
  std::string err;          // kError：err::line token
  uint64_t term{0};         // 本机当前 term
  bool ok{false};           // 投票通过 / AE 成功 / 快照接受
  uint64_t match_index{0};  // AE / Snap 对齐点

  /**
   * 构造错误回包。
   * @param e err::line 文本
   * @return 回包
   */
  static RaftReply error(std::string e) {
    RaftReply r;
    r.kind = Kind::kError;
    r.err = std::move(e);
    return r;
  }
};

}  // namespace openkv::raft
