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

// @file errors.hpp
// 跨模块共用的错误 token（行协议 `ERR <token>` 里的 token）与拼装助手。
// 模块私有 token 见 raftstore/storage/meta 各自的 errors.hpp。
// 目录说明：docs/architecture/ERROR_CODES.md

#pragma once

#include <string>
#include <string_view>

#include "openkv/protocol.hpp"

namespace openkv::err {

// ---------------------------------------------------------------------------
// 共用 token（裸串；拼 ERR 行用 line() / not_leader_redirect()）
// ---------------------------------------------------------------------------
inline constexpr std::string_view kNoRegion = "no_region";
inline constexpr std::string_view kNotLeader = "not_leader";
inline constexpr std::string_view kProposeFailed = "propose_failed";
inline constexpr std::string_view kApplyTimeout = "apply_timeout";
inline constexpr std::string_view kEpochNotMatch = "epoch_not_match";
inline constexpr std::string_view kCasFailed = "cas_failed";
inline constexpr std::string_view kLocked = "locked";
inline constexpr std::string_view kSplitting = "splitting";
inline constexpr std::string_view kUnauthorized = "unauthorized";
inline constexpr std::string_view kEmpty = "empty";
inline constexpr std::string_view kBadHex = "bad_hex";
inline constexpr std::string_view kBadRequest = "bad_request";
inline constexpr std::string_view kReadIndexFailed = "read_index_failed";
inline constexpr std::string_view kRaftNotReady = "raft_not_ready";
inline constexpr std::string_view kWriteConflict = "write_conflict";
inline constexpr std::string_view kBusy = "busy";
inline constexpr std::string_view kDestroyed = "destroyed";
inline constexpr std::string_view kKeyNotInRegion = "key_not_in_region";
inline constexpr std::string_view kLockNotFound = "lock_not_found";
inline constexpr std::string_view kNoEngine = "no_engine";
inline constexpr std::string_view kRedirect = "redirect=";

/**
 * 拼 `ERR <token>`。
 * @param token 错误 token
 * @return 行协议错误正文
 */
inline std::string line(std::string_view token) {
  std::string s;
  s.reserve(proto::kErr.size() + 1 + token.size());
  s.append(proto::kErr);
  s.push_back(' ');
  s.append(token);
  return s;
}

/**
 * 拼 `ERR <token> <suffix>`（suffix 自带前导空格或 k=v）。
 * @param token 错误 token
 * @param suffix 附加段，如 `class=joint` 或 `age_ms=3`
 */
inline std::string line(std::string_view token, std::string_view suffix) {
  std::string s = line(token);
  if (!suffix.empty()) {
    if (suffix.front() != ' ') {
      s.push_back(' ');
    }
    s.append(suffix);
  }
  return s;
}

/**
 * `ERR not_leader`；addr 非空时追加 ` redirect=<addr>`。
 * @param addr Leader 提示地址；可空
 */
inline std::string not_leader_redirect(std::string_view addr = {}) {
  if (addr.empty()) {
    return line(kNotLeader);
  }
  std::string s = line(kNotLeader);
  s.push_back(' ');
  s.append(kRedirect);
  s.append(addr);
  return s;
}

/**
 * 正文里是否出现某 token（跟 redirect / 重试探测用）。
 * @param body 响应或错误串
 * @param token 要找的 token
 */
inline bool has_token(std::string_view body, std::string_view token) {
  return body.find(token) != std::string_view::npos;
}

}  // namespace openkv::err
