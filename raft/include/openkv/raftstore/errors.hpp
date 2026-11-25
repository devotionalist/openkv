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
// raftstore 入站 / propose 错误 token（Store↔Store 或 async_write 出口）。
// 共用 token 见 openkv/errors.hpp。

#pragma once

#include <string_view>

#include "openkv/errors.hpp"

namespace openkv::raftstore::err {

using openkv::err::kNoRegion;
using openkv::err::kNotLeader;
using openkv::err::kProposeFailed;

// 入站 Raft 行解析 / step
inline constexpr std::string_view kBadVote = "bad_vote";
inline constexpr std::string_view kBadAe = "bad_ae";
inline constexpr std::string_view kBadSnap = "bad_snap";
inline constexpr std::string_view kAeTrunc = "ae_trunc";
inline constexpr std::string_view kAeBadHex = "ae_bad_hex";
inline constexpr std::string_view kSnapBadHex = "snap_bad_hex";
inline constexpr std::string_view kStep = "step";
inline constexpr std::string_view kUnknownRaftType = "unknown_raft_type";

}  // namespace openkv::raftstore::err
