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
// Meta 管控面私有错误 token。
// 共用 token 见 openkv/errors.hpp。

#pragma once

#include <string_view>

#include "openkv/errors.hpp"

namespace openkv::meta::err {

using openkv::err::kApplyTimeout;
using openkv::err::kBadHex;
using openkv::err::kBadRequest;
using openkv::err::kEmpty;
using openkv::err::kNotLeader;
using openkv::err::kProposeFailed;
using openkv::err::kRaftNotReady;
using openkv::err::kUnauthorized;

inline constexpr std::string_view kUnknownNode = "unknown_node";
inline constexpr std::string_view kNoScaler = "no_scaler";
inline constexpr std::string_view kNoScheduler = "no_scheduler";
inline constexpr std::string_view kNoTask = "no_task";
inline constexpr std::string_view kNoAddress = "no_address";
inline constexpr std::string_view kNotPending = "not_pending";
inline constexpr std::string_view kNoHeartbeat = "no_heartbeat";
inline constexpr std::string_view kHeartbeatStale = "heartbeat_stale";
inline constexpr std::string_view kUnreachable = "unreachable";
inline constexpr std::string_view kStillActive = "still_active";
inline constexpr std::string_view kStillHasRegions = "still_has_regions";
inline constexpr std::string_view kEmptyPeers = "empty_peers";
inline constexpr std::string_view kSplitApplyMissing = "split_apply_missing";
inline constexpr std::string_view kMergeLostLeft = "merge_lost_left";
inline constexpr std::string_view kMergeRightStillPresent = "merge_right_still_present";
inline constexpr std::string_view kNoAdjacent = "no_adjacent";
inline constexpr std::string_view kNotFound = "not_found";
inline constexpr std::string_view kBadRole = "bad_role";
inline constexpr std::string_view kBadHeartbeat = "bad_heartbeat";
inline constexpr std::string_view kCancel = "cancel";
inline constexpr std::string_view kAuthUsage = "usage: AUTH <token>";
inline constexpr std::string_view kAeTrunc = "ae_trunc";
inline constexpr std::string_view kAeBadHex = "ae_bad_hex";
inline constexpr std::string_view kSnapBadHex = "snap_bad_hex";
inline constexpr std::string_view kSnapc = "snapc";

// Scaler / scheduler 任务 error= 字段
inline constexpr std::string_view kNodeNotActive = "node_not_active";
inline constexpr std::string_view kNodeGone = "node_gone";
inline constexpr std::string_view kCancelled = "cancelled";
inline constexpr std::string_view kAddLearnerFailed = "add_learner_failed";
inline constexpr std::string_view kPromoteFailed = "promote_failed";
inline constexpr std::string_view kBumpRouteFailed = "bump_route_failed";
inline constexpr std::string_view kDuplicateInflight = "duplicate_inflight";
inline constexpr std::string_view kNoPlacement = "no_placement";
inline constexpr std::string_view kAlreadyMigrating = "already_migrating";
inline constexpr std::string_view kPlaceExhausted = "place_exhausted";
inline constexpr std::string_view kCommitPlaceFailed = "commit_place_failed";

}  // namespace openkv::meta::err
