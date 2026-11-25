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
// Storage 数据面 / Admin / Place 私有错误 token。
// 共用 token 见 openkv/errors.hpp。

#pragma once

#include <string_view>

#include "openkv/errors.hpp"

namespace openkv::storage::err {

using openkv::err::kApplyTimeout;
using openkv::err::kBusy;
using openkv::err::kCasFailed;
using openkv::err::kDestroyed;
using openkv::err::kEpochNotMatch;
using openkv::err::kKeyNotInRegion;
using openkv::err::kLockNotFound;
using openkv::err::kLocked;
using openkv::err::kNoEngine;
using openkv::err::kNoRegion;
using openkv::err::kNotLeader;
using openkv::err::kProposeFailed;
using openkv::err::kReadIndexFailed;
using openkv::err::kSplitting;
using openkv::err::kUnauthorized;
using openkv::err::kWriteConflict;

// Admin / ConfChange / 转主
inline constexpr std::string_view kBusyClassJoint = "busy class=joint";
inline constexpr std::string_view kAddLearnerFailed = "add_learner_failed";
inline constexpr std::string_view kPromoteFailed = "promote_failed";
inline constexpr std::string_view kRemoveFailed = "remove_failed";
inline constexpr std::string_view kUnknownPeer = "unknown_peer";
inline constexpr std::string_view kUnknownTarget = "unknown_target";
inline constexpr std::string_view kTargetIsLearner = "target_is_learner";
inline constexpr std::string_view kLostLeadership = "lost_leadership";
inline constexpr std::string_view kTargetNotCaughtUp = "target_not_caught_up";
inline constexpr std::string_view kAbdicateFailed = "abdicate_failed";
inline constexpr std::string_view kCampaignFailed = "campaign_failed";
inline constexpr std::string_view kPlaceFailed = "place_failed";
inline constexpr std::string_view kCheckpointFailed = "checkpoint_failed";
inline constexpr std::string_view kLocalEnsure = "local_ensure";
inline constexpr std::string_view kRemoteEnsure = "remote_ensure";
inline constexpr std::string_view kSnapc = "snapc";

// Apply / 命令解码
inline constexpr std::string_view kBadCommand = "bad_command";
inline constexpr std::string_view kBadPrewrite = "bad_prewrite";
inline constexpr std::string_view kBadCommit = "bad_commit";
inline constexpr std::string_view kBadRollback = "bad_rollback";
inline constexpr std::string_view kBadOnePc = "bad_onepc";
inline constexpr std::string_view kBadTs = "bad_ts";
inline constexpr std::string_view kBadAdminSplit = "bad_admin_split";
inline constexpr std::string_view kBadAdminMerge = "bad_admin_merge";
inline constexpr std::string_view kBadAdminSetEpoch = "bad_admin_set_epoch";
inline constexpr std::string_view kBadPessimisticLock = "bad_pessimistic_lock";
inline constexpr std::string_view kBadPessimisticRollback = "bad_pessimistic_rollback";
inline constexpr std::string_view kUnknownOp = "unknown_op";
inline constexpr std::string_view kPessimisticNotPrewritten = "pessimistic_not_prewritten";
inline constexpr std::string_view kEngineWriteFailed = "engine_write_failed";

// CHECK_TXN_STATUS 成功态（非 ERR；locked 复用上面的 kLocked）
inline constexpr std::string_view kTxnExpired = "expired";
inline constexpr std::string_view kTxnCommitted = "committed";
inline constexpr std::string_view kTxnRolledback = "rolledback";

}  // namespace openkv::storage::err
