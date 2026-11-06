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

// @file protocol.hpp
// 行协议操作名、响应前缀、环境变量与超时常量（C++ Storage/Meta TCP）。

#pragma once

#include <string_view>

namespace opskv::proto {

// ---------------------------------------------------------------------------
// 响应前缀
// ---------------------------------------------------------------------------
inline constexpr std::string_view kOk = "OK";
inline constexpr std::string_view kErr = "ERR";

inline bool starts_with_ok(std::string_view s) { return s.rfind(kOk, 0) == 0; }
inline bool starts_with_err(std::string_view s) { return s.rfind(kErr, 0) == 0; }

// ---------------------------------------------------------------------------
// 前缀（分类 / ACL）
// ---------------------------------------------------------------------------
inline constexpr std::string_view kAdminPrefix = "ADMIN_";
inline constexpr std::string_view kRaftPrefix = "RAFT_";
inline constexpr std::string_view kRaftSnapPrefix = "RAFT_SNAP";

// ---------------------------------------------------------------------------
// Storage 通用
// ---------------------------------------------------------------------------
inline constexpr std::string_view kOpAuth = "AUTH";
inline constexpr std::string_view kOpPing = "PING";
inline constexpr std::string_view kOpStats = "STATS";
inline constexpr std::string_view kOpMetrics = "METRICS";

inline constexpr std::string_view kOpPut = "PUT";
inline constexpr std::string_view kOpGet = "GET";
inline constexpr std::string_view kOpDel = "DEL";

inline constexpr std::string_view kOpPrewrite = "PREWRITE";
inline constexpr std::string_view kOpCommit = "COMMIT";
inline constexpr std::string_view kOpRollback = "ROLLBACK";
inline constexpr std::string_view kOpPessimisticLock = "PESSIMISTIC_LOCK";
inline constexpr std::string_view kOpPessimisticRollback = "PESSIMISTIC_ROLLBACK";
inline constexpr std::string_view kOpResolveLock = "RESOLVE_LOCK";
inline constexpr std::string_view kOpGetLock = "GET_LOCK";
inline constexpr std::string_view kOpCheckTxnStatus = "CHECK_TXN_STATUS";
inline constexpr std::string_view kOpTxnGet = "TXN_GET";

inline constexpr std::string_view kOpWatchHead = "WATCH_HEAD";
inline constexpr std::string_view kOpWatchPoll = "WATCH_POLL";

// ---------------------------------------------------------------------------
// Raft 行协议
// ---------------------------------------------------------------------------
inline constexpr std::string_view kOpRaftVote = "RAFT_VOTE";
inline constexpr std::string_view kOpRaftPreVote = "RAFT_PREVOTE";
inline constexpr std::string_view kOpRaftAe = "RAFT_AE";
inline constexpr std::string_view kOpRaftSnap = "RAFT_SNAP";
inline constexpr std::string_view kOpRaftSnapc = "RAFT_SNAPC";
inline constexpr std::string_view kOpRaftSnapshot = "RAFT_SNAPSHOT";

// ---------------------------------------------------------------------------
// Storage ADMIN_*
// ---------------------------------------------------------------------------
inline constexpr std::string_view kOpAdminPrepareSplit = "ADMIN_PREPARE_SPLIT";
inline constexpr std::string_view kOpAdminSplit = "ADMIN_SPLIT";
inline constexpr std::string_view kOpAdminMerge = "ADMIN_MERGE";
inline constexpr std::string_view kOpAdminEnsureRegion = "ADMIN_ENSURE_REGION";
inline constexpr std::string_view kOpAdminAddLearner = "ADMIN_ADD_LEARNER";
inline constexpr std::string_view kOpAdminPromote = "ADMIN_PROMOTE";
inline constexpr std::string_view kOpAdminRemovePeer = "ADMIN_REMOVE_PEER";
inline constexpr std::string_view kOpAdminDestroyRegion = "ADMIN_DESTROY_REGION";
inline constexpr std::string_view kOpAdminPeerProgress = "ADMIN_PEER_PROGRESS";
inline constexpr std::string_view kOpAdminTransferLeader = "ADMIN_TRANSFER_LEADER";
inline constexpr std::string_view kOpAdminCampaign = "ADMIN_CAMPAIGN";
inline constexpr std::string_view kOpAdminPlaceRegion = "ADMIN_PLACE_REGION";
inline constexpr std::string_view kOpAdminSetEpoch = "ADMIN_SET_EPOCH";
inline constexpr std::string_view kOpAdminCheckpoint = "ADMIN_CHECKPOINT";
inline constexpr std::string_view kOpAdminRegionStats = "ADMIN_REGION_STATS";
inline constexpr std::string_view kOpAdminCop = "ADMIN_COP";

// storage_rpc 长超时匹配前缀（ENSURE / TRANSFER 覆盖 ENSURE_REGION / TRANSFER_LEADER）
inline constexpr std::string_view kAdminEnsurePrefix = "ADMIN_ENSURE";
inline constexpr std::string_view kAdminTransferPrefix = "ADMIN_TRANSFER";

// ---------------------------------------------------------------------------
// Meta 写路径（需 ACL；与 meta_needs_admin 同源）
// ---------------------------------------------------------------------------
inline constexpr std::string_view kOpDrainNode = "DRAIN_NODE";
inline constexpr std::string_view kOpActivateNode = "ACTIVATE_NODE";
inline constexpr std::string_view kOpOfflineNode = "OFFLINE_NODE";
inline constexpr std::string_view kOpScaleOut = "SCALE_OUT";
inline constexpr std::string_view kOpStoreDelete = "STORE_DELETE";
inline constexpr std::string_view kOpJoinNode = "JOIN_NODE";
inline constexpr std::string_view kOpRemoveTombstone = "REMOVE_TOMBSTONE";
inline constexpr std::string_view kOpKickNode = "KICK_NODE";
inline constexpr std::string_view kOpSplitRegion = "SPLIT_REGION";
inline constexpr std::string_view kOpMergeRegions = "MERGE_REGIONS";
inline constexpr std::string_view kOpPlaceRegion = "PLACE_REGION";
inline constexpr std::string_view kOpScheduleMigrate = "SCHEDULE_MIGRATE";
inline constexpr std::string_view kOpCancelMigrate = "CANCEL_MIGRATE";
inline constexpr std::string_view kOpAdvanceSafePoint = "ADVANCE_SAFE_POINT";
inline constexpr std::string_view kOpBumpRoute = "BUMP_ROUTE";

/// Meta 需鉴权的写操作列表（供 meta_needs_admin）。
inline constexpr std::string_view kMetaAdminOps[] = {
    kOpDrainNode,         kOpActivateNode,   kOpOfflineNode,     kOpScaleOut,
    kOpStoreDelete,       kOpJoinNode,       kOpRemoveTombstone, kOpKickNode,
    kOpSplitRegion,       kOpMergeRegions,   kOpPlaceRegion,     kOpScheduleMigrate,
    kOpCancelMigrate,     kOpAdvanceSafePoint, kOpBumpRoute,
};

// ---------------------------------------------------------------------------
// 环境变量
// ---------------------------------------------------------------------------
namespace env {
inline constexpr const char* kAdminToken = "SKV_ADMIN_TOKEN";
inline constexpr const char* kPdAutoBalance = "SKV_PD_AUTO_BALANCE";
inline constexpr const char* kPersistSync = "SKV_PERSIST_SYNC";
inline constexpr const char* kSplitKeys = "SKV_SPLIT_KEYS";
inline constexpr const char* kSplitBytes = "SKV_SPLIT_BYTES";
inline constexpr const char* kMergeKeys = "SKV_MERGE_KEYS";
inline constexpr const char* kTxnGc = "SKV_TXN_GC";
inline constexpr const char* kStoreHbSec = "SKV_STORE_HB_SEC";
inline constexpr const char* kMetaStoreTtlMs = "SKV_META_STORE_TTL_MS";
}  // namespace env

// ---------------------------------------------------------------------------
// 超时（毫秒）
// ---------------------------------------------------------------------------
namespace timeout_ms {
inline constexpr int kStorageRpcDefault = 3000;
inline constexpr int kStorageRpcAdmin = 60000;
}  // namespace timeout_ms

}  // namespace opskv::proto
