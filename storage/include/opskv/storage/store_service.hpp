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

// @file store_service.hpp
// 存储门面：Command 编解码、Propose/Get、Percolator（含悲观锁 / resolve）。

#pragma once

#include "opskv/storage/latency_hist.hpp"
#include "opskv/storage/mvcc.hpp"
#include "opskv/storage/storage_node.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace opskv::storage {

enum class CmdOp : uint8_t {
  kPut = 0,
  kDelete = 1,
  kPrewrite = 2,
  kCommit = 3,
  kRollback = 4,
  kPessimisticLock = 5,
  kPessimisticRollback = 6,
  kGcBelow = 7,  // value = safe_ts u64；删非最新且 commit_ts < safe_ts
  /// S1：Admin 入 Raft（与 ConfChange 0xFF 前缀不冲突）
  kAdminPrepareSplit = 8,  // key=split_key
  kAdminSplit = 9,         // key=split_key, value=right_region u32
  kAdminMerge = 10,        // value=right_region u32
  kAdminSetEpoch = 11,     // value=epoch u64
};

/// Region epoch 持久化 key（引擎内 meta）。
inline constexpr std::string_view kRegionEpochKeyPrefix = "\xff\xffskv/epoch/";
/// 每 Region 独立 epoch key（同机共享引擎时不可共用单 key）。
inline std::string region_epoch_storage_key(uint32_t region_id) {
  return std::string(kRegionEpochKeyPrefix) + std::to_string(region_id);
}

struct Command {
  CmdOp op{CmdOp::kPut};
  std::string key;
  std::string value;
  int64_t ttl_ms{0};
  /// E4：非 0 时 Leader 去重；编码为 payload 尾部 u64（旧消息无尾部则视为 0）。
  uint64_t request_id{0};
};

std::string encode_command(const Command& cmd);
bool decode_command(const std::string& data, Command* out);

std::string encode_prewrite_value(uint64_t start_ts, uint8_t mvcc_op, std::string_view primary,
                                  std::string_view value);
bool decode_prewrite_value(std::string_view data, uint64_t* start_ts, uint8_t* mvcc_op,
                           std::string* primary, std::string* value);

std::string encode_commit_value(uint64_t start_ts, uint64_t commit_ts);
bool decode_commit_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts);

std::string encode_rollback_value(uint64_t start_ts);
bool decode_rollback_value(std::string_view data, uint64_t* start_ts);

/// PessimisticLock value：start_ts | for_update_ts | ttl_ms | primary_len | primary
std::string encode_pessimistic_lock_value(uint64_t start_ts, uint64_t for_update_ts,
                                          uint64_t ttl_ms, std::string_view primary);
bool decode_pessimistic_lock_value(std::string_view data, uint64_t* start_ts,
                                   uint64_t* for_update_ts, uint64_t* ttl_ms,
                                   std::string* primary);

struct ProposeResult {
  bool ok{false};
  uint64_t revision{0};
  std::string error;
  std::string redirect_to;
};

struct GetResult {
  bool found{false};
  std::string value;
  uint64_t revision{0};
  bool is_leader{false};
  std::string redirect_to;
};

struct TxnGetResult {
  bool found{false};
  bool locked{false};
  std::string value;
  LockInfo blocking_lock;
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

struct GetLockResult {
  bool found{false};
  LockInfo lock;
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

struct CheckTxnResult {
  TxnStatus status{TxnStatus::kRolledBack};
  uint64_t commit_ts{0};
  bool lock_expired{false};
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

class StoreFacade {
 public:
  explicit StoreFacade(StorageNode& node);

  ProposeResult propose(uint32_t shard_id, const Command& cmd);
  GetResult get(uint32_t shard_id, std::string_view key, bool linearizable);

  /// expected_epoch==0 跳过校验。
  ProposeResult prewrite(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                         std::string_view primary, uint8_t mvcc_op, std::string_view value,
                         uint64_t expected_epoch = 0);
  ProposeResult commit(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       uint64_t commit_ts, uint64_t expected_epoch = 0);
  ProposeResult rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                         uint64_t expected_epoch = 0);
  ProposeResult pessimistic_lock(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                 uint64_t for_update_ts, std::string_view primary,
                                 uint64_t ttl_ms = 5000, uint64_t expected_epoch = 0);
  ProposeResult pessimistic_rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                     uint64_t expected_epoch = 0);
  /// commit_ts==0 → rollback resolve。
  ProposeResult resolve_lock(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                             uint64_t commit_ts, uint64_t expected_epoch = 0);

  /// linearizable=true 仅 Leader；false 可读任意副本本地 MVCC（可能落后）。
  TxnGetResult txn_get(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       bool linearizable = true);
  GetLockResult get_lock(uint32_t shard_id, std::string_view key);
  CheckTxnResult check_txn_status(uint32_t shard_id, std::string_view primary, uint64_t lock_ts);

  StorageNode& node() { return node_; }
  RegionMetrics& metrics() { return metrics_; }
  const RegionMetrics& metrics() const { return metrics_; }

 private:
  ProposeResult require_leader(uint32_t shard_id, uint64_t expected_epoch);
  StorageNode& node_;
  RegionMetrics metrics_;
};

}  // namespace opskv::storage
