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

// @file store_rpc.hpp
// Gateway → Storage：StoreService / AdminService 类型化 gRPC。

#pragma once

#include "openkv/compute/grpc_channel.hpp"

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace openkv::compute {

/// Storage 返回 locked（带 primary / lock_ts）。
class LockedError : public std::runtime_error {
 public:
  LockedError(std::string primary, uint64_t lock_ts, std::string raw)
      : std::runtime_error(raw),
        primary_(std::move(primary)),
        lock_ts_(lock_ts),
        raw_(std::move(raw)) {}

  const std::string& primary() const { return primary_; }
  uint64_t lock_ts() const { return lock_ts_; }
  const std::string& raw() const { return raw_; }

 private:
  std::string primary_;
  uint64_t lock_ts_{0};
  std::string raw_;
};

/// 从 `ERR locked primary=.. lock_ts=..` 解析为 LockedError。
LockedError parse_locked_error(const std::string& resp);

/// 单次 AdminService.Ping（集群就绪探测）。
void store_admin_ping(const std::string& addr, int timeout_ms = 3000);

/// 异步 AdminService.SetEpoch。
void store_admin_set_epoch_async(const std::string& addr, uint32_t region_id, uint64_t epoch,
                                 int timeout_ms, std::function<void(grpc::Status, std::string error)> cb);

/// StoreService.Propose 结果（ONEPC / PREWRITE / COMMIT / … 共用）。
struct ProposeOnePcResult {
  bool ok{false};
  uint64_t revision{0};
  std::string error;
  std::string redirect_to;
  bool cas_failed{false};
  uint64_t current_revision{0};
};
using ProposeResult = ProposeOnePcResult;

/// ProposeRequest.Op 数值（与 storage.proto 对齐，避免头文件拉过多细节）。
enum class ProposeOp : int {
  kPut = 0,
  kDelete = 1,
  kOnePc = 2,
  kPrewrite = 3,
  kCommit = 4,
  kRollback = 5,
  kPessimisticLock = 6,
  kPessimisticRollback = 7,
  kResolveLock = 8,
};

struct ProposeOnePcArgs {
  uint32_t shard_id{0};
  std::string key;
  std::string value;
  uint64_t start_ts{0};
  uint64_t commit_ts{0};
  std::string primary;  // 空则等同 key
  uint32_t mvcc_op{0};  // 0=Put 1=Delete
  uint64_t epoch{0};
  bool cas{false};
  uint64_t expected_revision{0};
  int64_t expire_at_ms{0};  // 绝对时间；0=无 TTL
};

void store_propose_onepc_async(const std::string& addr, ProposeOnePcArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposeOnePcResult)> cb);

struct ProposePlainArgs {
  uint32_t shard_id{0};
  bool is_delete{false};
  std::string key;
  std::string value;
  int64_t ttl_ms{0};  // 相对寿命；Storage 侧 stamp 成绝对 expire
  uint64_t epoch{0};
  bool cas{false};
  uint64_t expected_revision{0};
};

using ProposePlainResult = ProposeOnePcResult;

void store_propose_plain_async(const std::string& addr, ProposePlainArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposePlainResult)> cb);

struct ProposeTxnArgs {
  uint32_t shard_id{0};
  ProposeOp op{ProposeOp::kPrewrite};
  std::string key;
  std::string value;
  uint64_t start_ts{0};
  uint64_t commit_ts{0};
  std::string primary;
  uint32_t mvcc_op{0};
  uint64_t epoch{0};
  bool cas{false};
  uint64_t expected_revision{0};
  int64_t expire_at_ms{0};
  uint64_t for_update_ts{0};
  uint64_t lock_ttl_ms{0};
};

void store_propose_op_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                            std::function<void(grpc::Status, ProposeResult)> cb);

void store_prewrite_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb);
void store_commit_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                        std::function<void(grpc::Status, ProposeResult)> cb);
void store_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb);
void store_pessimistic_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                  std::function<void(grpc::Status, ProposeResult)> cb);
void store_pessimistic_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                      std::function<void(grpc::Status, ProposeResult)> cb);
void store_resolve_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                              std::function<void(grpc::Status, ProposeResult)> cb);

struct StoreGetResult {
  bool found{false};
  std::string value;
  uint64_t revision{0};
  bool is_leader{false};
  std::string redirect_to;
  bool locked{false};
  std::string error;
  std::string lock_primary;
  uint64_t lock_ts{0};
};

void store_get_async(const std::string& addr, uint32_t shard_id, std::string key, bool linearizable,
                     uint64_t start_ts, int timeout_ms,
                     std::function<void(grpc::Status, StoreGetResult)> cb);

struct StoreScanPair {
  std::string key;
  std::string value;
  uint64_t revision{0};
};

struct StoreScanResult {
  std::vector<StoreScanPair> pairs;
  bool truncated{false};
  std::string next_key;
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

struct StoreScanArgs {
  uint32_t shard_id{0};
  std::string start_key;
  std::string end_key;
  std::string start_after;
  uint64_t start_ts{0};
  uint32_t limit{0};
  bool keys_only{false};
  bool linearizable{true};
};

void store_scan_async(const std::string& addr, StoreScanArgs args, int timeout_ms,
                      std::function<void(grpc::Status, StoreScanResult)> cb);

struct StoreTtlResult {
  int64_t ttl_ms{-2};
  bool locked{false};
  std::string lock_primary;
  uint64_t lock_ts{0};
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

void store_ttl_async(const std::string& addr, uint32_t shard_id, std::string key, uint64_t start_ts,
                     bool linearizable, int timeout_ms,
                     std::function<void(grpc::Status, StoreTtlResult)> cb);

struct StoreCheckTxnResult {
  std::string status;
  uint64_t commit_ts{0};
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

void store_check_txn_status_async(const std::string& addr, uint32_t shard_id, std::string primary,
                                  uint64_t lock_ts, int timeout_ms,
                                  std::function<void(grpc::Status, StoreCheckTxnResult)> cb);

struct StoreWatchHeadResult {
  uint64_t revision{0};
  std::string redirect_to;
  std::string error;
};

void store_watch_head_async(const std::string& addr, uint32_t shard_id, int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchHeadResult)> cb);

struct StoreWatchEvent {
  std::string key;
  std::string value;
  uint64_t revision{0};
  bool deleted{false};
};

struct StoreWatchPollResult {
  std::vector<StoreWatchEvent> events;
  uint64_t max_revision{0};
  std::string redirect_to;
  std::string error;
};

void store_watch_poll_async(const std::string& addr, uint32_t shard_id, uint64_t since, int32_t limit,
                            int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchPollResult)> cb);

}  // namespace openkv::compute
