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

// @file mvcc.hpp
// Percolator 风格 MVCC（含悲观锁）。
//
// 物理 key：user + '\0' + kind[+ ts]
//   L  lock（乐观 prewrite / 悲观）
//   D  data + start_ts
//   W  write + ~commit_ts

#pragma once

#include "opskv/storage/kv_engine.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::storage {

enum class MvccOp : uint8_t { kPut = 0, kDelete = 1, kPessimistic = 2 };

struct LockInfo {
  uint64_t start_ts{0};
  MvccOp op{MvccOp::kPut};
  std::string primary;
  std::string value;
  uint64_t for_update_ts{0};
  uint64_t ttl_ms{5000};
  int64_t lock_wall_ms{0};
};

struct WriteInfo {
  uint64_t start_ts{0};
  MvccOp op{MvccOp::kPut};
};

enum class TxnStatus : uint8_t {
  kLocked = 0,
  kCommitted = 1,
  kRolledBack = 2,
};

struct CheckTxnStatusResult {
  TxnStatus status{TxnStatus::kRolledBack};
  uint64_t commit_ts{0};
  uint64_t lock_ttl_ms{0};
  bool lock_expired{false};
};

std::string lock_key(std::string_view user);
std::string data_key(std::string_view user, uint64_t start_ts);
std::string write_key(std::string_view user, uint64_t commit_ts);
std::string write_key_prefix(std::string_view user);

std::string encode_lock(const LockInfo& li);
bool decode_lock(std::string_view payload, LockInfo* out);

std::string encode_write(const WriteInfo& wi);
bool decode_write(std::string_view payload, WriteInfo* out);

std::string user_key_of(std::string_view physical);
int64_t wall_ms_now();

class MvccStore {
 public:
  explicit MvccStore(KvEngine& eng) : eng_(eng) {}

  std::optional<LockInfo> get_lock(std::string_view user) const;
  std::optional<std::string> txn_get(std::string_view user, uint64_t start_ts,
                                     bool* locked, LockInfo* blocking_lock = nullptr) const;
  bool has_write_conflict(std::string_view user, uint64_t start_ts) const;
  /// 查找 start_ts 对应的 commit_ts（若已提交）。
  std::optional<uint64_t> find_commit_ts(std::string_view user, uint64_t start_ts) const;
  CheckTxnStatusResult check_txn_status(std::string_view primary, uint64_t lock_ts) const;

  void put_lock(std::string_view user, const LockInfo& li, uint64_t rev);
  void put_data(std::string_view user, uint64_t start_ts, std::string_view value, uint64_t rev);
  void put_write(std::string_view user, uint64_t commit_ts, const WriteInfo& wi, uint64_t rev);
  void clear_lock(std::string_view user, uint64_t rev);
  void clear_data(std::string_view user, uint64_t start_ts, uint64_t rev);
  void clear_write(std::string_view user, uint64_t commit_ts, uint64_t rev);

  /// 收集过期悲观/乐观锁的 user key（按 ttl + lock_wall_ms）。
  std::vector<std::string> list_expired_locks() const;
  /// T2：本引擎所有锁的最小 start_ts；无锁返回 UINT64_MAX。
  uint64_t min_lock_start_ts() const;
  /// 删除 commit_ts < safe_ts 的旧 write/data，每个 user 至少保留最新一版。
  size_t gc_below(uint64_t safe_ts);

 private:
  KvEngine& eng_;
};

}  // namespace opskv::storage
