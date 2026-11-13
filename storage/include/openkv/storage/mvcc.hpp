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

#include "openkv/storage/kv_engine.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace openkv::storage {

enum class MvccOp : uint8_t { kPut = 0, kDelete = 1, kPessimistic = 2 };

struct LockInfo {// 锁信息
  uint64_t start_ts{0}; // 开始时间
  MvccOp op{MvccOp::kPut}; // 操作
  std::string primary; // 主键
  std::string value; // 值
  uint64_t for_update_ts{0}; // 更新时间
  uint64_t ttl_ms{5000}; // 超时时间
  int64_t lock_wall_ms{0}; // 锁墙时间
  /// 用户 KV TTL（绝对 ms）；与锁租约 ttl_ms 无关。0=永久。
  int64_t user_expire_at_ms{0};
};

struct WriteInfo {// 写信息
  uint64_t start_ts{0}; // 开始时间
  MvccOp op{MvccOp::kPut}; // 操作
  int64_t expire_at_ms{0}; // 过期时间
};

enum class TxnStatus : uint8_t { // 事务状态
  kLocked = 0, // 锁定
  kCommitted = 1, // 已提交
  kRolledBack = 2, // 回滚
};

struct CheckTxnStatusResult {// 检查事务状态结果
  TxnStatus status{TxnStatus::kRolledBack}; // 状态
  uint64_t commit_ts{0}; // 提交时间
  uint64_t lock_ttl_ms{0}; // 锁超时时间
  bool lock_expired{false}; // 锁是否过期
};

std::string lock_key(std::string_view user); // 锁键
std::string data_key(std::string_view user, uint64_t start_ts); // 数据键
std::string write_key(std::string_view user, uint64_t commit_ts); // 写键
std::string write_key_prefix(std::string_view user); // 写键前缀

std::string encode_lock(const LockInfo& li); // 编码锁
bool decode_lock(std::string_view payload, LockInfo* out); // 解码锁

std::string encode_write(const WriteInfo& wi); // 编码写
bool decode_write(std::string_view payload, WriteInfo* out); // 解码写

std::string user_key_of(std::string_view physical); // 用户键
int64_t wall_ms_now(); // 墙钟时间

class MvccStore {// MVCC 存储
 public:
  explicit MvccStore(KvEngine& eng) : eng_(eng) {}

  struct ScanPair {// 扫描对      
    std::string key; // 键
    std::string value; // 值
    uint64_t revision{0};  ///< 可见 commit_ts
  };
  struct ScanResult {
    std::vector<ScanPair> pairs; // 对
    bool truncated{false}; // 是否截断
    std::string next_key;  ///< truncated 时下一页 start_after（用户键）
  };

  std::optional<LockInfo> get_lock(std::string_view user) const; // 获取锁
  std::optional<std::string> txn_get(std::string_view user, uint64_t start_ts, bool* locked, LockInfo* blocking_lock = nullptr, uint64_t* out_commit_ts = nullptr) const; // 获取事务
  std::optional<int64_t> txn_ttl(std::string_view user, uint64_t start_ts, bool* locked, LockInfo* blocking_lock = nullptr) const; // 获取事务 TTL
  ScanResult txn_scan(std::string_view start_key, std::string_view end_key, std::string_view start_after, uint64_t start_ts, uint32_t limit, bool keys_only, const openkv::KeyRange& region_range = {}) const; // 扫描事务
  std::optional<uint64_t> latest_commit_ts(std::string_view user) const; // 最新已提交写的时间戳
  bool has_write_conflict(std::string_view user, uint64_t start_ts) const; // 是否有写冲突
  std::optional<uint64_t> find_commit_ts(std::string_view user, uint64_t start_ts) const; // 查找 start_ts 对应的 commit_ts（若已提交）
  CheckTxnStatusResult check_txn_status(std::string_view primary, uint64_t lock_ts) const; // 检查事务状态

  void put_lock(std::string_view user, const LockInfo& li, uint64_t rev);
  void put_data(std::string_view user, uint64_t start_ts, std::string_view value, uint64_t rev,
                int64_t expire_at_ms = 0);
  void put_write(std::string_view user, uint64_t commit_ts, const WriteInfo& wi, uint64_t rev);
  /// ONEPC：data+write 一次 WriteBatch（可无 data，仅写 delete tombstone）。
  bool put_onepc(std::string_view user, uint64_t start_ts, uint64_t commit_ts, MvccOp op,
                 std::string_view value, uint64_t rev, int64_t expire_at_ms = 0);
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

}  // namespace openkv::storage
