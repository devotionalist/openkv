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

#include "openkv/storage/latency_hist.hpp"
#include "openkv/storage/mvcc.hpp"
#include "openkv/storage/storage_node.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace openkv::storage {

enum class CmdOp : uint8_t {// 命令操作
  kPut = 0, // 插入
  kDelete = 1,
  kPrewrite = 2, // 预写
  kCommit = 3, // 提交
  kRollback = 4, // 回滚
  kPessimisticLock = 5,
  kPessimisticRollback = 6,
  kGcBelow = 7,  // value = safe_ts u64；删非最新且 commit_ts < safe_ts
  /// S1：Admin 入 Raft（与 ConfChange 0xFF 前缀不冲突）
  kAdminPrepareSplit = 8,  // key=split_key
  kAdminSplit = 9,         // key=split_key, value=right_region u32
  kAdminMerge = 10,        // value=right_region u32
  kAdminSetEpoch = 11,     // value=epoch u64
  /// 单 key 1PC：一次 propose 写 data+write（无残留 lock）。
  kOnePc = 12,
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
  /// 用户 KV：propose 前打成绝对 expire_at_ms（0=永久）。锁路径仍用独立 value 编码的相对 ttl。
  int64_t ttl_ms{0};
  /// 版本 CAS：apply 内校验；plain Put/Del 用 KvRecord.revision，Prewrite 用最新 commit_ts。
  bool cas{false};
  uint64_t expected_revision{0};
};

/// 相对 ttl_ms → 绝对 expire_at；ttl<=0 返回 0。
int64_t stamp_expire_at_ms(int64_t ttl_ms);

std::string encode_command(const Command& cmd);
bool decode_command(const std::string& data, Command* out);

std::string encode_prewrite_value(uint64_t start_ts, uint8_t mvcc_op, std::string_view primary,
                                  std::string_view value);
bool decode_prewrite_value(std::string_view data, uint64_t* start_ts, uint8_t* mvcc_op,
                           std::string* primary, std::string* value);

std::string encode_commit_value(uint64_t start_ts, uint64_t commit_ts);
bool decode_commit_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts);

/// ONEPC value：commit_ts | prewrite payload（start_ts|op|primary|value）
std::string encode_onepc_value(uint64_t start_ts, uint64_t commit_ts, uint8_t mvcc_op,
                               std::string_view primary, std::string_view value);
bool decode_onepc_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts,
                        uint8_t* mvcc_op, std::string* primary, std::string* value);

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
  bool cas_failed{false};
  uint64_t current_revision{0};
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
  uint64_t revision{0};  ///< 可见版本 commit_ts；无则为 0
  LockInfo blocking_lock;
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

struct TxnScanResult {
  std::vector<MvccStore::ScanPair> pairs;
  bool truncated{false};
  std::string next_key;
  bool is_leader{false};
  std::string redirect_to;
  std::string error;
};

struct TxnTtlResult {
  int64_t ttl_ms{-2};  ///< -2 不存在/过期；-1 永久；>=0 剩余
  bool locked{false};
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

  /// 同步 propose（内部 promise 等 apply）。**仅单测 / 后台**；gRPC CQ 用 propose_async。
  ProposeResult propose(uint32_t shard_id, const Command& cmd);
  /// 异步提议（热路径）。
  void propose_async(uint32_t shard_id, const Command& cmd,
                     std::function<void(ProposeResult)> cb, uint64_t expected_epoch = 0);

  // 获取  
  GetResult get(uint32_t shard_id, std::string_view key, bool linearizable);


  // 预写
  ProposeResult prewrite(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                         std::string_view primary, uint8_t mvcc_op, std::string_view value,
                         uint64_t expected_epoch = 0, bool cas = false,
                         uint64_t expected_revision = 0, int64_t expire_at_ms = 0);
  // 单 key 原子写（Put/Delete）；revision 返回 commit_ts。
  ProposeResult one_pc(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       uint64_t commit_ts, std::string_view primary, uint8_t mvcc_op,
                       std::string_view value, uint64_t expected_epoch = 0, bool cas = false,
                       uint64_t expected_revision = 0, int64_t expire_at_ms = 0);
  // 异步 ONEPC：预检失败同步回调；成功走 propose_async（不占调用线程等 apply）。
  void one_pc_async(uint32_t shard_id, std::string key, uint64_t start_ts, uint64_t commit_ts,
                    std::string primary, uint8_t mvcc_op, std::string value,
                    uint64_t expected_epoch, bool cas, uint64_t expected_revision,
                    int64_t expire_at_ms, std::function<void(ProposeResult)> cb);
  // 异步 Prewrite：预检同 sync；成功走 propose_async。
  void prewrite_async(uint32_t shard_id, std::string key, uint64_t start_ts, std::string primary,
                      uint8_t mvcc_op, std::string value, uint64_t expected_epoch, bool cas,
                      uint64_t expected_revision, int64_t expire_at_ms,
                      std::function<void(ProposeResult)> cb);
  // 提交
  ProposeResult commit(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       uint64_t commit_ts, uint64_t expected_epoch = 0);
  // 异步提交
  void commit_async(uint32_t shard_id, std::string key, uint64_t start_ts, uint64_t commit_ts,
                    uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  // 回滚
  ProposeResult rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                         uint64_t expected_epoch = 0);
  // 异步回滚
  void rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                      uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  // 悲观锁
  ProposeResult pessimistic_lock(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                 uint64_t for_update_ts, std::string_view primary,
                                 uint64_t ttl_ms = 5000, uint64_t expected_epoch = 0);
  // 异步悲观锁
  void pessimistic_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                              uint64_t for_update_ts, std::string primary, uint64_t ttl_ms,
                              uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  // 悲观回滚
  ProposeResult pessimistic_rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                     uint64_t expected_epoch = 0);
  // 异步悲观回滚
  void pessimistic_rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                  uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  // 解决锁
  ProposeResult resolve_lock(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                             uint64_t commit_ts, uint64_t expected_epoch = 0);
  // 异步解决锁
  void resolve_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                          uint64_t commit_ts, uint64_t expected_epoch,
                          std::function<void(ProposeResult)> cb);

  // 事务获取
  TxnGetResult txn_get(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       bool linearizable = true);
  // MVCC 用户键范围扫；limit==0 → 256；硬顶 4096。
  TxnScanResult txn_scan(uint32_t shard_id, std::string_view start_key, std::string_view end_key,
                         std::string_view start_after, uint64_t start_ts, uint32_t limit,
                         bool keys_only = false, bool linearizable = true);
  // 事务 TTL
  TxnTtlResult txn_ttl(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       bool linearizable = true);
  // 获取锁
  GetLockResult get_lock(uint32_t shard_id, std::string_view key);
  // 检查事务状态
  CheckTxnResult check_txn_status(uint32_t shard_id, std::string_view primary, uint64_t lock_ts);

  StorageNode& node() { return node_; }
  RegionMetrics& metrics() { return *metrics_; }
  const RegionMetrics& metrics() const { return *metrics_; }

 private:
  ProposeResult require_leader(uint32_t shard_id, uint64_t expected_epoch); // 需要领导者
  StorageNode& node_; // 存储节点
  /// shared：propose_async 回调可能晚于临时 StoreFacade（如 txn GC）析构。
  std::shared_ptr<RegionMetrics> metrics_;
};

}  // namespace openkv::storage
