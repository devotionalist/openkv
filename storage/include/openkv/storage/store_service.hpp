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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "openkv/storage/latency_hist.hpp"
#include "openkv/storage/mvcc.hpp"
#include "openkv/storage/storage_node.hpp"

namespace openkv::storage {

/* Raft 日志里业务/Admin 命令操作码。 */
enum class CmdOp : uint8_t {
  kPut = 0,                  // 普通 Put
  kDelete = 1,               // 普通 Delete
  kPrewrite = 2,             // Percolator 预写
  kCommit = 3,               // 提交
  kRollback = 4,             // 回滚
  kPessimisticLock = 5,      // 悲观锁
  kPessimisticRollback = 6,  // 悲观锁回滚
  kGcBelow = 7,              // value = safe_ts u64；删非最新且 commit_ts < safe_ts
  // S1：Admin 入 Raft（与 ConfChange 0xFF 前缀不冲突）
  kAdminPrepareSplit = 8,  // key=split_key
  kAdminSplit = 9,         // key=split_key, value=right_region u32
  kAdminMerge = 10,        // value=right_region u32
  kAdminSetEpoch = 11,     // value=epoch u64
  // 单 key 1PC：一次 propose 写 data+write（无残留 lock）。
  kOnePc = 12,
};

// Region epoch 持久化 key（引擎内 meta）。
inline constexpr std::string_view kRegionEpochKeyPrefix = "\xff\xffskv/epoch/";
/**
 * 每 Region 独立 epoch key（同机共享引擎时不可共用单 key）。
 * @param region_id Region id
 * @return 引擎内 meta key
 */
inline std::string region_epoch_storage_key(uint32_t region_id) {
  return std::string(kRegionEpochKeyPrefix) + std::to_string(region_id);
}

/* 一条待 propose 的业务/Admin 命令（编码进 Raft 日志）。 */
struct Command {
  CmdOp op{CmdOp::kPut};  // 操作码
  std::string key;        // 用户键或 Admin 载荷键
  std::string value;      // 值或编码后的事务载荷
  // 用户 KV：propose 前打成绝对 expire_at_ms（0=永久）。锁路径仍用独立 value 编码的相对 ttl。
  int64_t ttl_ms{0};
  // 版本 CAS：apply 内校验；plain Put/Del 用 KvRecord.revision，Prewrite 用最新 commit_ts。
  bool cas{false};                // 是否启用 CAS
  uint64_t expected_revision{0};  // CAS 期望版本
};

/**
 * 相对 ttl_ms → 绝对 expire_at；ttl<=0 返回 0。
 * @param ttl_ms 相对毫秒；<=0 表示永久
 * @return 绝对 unix ms；永久为 0
 */
int64_t stamp_expire_at_ms(int64_t ttl_ms);

/**
 * 编码 Command 为 Raft 日志 data。
 * @param cmd 命令
 * @return 字节串
 */
std::string encode_command(const Command& cmd);
/**
 * 解码 Raft 日志 data 为 Command。
 * @param data 日志载荷
 * @param out 输出
 * @return 是否成功
 */
bool decode_command(const std::string& data, Command* out);

/**
 * 编码 Prewrite value：start_ts | op | primary | value。
 * @param start_ts 事务 start_ts
 * @param mvcc_op MvccOp 数值
 * @param primary primary 键
 * @param value 暂存值
 * @return Command.value 载荷
 */
std::string encode_prewrite_value(uint64_t start_ts, uint8_t mvcc_op, std::string_view primary,
                                  std::string_view value);
/**
 * 解码 Prewrite value。
 * @param data Command.value
 * @param start_ts 输出 start_ts
 * @param mvcc_op 输出 MvccOp
 * @param primary 输出 primary
 * @param value 输出暂存值
 * @return 是否成功
 */
bool decode_prewrite_value(std::string_view data, uint64_t* start_ts, uint8_t* mvcc_op,
                           std::string* primary, std::string* value);

/**
 * 编码 Commit value：start_ts | commit_ts。
 * @param start_ts 事务 start_ts
 * @param commit_ts 提交时间戳
 * @return Command.value 载荷
 */
std::string encode_commit_value(uint64_t start_ts, uint64_t commit_ts);
/**
 * 解码 Commit value。
 * @param data Command.value
 * @param start_ts 输出 start_ts
 * @param commit_ts 输出 commit_ts
 * @return 是否成功
 */
bool decode_commit_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts);

/**
 * ONEPC value：commit_ts | prewrite payload（start_ts|op|primary|value）
 * @param start_ts 事务 start_ts
 * @param commit_ts 提交时间戳
 * @param mvcc_op MvccOp 数值
 * @param primary primary 键
 * @param value Put 值
 * @return Command.value 载荷
 */
std::string encode_onepc_value(uint64_t start_ts, uint64_t commit_ts, uint8_t mvcc_op,
                               std::string_view primary, std::string_view value);
/**
 * 解码 ONEPC value。
 * @param data Command.value
 * @param start_ts 输出 start_ts
 * @param commit_ts 输出 commit_ts
 * @param mvcc_op 输出 MvccOp
 * @param primary 输出 primary
 * @param value 输出值
 * @return 是否成功
 */
bool decode_onepc_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts,
                        uint8_t* mvcc_op, std::string* primary, std::string* value);

/**
 * 编码 Rollback value：仅 start_ts。
 * @param start_ts 事务 start_ts
 * @return Command.value 载荷
 */
std::string encode_rollback_value(uint64_t start_ts);
/**
 * 解码 Rollback value。
 * @param data Command.value
 * @param start_ts 输出 start_ts
 * @return 是否成功
 */
bool decode_rollback_value(std::string_view data, uint64_t* start_ts);

/**
 * PessimisticLock value：start_ts | for_update_ts | ttl_ms | primary_len | primary
 * @param start_ts 事务 start_ts
 * @param for_update_ts 悲观锁 for_update_ts
 * @param ttl_ms 锁租约（相对 ms）
 * @param primary primary 键
 * @return Command.value 载荷
 */
std::string encode_pessimistic_lock_value(uint64_t start_ts, uint64_t for_update_ts,
                                          uint64_t ttl_ms, std::string_view primary);
/**
 * 解码 PessimisticLock value。
 * @param data Command.value
 * @param start_ts 输出 start_ts
 * @param for_update_ts 输出 for_update_ts
 * @param ttl_ms 输出锁租约
 * @param primary 输出 primary
 * @return 是否成功
 */
bool decode_pessimistic_lock_value(std::string_view data, uint64_t* start_ts,
                                   uint64_t* for_update_ts, uint64_t* ttl_ms, std::string* primary);

/* propose / 写路径完成结果。 */
struct ProposeResult {
  bool ok{false};                // 是否成功
  uint64_t revision{0};          // 成功时的 apply index
  std::string error;             // 失败原因；空=无错
  std::string redirect_to;       // 非 Leader 时建议跳转地址
  bool cas_failed{false};        // CAS 期望版本不匹配
  uint64_t current_revision{0};  // CAS 失败时的当前版本
};

/* 普通 Get 结果。 */
struct GetResult {
  bool found{false};        // 是否找到
  std::string value;        // 值
  uint64_t revision{0};     // 引擎 revision
  bool is_leader{false};    // 本节点是否 Leader
  std::string redirect_to;  // 非 Leader 时建议跳转
};

/* 事务快照读结果。 */
struct TxnGetResult {
  bool found{false};        // 是否有可见值
  bool locked{false};       // 是否被锁挡住
  std::string value;        // 可见值
  uint64_t revision{0};     // 可见版本 commit_ts；无则为 0
  LockInfo blocking_lock;   // locked 时挡住的锁
  bool is_leader{false};    // 本节点是否 Leader
  std::string redirect_to;  // 非 Leader 时建议跳转
  std::string error;        // 错误串
};

/* 事务范围扫结果。 */
struct TxnScanResult {
  std::vector<MvccStore::ScanPair> pairs;  // 本页命中
  bool truncated{false};                   // 是否还有下一页
  std::string next_key;                    // 下一页 start_after
  bool is_leader{false};                   // 本节点是否 Leader
  std::string redirect_to;                 // 非 Leader 时建议跳转
  std::string error;                       // 错误串
};

/* 事务 TTL 查询结果。 */
struct TxnTtlResult {
  int64_t ttl_ms{-2};       // -2 不存在/过期；-1 永久；>=0 剩余
  bool locked{false};       // 是否被锁挡住
  LockInfo blocking_lock;   // locked 时挡住的锁
  bool is_leader{false};    // 本节点是否 Leader
  std::string redirect_to;  // 非 Leader 时建议跳转
  std::string error;        // 错误串
};

/* 读锁列结果。 */
struct GetLockResult {
  bool found{false};        // 是否有锁
  LockInfo lock;            // 锁内容
  bool is_leader{false};    // 本节点是否 Leader
  std::string redirect_to;  // 非 Leader 时建议跳转
  std::string error;        // 错误串
};

/* check_txn_status 门面结果（含 redirect）。 */
struct CheckTxnResult {
  TxnStatus status{TxnStatus::kRolledBack};  // 事务状态
  uint64_t commit_ts{0};                     // 已提交时的 commit_ts
  bool lock_expired{false};                  // 锁是否过期
  bool is_leader{false};                     // 本节点是否 Leader
  std::string redirect_to;                   // 非 Leader 时建议跳转
  std::string error;                         // 错误串
};

/* 存储业务门面：编解码 + propose/get + Percolator API。 */
class StoreFacade {
 public:
  /**
   * 绑定 StorageNode；不拥有生命周期。
   * @param node 存储节点
   */
  explicit StoreFacade(StorageNode& node);

  /**
   * 异步提议（热路径；gRPC CQ / 后台统一走这）。
   * @param shard_id 分片 / Region id
   * @param cmd 待编码命令
   * @param cb 完成回调（任意线程）
   * @param expected_epoch 期望 Region epoch；0=不校验
   */
  void propose_async(uint32_t shard_id, const Command& cmd, std::function<void(ProposeResult)> cb,
                     uint64_t expected_epoch = 0);

  /**
   * 读单 key；linearizable 时走读索引，否则本地引擎。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param linearizable 是否线性一致读
   * @return Get 结果（含 redirect）
   */
  GetResult get(uint32_t shard_id, std::string_view key, bool linearizable);

  /**
   * 异步 ONEPC：预检失败同步回调；成功走 propose_async。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param commit_ts 提交时间戳
   * @param primary primary 键
   * @param mvcc_op MvccOp 数值
   * @param value Put 值
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cas 是否 CAS
   * @param expected_revision CAS 期望版本
   * @param expire_at_ms 用户值绝对过期；0=永久
   * @param cb 完成回调
   */
  void one_pc_async(uint32_t shard_id, std::string key, uint64_t start_ts, uint64_t commit_ts,
                    std::string primary, uint8_t mvcc_op, std::string value,
                    uint64_t expected_epoch, bool cas, uint64_t expected_revision,
                    int64_t expire_at_ms, std::function<void(ProposeResult)> cb);
  /**
   * 异步 Prewrite。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param primary primary 键
   * @param mvcc_op MvccOp 数值
   * @param value 暂存值
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cas 是否 CAS
   * @param expected_revision CAS 期望版本
   * @param expire_at_ms 用户值绝对过期；0=永久
   * @param cb 完成回调
   */
  void prewrite_async(uint32_t shard_id, std::string key, uint64_t start_ts, std::string primary,
                      uint8_t mvcc_op, std::string value, uint64_t expected_epoch, bool cas,
                      uint64_t expected_revision, int64_t expire_at_ms,
                      std::function<void(ProposeResult)> cb);
  /**
   * 异步 Commit。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param commit_ts 提交时间戳
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cb 完成回调
   */
  void commit_async(uint32_t shard_id, std::string key, uint64_t start_ts, uint64_t commit_ts,
                    uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  /**
   * 异步 Rollback。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cb 完成回调
   */
  void rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                      uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  /**
   * 异步悲观锁。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param for_update_ts 悲观锁 for_update_ts
   * @param primary primary 键
   * @param ttl_ms 锁租约（相对 ms）
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cb 完成回调
   */
  void pessimistic_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                              uint64_t for_update_ts, std::string primary, uint64_t ttl_ms,
                              uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  /**
   * 异步悲观锁回滚。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 事务 start_ts
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cb 完成回调
   */
  void pessimistic_rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                  uint64_t expected_epoch, std::function<void(ProposeResult)> cb);
  /**
   * 异步 resolve lock：commit_ts>0 则提交，否则回滚。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 锁上的 start_ts
   * @param commit_ts >0 提交，否则回滚
   * @param expected_epoch 期望 epoch；0=不校验
   * @param cb 完成回调
   */
  void resolve_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts, uint64_t commit_ts,
                          uint64_t expected_epoch, std::function<void(ProposeResult)> cb);

  /**
   * 事务快照读。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 读时间戳
   * @param linearizable 是否线性一致；默认 true
   * @return 读结果（含锁挡 / redirect）
   */
  TxnGetResult txn_get(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       bool linearizable = true);
  /**
   * MVCC 用户键范围扫；limit==0 → 256；硬顶 4096。
   * @param shard_id 分片 / Region id
   * @param start_key 起点（含）
   * @param end_key 终点（不含）；空=不限
   * @param start_after 分页游标（跳过 <=）
   * @param start_ts 读时间戳
   * @param limit 最多条数；0→256
   * @param keys_only 不返回 value；默认 false
   * @param linearizable 是否线性一致；默认 true
   * @return 扫描结果
   */
  TxnScanResult txn_scan(uint32_t shard_id, std::string_view start_key, std::string_view end_key,
                         std::string_view start_after, uint64_t start_ts, uint32_t limit,
                         bool keys_only = false, bool linearizable = true);
  /**
   * 查用户 KV 剩余 TTL。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @param start_ts 读时间戳
   * @param linearizable 是否线性一致；默认 true
   * @return TTL 结果
   */
  TxnTtlResult txn_ttl(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                       bool linearizable = true);
  /**
   * 读某 key 的锁列。
   * @param shard_id 分片 / Region id
   * @param key 用户键
   * @return 锁结果
   */
  GetLockResult get_lock(uint32_t shard_id, std::string_view key);
  /**
   * 查 primary 事务状态。
   * @param shard_id 分片 / Region id
   * @param primary primary 用户键
   * @param lock_ts 期望的锁 start_ts
   * @return 状态结果
   */
  CheckTxnResult check_txn_status(uint32_t shard_id, std::string_view primary, uint64_t lock_ts);

  /**
   * 底层存储节点。
   * @return 节点引用
   */
  StorageNode& node() { return node_; }
  /**
   * 按 Region 延迟指标（可写）。
   * @return 指标引用
   */
  RegionMetrics& metrics() { return *metrics_; }
  /**
   * 按 Region 延迟指标（只读）。
   * @return 指标常量引用
   */
  const RegionMetrics& metrics() const { return *metrics_; }

 private:
  /**
   * 要求本节点是 Leader 且 epoch 匹配；否则填 redirect/error。
   * @param shard_id 分片 / Region id
   * @param expected_epoch 期望 epoch；0=不校验
   * @return ok=true 表示可继续写
   */
  ProposeResult require_leader(uint32_t shard_id, uint64_t expected_epoch);
  StorageNode& node_;  // 存储节点（不拥有）
  // shared：propose_async 回调可能晚于临时 StoreFacade（如 txn GC）析构。
  std::shared_ptr<RegionMetrics> metrics_;
};

}  // namespace openkv::storage
