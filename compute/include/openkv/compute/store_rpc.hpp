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

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/compute/grpc_channel.hpp"

namespace openkv::compute {

/* Storage 返回 locked（带 primary / lock_ts）。 */
class LockedError : public std::runtime_error {
 public:
  /**
   * @param primary 锁的 primary key
   * @param lock_ts 持锁事务的 start_ts
   * @param raw 原始错误正文（供日志）
   */
  LockedError(std::string primary, uint64_t lock_ts, std::string raw)
      : std::runtime_error(raw),
        primary_(std::move(primary)),
        lock_ts_(lock_ts),
        raw_(std::move(raw)) {}

  /**
   * 锁的 primary key。
   */
  const std::string& primary() const { return primary_; }
  /**
   * 持锁事务 start_ts。
   */
  uint64_t lock_ts() const { return lock_ts_; }
  /**
   * 原始错误正文。
   */
  const std::string& raw() const { return raw_; }

 private:
  std::string primary_;  // primary key
  uint64_t lock_ts_{0};  // 持锁 start_ts
  std::string raw_;      // 原始正文
};

/**
 * 从 `ERR locked primary=.. lock_ts=..` 解析为 LockedError。
 * @param resp 错误正文
 * @return 解析后的 LockedError
 */
LockedError parse_locked_error(const std::string& resp);

/**
 * 单次 AdminService.Ping（集群就绪探测）。
 * @param addr Storage 地址
 * @param timeout_ms 超时毫秒
 */
void store_admin_ping(const std::string& addr, int timeout_ms = 3000);

/**
 * 异步 AdminService.SetEpoch。
 * @param addr Storage 地址
 * @param region_id Region id
 * @param epoch 配置纪元
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调（status + 可选错误串）
 */
void store_admin_set_epoch_async(const std::string& addr, uint32_t region_id, uint64_t epoch,
                                 int timeout_ms,
                                 std::function<void(grpc::Status, std::string error)> cb);

/* StoreService.Propose 结果（ONEPC / PREWRITE / COMMIT / … 共用）。 */
struct ProposeOnePcResult {
  bool ok{false};                // 是否成功
  uint64_t revision{0};          // 新 revision（成功时）
  std::string error;             // 错误正文
  std::string redirect_to;       // 非 Leader 时下一跳
  bool cas_failed{false};        // CAS 未命中
  uint64_t current_revision{0};  // CAS 失败时的当前 revision
};
using ProposeResult = ProposeOnePcResult;

/* ProposeRequest.Op 数值（与 storage.proto 对齐，避免头文件拉过多细节）。 */
enum class ProposeOp : int {
  kPut = 0,                  // 明文 Put
  kDelete = 1,               // 明文 Delete
  kOnePc = 2,                // 单键 1PC
  kPrewrite = 3,             // 预写
  kCommit = 4,               // 提交
  kRollback = 5,             // 回滚
  kPessimisticLock = 6,      // 悲观锁
  kPessimisticRollback = 7,  // 悲观锁回滚
  kResolveLock = 8,          // 解析锁
};

/* 1PC Propose 参数。 */
struct ProposeOnePcArgs {
  uint32_t shard_id{0};           // Region id
  std::string key;                // 用户键
  std::string value;              // 值（Delete 可空）
  uint64_t start_ts{0};           // 事务 start_ts
  uint64_t commit_ts{0};          // 提交 ts
  std::string primary;            // 空则等同 key
  uint32_t mvcc_op{0};            // 0=Put 1=Delete
  uint64_t epoch{0};              // 路由 epoch
  bool cas{false};                // 是否带期望 revision
  uint64_t expected_revision{0};  // CAS 期望
  int64_t expire_at_ms{0};        // 绝对过期毫秒；0=无 TTL
};

/**
 * 异步 1PC Propose。
 * @param addr Storage 地址
 * @param args 1PC 参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_propose_onepc_async(const std::string& addr, ProposeOnePcArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposeOnePcResult)> cb);

/* 明文 Put/Delete Propose 参数（无事务）。 */
struct ProposePlainArgs {
  uint32_t shard_id{0};           // Region id
  bool is_delete{false};          // true=Delete
  std::string key;                // 用户键
  std::string value;              // Put 值
  int64_t ttl_ms{0};              // 相对寿命；Storage 侧 stamp 成绝对 expire
  uint64_t epoch{0};              // 路由 epoch
  bool cas{false};                // 是否 CAS
  uint64_t expected_revision{0};  // CAS 期望
};

using ProposePlainResult = ProposeOnePcResult;

/**
 * 异步明文 Put/Delete。
 * @param addr Storage 地址
 * @param args 明文参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_propose_plain_async(const std::string& addr, ProposePlainArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposePlainResult)> cb);

/* 事务类 Propose 参数（Prewrite/Commit/…）。 */
struct ProposeTxnArgs {
  uint32_t shard_id{0};                // Region id
  ProposeOp op{ProposeOp::kPrewrite};  // 操作类型
  std::string key;                     // 用户键
  std::string value;                   // Prewrite 值等
  uint64_t start_ts{0};                // start_ts
  uint64_t commit_ts{0};               // commit_ts（Commit/Resolve）
  std::string primary;                 // primary key
  uint32_t mvcc_op{0};                 // 0=Put 1=Delete
  uint64_t epoch{0};                   // 路由 epoch
  bool cas{false};                     // 是否 CAS
  uint64_t expected_revision{0};       // CAS 期望
  int64_t expire_at_ms{0};             // 锁/值绝对过期
  uint64_t for_update_ts{0};           // 悲观锁 for_update_ts
  uint64_t lock_ttl_ms{0};             // 锁 TTL
};

/**
 * 异步通用事务 Propose（按 args.op）。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_propose_op_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                            std::function<void(grpc::Status, ProposeResult)> cb);

/**
 * 异步 Prewrite。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_prewrite_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb);
/**
 * 异步 Commit。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_commit_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                        std::function<void(grpc::Status, ProposeResult)> cb);
/**
 * 异步 Rollback。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb);
/**
 * 异步悲观锁。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_pessimistic_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                  std::function<void(grpc::Status, ProposeResult)> cb);
/**
 * 异步悲观锁回滚。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_pessimistic_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                      std::function<void(grpc::Status, ProposeResult)> cb);
/**
 * 异步 ResolveLock。
 * @param addr Storage 地址
 * @param args 事务参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_resolve_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                              std::function<void(grpc::Status, ProposeResult)> cb);

/* StoreService.Get 结果。 */
struct StoreGetResult {
  bool found{false};         // 是否命中未删版本
  std::string value;         // 值
  uint64_t revision{0};      // 版本
  bool is_leader{false};     // 对端是否自认 Leader
  std::string redirect_to;   // 非 Leader 下一跳
  bool locked{false};        // 是否撞锁
  std::string error;         // 错误正文
  std::string lock_primary;  // 锁 primary
  uint64_t lock_ts{0};       // 锁 start_ts
};

/**
 * 异步 Get（可带 start_ts 做快照读）。
 * @param addr Storage 地址
 * @param shard_id Region id
 * @param key 用户键
 * @param linearizable 是否走 Leader 线性读
 * @param start_ts 快照 ts；0=读最新
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_get_async(const std::string& addr, uint32_t shard_id, std::string key, bool linearizable,
                     uint64_t start_ts, int timeout_ms,
                     std::function<void(grpc::Status, StoreGetResult)> cb);

/* Scan 返回的一对 kv。 */
struct StoreScanPair {
  std::string key;       // 键
  std::string value;     // 值（keys_only 时可空）
  uint64_t revision{0};  // 版本
};

/* StoreService.Scan 结果。 */
struct StoreScanResult {
  std::vector<StoreScanPair> pairs;  // 本页结果
  bool truncated{false};             // 是否还有后续
  std::string next_key;              // 下一页起点
  bool is_leader{false};             // 对端是否 Leader
  std::string redirect_to;           // 非 Leader 下一跳
  std::string error;                 // 错误正文
};

/* Scan 请求参数。 */
struct StoreScanArgs {
  uint32_t shard_id{0};     // Region id
  std::string start_key;    // 区间起点（含）
  std::string end_key;      // 区间终点（不含）；空=+∞
  std::string start_after;  // 严格大于此 key 起扫（翻页）
  uint64_t start_ts{0};     // 快照 ts；0=最新
  uint32_t limit{0};        // 本页上限；0=默认
  bool keys_only{false};    // 只返回 key
  bool linearizable{true};  // 是否线性读
};

/**
 * 异步 Scan。
 * @param addr Storage 地址
 * @param args Scan 参数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_scan_async(const std::string& addr, StoreScanArgs args, int timeout_ms,
                      std::function<void(grpc::Status, StoreScanResult)> cb);

/* TTL 查询结果。 */
struct StoreTtlResult {
  int64_t ttl_ms{-2};        // 剩余毫秒；-2=不存在；-1=无过期
  bool locked{false};        // 是否撞锁
  std::string lock_primary;  // 锁 primary
  uint64_t lock_ts{0};       // 锁 start_ts
  bool is_leader{false};     // 对端是否 Leader
  std::string redirect_to;   // 非 Leader 下一跳
  std::string error;         // 错误正文
};

/**
 * 异步查 TTL。
 * @param addr Storage 地址
 * @param shard_id Region id
 * @param key 用户键
 * @param start_ts 快照 ts
 * @param linearizable 是否线性读
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_ttl_async(const std::string& addr, uint32_t shard_id, std::string key, uint64_t start_ts,
                     bool linearizable, int timeout_ms,
                     std::function<void(grpc::Status, StoreTtlResult)> cb);

/* CheckTxnStatus 结果。 */
struct StoreCheckTxnResult {
  std::string status;       // 状态串（locked/committed/…）
  uint64_t commit_ts{0};    // 已提交则带 commit_ts
  bool is_leader{false};    // 对端是否 Leader
  std::string redirect_to;  // 非 Leader 下一跳
  std::string error;        // 错误正文
};

/**
 * 异步查事务状态（primary 上）。
 * @param addr Storage 地址
 * @param shard_id Region id
 * @param primary primary key
 * @param lock_ts 锁/事务 start_ts
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_check_txn_status_async(const std::string& addr, uint32_t shard_id, std::string primary,
                                  uint64_t lock_ts, int timeout_ms,
                                  std::function<void(grpc::Status, StoreCheckTxnResult)> cb);

/* WatchHead：当前 Region 最大 revision。 */
struct StoreWatchHeadResult {
  uint64_t revision{0};     // 当前头 revision
  std::string redirect_to;  // 非 Leader 下一跳
  std::string error;        // 错误正文
};

/**
 * 异步取 Watch 头 revision。
 * @param addr Storage 地址
 * @param shard_id Region id
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_watch_head_async(const std::string& addr, uint32_t shard_id, int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchHeadResult)> cb);

/* 单条 Watch 事件。 */
struct StoreWatchEvent {
  std::string key;       // 键
  std::string value;     // 新值（删除可空）
  uint64_t revision{0};  // 事件 revision
  bool deleted{false};   // 是否删除
};

/* WatchPoll 一页结果。 */
struct StoreWatchPollResult {
  std::vector<StoreWatchEvent> events;  // 本页事件
  uint64_t max_revision{0};             // 本页最大 revision
  std::string redirect_to;              // 非 Leader 下一跳
  std::string error;                    // 错误正文
};

/**
 * 异步拉取 since 之后的变更（短轮询）。
 * @param addr Storage 地址
 * @param shard_id Region id
 * @param since 已知 revision；大于此值的事件
 * @param limit 本页上限
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void store_watch_poll_async(const std::string& addr, uint32_t shard_id, uint64_t since,
                            int32_t limit, int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchPollResult)> cb);

}  // namespace openkv::compute
