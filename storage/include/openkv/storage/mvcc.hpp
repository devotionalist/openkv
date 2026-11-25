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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/storage/kv_engine.hpp"

namespace openkv::storage {

/* MVCC 写意图类型：Put / Delete / 悲观锁占位。 */
enum class MvccOp : uint8_t {
  kPut = 0,          // 写入
  kDelete = 1,       // 删除
  kPessimistic = 2,  // 悲观锁（无 data）
};

/* 锁列（L）解码后的内容。 */
struct LockInfo {
  uint64_t start_ts{0};       // 事务 start_ts
  MvccOp op{MvccOp::kPut};    // 写意图类型
  std::string primary;        // 主键（primary key）
  std::string value;          // prewrite 暂存值；悲观锁可空
  uint64_t for_update_ts{0};  // 悲观锁 for_update_ts
  uint64_t ttl_ms{5000};      // 锁租约（相对 ms）
  int64_t lock_wall_ms{0};    // 上锁墙钟 ms（判过期）
  // 用户 KV TTL（绝对 ms）；与锁租约 ttl_ms 无关。0=永久。
  int64_t user_expire_at_ms{0};
};

/* 写列（W）解码后的内容。 */
struct WriteInfo {
  uint64_t start_ts{0};     // 对应事务 start_ts
  MvccOp op{MvccOp::kPut};  // 提交的操作类型
  int64_t expire_at_ms{0};  // 用户值绝对过期；0=永久
};

/* 查 primary 时事务所处状态。 */
enum class TxnStatus : uint8_t {
  kLocked = 0,      // 仍持锁
  kCommitted = 1,   // 已提交
  kRolledBack = 2,  // 已回滚 / 无记录
};

/* check_txn_status 的返回。 */
struct CheckTxnStatusResult {
  TxnStatus status{TxnStatus::kRolledBack};  // 事务状态
  uint64_t commit_ts{0};                     // 已提交时的 commit_ts
  uint64_t lock_ttl_ms{0};                   // 锁上记录的租约
  bool lock_expired{false};                  // 锁是否已过墙钟租约
};

/**
 * 拼 lock 物理 key：user + '\0' + 'L'。
 * @param user 用户键
 * @return 引擎 key
 */
std::string lock_key(std::string_view user);
/**
 * 拼 data 物理 key：user + '\0' + 'D' + start_ts。
 * @param user 用户键
 * @param start_ts 事务 start_ts
 * @return 引擎 key
 */
std::string data_key(std::string_view user, uint64_t start_ts);
/**
 * 拼 write 物理 key：user + '\0' + 'W' + ~commit_ts（倒序便于扫最新）。
 * @param user 用户键
 * @param commit_ts 提交时间戳
 * @return 引擎 key
 */
std::string write_key(std::string_view user, uint64_t commit_ts);
/**
 * write 列前缀：user + '\0' + 'W'（扫某 user 全部写记录）。
 * @param user 用户键
 * @return 前缀
 */
std::string write_key_prefix(std::string_view user);

/**
 * 编码 LockInfo 为引擎 payload。
 * @param li 锁信息
 * @return 字节串
 */
std::string encode_lock(const LockInfo& li);
/**
 * 解码锁 payload。
 * @param payload 引擎 value
 * @param out 输出
 * @return 是否成功
 */
bool decode_lock(std::string_view payload, LockInfo* out);

/**
 * 编码 WriteInfo 为引擎 payload。
 * @param wi 写信息
 * @return 字节串
 */
std::string encode_write(const WriteInfo& wi);
/**
 * 解码写列 payload。
 * @param payload 引擎 value
 * @param out 输出
 * @return 是否成功
 */
bool decode_write(std::string_view payload, WriteInfo* out);

/**
 * 从物理 key 还原用户键（截到第一个 '\0'）。
 * @param physical 引擎 key
 * @return 用户键
 */
std::string user_key_of(std::string_view physical);
/**
 * 当前墙钟毫秒。
 * @return unix ms
 */
int64_t wall_ms_now();

/* 在 KvEngine 上做 Percolator 读写：lock/data/write 三列。 */
class MvccStore {
 public:
  /**
   * 绑定底层引擎；不拥有生命周期。
   * @param eng 分片引擎
   */
  explicit MvccStore(KvEngine& eng) : eng_(eng) {}

  /* 可见扫描的一条用户 KV。 */
  struct ScanPair {
    std::string key;       // 用户键
    std::string value;     // 值；keys_only 时为空
    uint64_t revision{0};  // 可见 commit_ts
  };
  /* txn_scan 分页结果。 */
  struct ScanResult {
    std::vector<ScanPair> pairs;  // 本页命中
    bool truncated{false};        // 是否还有下一页
    std::string next_key;         // truncated 时下一页 start_after（用户键）
  };

  /**
   * 读某 user 的锁列。
   * @param user 用户键
   * @return 有锁则返回 LockInfo
   */
  std::optional<LockInfo> get_lock(std::string_view user) const;
  /**
   * 快照读：找 start_ts 可见的最新已提交值。
   * @param user 用户键
   * @param start_ts 读时间戳
   * @param locked 输出：是否被未过期锁挡住
   * @param blocking_lock 输出挡住的锁（可空）
   * @param out_commit_ts 输出可见版本的 commit_ts（可空）
   * @return 可见值；无则 nullopt
   */
  std::optional<std::string> txn_get(std::string_view user, uint64_t start_ts, bool* locked,
                                     LockInfo* blocking_lock = nullptr,
                                     uint64_t* out_commit_ts = nullptr) const;
  /**
   * 读用户 KV 剩余 TTL（相对 ms）；被锁挡住时 locked=true。
   * @param user 用户键
   * @param start_ts 读时间戳
   * @param locked 输出是否被锁挡
   * @param blocking_lock 输出挡住的锁（可空）
   * @return 剩余 ttl；不存在/过期为 nullopt
   */
  std::optional<int64_t> txn_ttl(std::string_view user, uint64_t start_ts, bool* locked,
                                 LockInfo* blocking_lock = nullptr) const;
  /**
   * 用户键范围扫；可见性同 txn_get。
   * @param start_key 起点（含）
   * @param end_key 终点（不含）；空=不限
   * @param start_after 跳过 <= 该键的条目（分页）
   * @param start_ts 读时间戳
   * @param limit 最多条数
   * @param keys_only 不填 value
   * @param region_range 本 Region 区间过滤；默认全空间
   * @return 本页结果
   */
  ScanResult txn_scan(std::string_view start_key, std::string_view end_key,
                      std::string_view start_after, uint64_t start_ts, uint32_t limit,
                      bool keys_only, const openkv::KeyRange& region_range = {}) const;
  /**
   * 某 user 最新已提交写的 commit_ts。
   * @param user 用户键
   * @return commit_ts；无写则 nullopt
   */
  std::optional<uint64_t> latest_commit_ts(std::string_view user) const;
  /**
   * 是否存在 commit_ts > start_ts 的已提交写（prewrite 写冲突）。
   * @param user 用户键
   * @param start_ts 本事务 start_ts
   * @return true 表示有冲突
   */
  bool has_write_conflict(std::string_view user, uint64_t start_ts) const;
  /**
   * 查找 start_ts 对应的 commit_ts（若已提交）。
   * @param user 用户键
   * @param start_ts 事务 start_ts
   * @return commit_ts；未提交则 nullopt
   */
  std::optional<uint64_t> find_commit_ts(std::string_view user, uint64_t start_ts) const;
  /**
   * 查 primary 事务状态（锁 / 已提交 / 已回滚）。
   * @param primary primary 用户键
   * @param lock_ts 期望的锁 start_ts
   * @return 状态结果
   */
  CheckTxnStatusResult check_txn_status(std::string_view primary, uint64_t lock_ts) const;

  /**
   * 写/覆盖锁列。
   * @param user 用户键
   * @param li 锁内容
   * @param rev 引擎 revision
   */
  void put_lock(std::string_view user, const LockInfo& li, uint64_t rev);
  /**
   * 写 data 列（prewrite / ONEPC）。
   * @param user 用户键
   * @param start_ts 事务 start_ts
   * @param value 暂存值
   * @param rev 引擎 revision
   * @param expire_at_ms 用户值绝对过期；0=永久
   */
  void put_data(std::string_view user, uint64_t start_ts, std::string_view value, uint64_t rev,
                int64_t expire_at_ms = 0);
  /**
   * 写 write 列（commit）。
   * @param user 用户键
   * @param commit_ts 提交时间戳
   * @param wi 写信息
   * @param rev 引擎 revision
   */
  void put_write(std::string_view user, uint64_t commit_ts, const WriteInfo& wi, uint64_t rev);
  /**
   * ONEPC：data+write 一次 WriteBatch（可无 data，仅写 delete tombstone）。
   * @param user 用户键
   * @param start_ts 事务 start_ts
   * @param commit_ts 提交时间戳
   * @param op Put/Delete
   * @param value Put 时的值
   * @param rev 引擎 revision
   * @param expire_at_ms 用户值绝对过期；0=永久
   * @return 批写是否成功
   */
  bool put_onepc(std::string_view user, uint64_t start_ts, uint64_t commit_ts, MvccOp op,
                 std::string_view value, uint64_t rev, int64_t expire_at_ms = 0);
  /**
   * 删锁列。
   * @param user 用户键
   * @param rev 引擎 revision
   */
  void clear_lock(std::string_view user, uint64_t rev);
  /**
   * 删 data 列。
   * @param user 用户键
   * @param start_ts 事务 start_ts
   * @param rev 引擎 revision
   */
  void clear_data(std::string_view user, uint64_t start_ts, uint64_t rev);
  /**
   * 删 write 列。
   * @param user 用户键
   * @param commit_ts 提交时间戳
   * @param rev 引擎 revision
   */
  void clear_write(std::string_view user, uint64_t commit_ts, uint64_t rev);

  /**
   * 收集过期悲观/乐观锁的 user key（按 ttl + lock_wall_ms）。
   * @return 过期锁对应的用户键列表
   */
  std::vector<std::string> list_expired_locks() const;
  /**
   * T2：本引擎所有锁的最小 start_ts；无锁返回 UINT64_MAX。
   * @return 最小 start_ts 或 UINT64_MAX
   */
  uint64_t min_lock_start_ts() const;
  /**
   * 删除 commit_ts < safe_ts 的旧 write/data，每个 user 至少保留最新一版。
   * @param safe_ts GC 安全点
   * @return 删除条数
   */
  size_t gc_below(uint64_t safe_ts);

 private:
  KvEngine& eng_;  // 底层引擎（不拥有）
};

}  // namespace openkv::storage
