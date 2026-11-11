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

// @file store_client.hpp
// Gateway → Storage Admin ExecLine（对齐 Go storagetcp.Client）。

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openkv::compute {

class NotLeaderError : public std::runtime_error {
 public:
  explicit NotLeaderError(std::string redirect, std::string raw)
      : std::runtime_error(redirect.empty() ? raw : ("not_leader redirect=" + redirect)),
        redirect_(std::move(redirect)),
        raw_(std::move(raw)) {}

  const std::string& redirect() const { return redirect_; }
  const std::string& raw() const { return raw_; }

 private:
  std::string redirect_;
  std::string raw_;
};

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

/// 从 Storage `ERR locked primary=.. lock_ts=..` 解析；供 Put 重试路径使用。
LockedError parse_locked_error(const std::string& resp);

struct ScanPair {
  std::string key;
  std::string value;
  uint64_t revision{0};
};

struct TxnScanResult {
  std::vector<ScanPair> pairs;
  bool truncated{false};
  std::string next_key;
};

struct WatchEvent {
  std::string key;
  std::string value;
  uint64_t revision{0};
  bool deleted{false};
};

struct TxnGetResult {
  std::string value;
  bool found{false};
  uint64_t revision{0};
};

class StoreClient {
 public:
  explicit StoreClient(std::string addr, int timeout_ms = 15000);

  const std::string& addr() const { return addr_; }
  void set_timeout_ms(int ms) { timeout_ms_ = ms; }

  void ping();
  std::string call_raw(const std::string& line);

  TxnGetResult txn_get_opt(uint32_t shard, std::string_view key, uint64_t start_ts,
                           bool linearizable = true);
  int64_t txn_ttl_opt(uint32_t shard, std::string_view key, uint64_t start_ts,
                      bool linearizable = true);

  uint64_t prewrite_cas(uint32_t shard, std::string_view key, uint64_t start_ts,
                        std::string_view primary, uint8_t op, std::string_view value,
                        uint64_t epoch, bool cas, uint64_t expected_rev, int64_t expire_at_ms = 0);

  /// 单 key 1PC；返回 commit_ts（与 OK ONEPC revision 一致）。
  uint64_t one_pc(uint32_t shard, std::string_view key, uint64_t start_ts, uint64_t commit_ts,
                  std::string_view primary, uint8_t op, std::string_view value, uint64_t epoch,
                  bool cas = false, uint64_t expected_rev = 0, int64_t expire_at_ms = 0);

  uint64_t commit_key(uint32_t shard, std::string_view key, uint64_t start_ts, uint64_t commit_ts,
                      uint64_t epoch = 0);
  uint64_t rollback_key(uint32_t shard, std::string_view key, uint64_t start_ts,
                        uint64_t epoch = 0);

  uint64_t pessimistic_lock(uint32_t shard, std::string_view key, uint64_t start_ts,
                            uint64_t for_update_ts, std::string_view primary, uint64_t ttl_ms,
                            uint64_t epoch = 0);
  uint64_t pessimistic_rollback(uint32_t shard, std::string_view key, uint64_t start_ts,
                                uint64_t epoch = 0);

  uint64_t resolve_lock(uint32_t shard, std::string_view key, uint64_t start_ts, uint64_t commit_ts,
                        uint64_t epoch = 0);

  /// @return {status, commit_ts}
  std::pair<std::string, uint64_t> check_txn_status(uint32_t shard, std::string_view primary,
                                                    uint64_t lock_ts);

  TxnScanResult txn_scan_opt(uint32_t shard, std::string_view start, std::string_view end,
                             std::string_view start_after, uint64_t start_ts, uint32_t limit,
                             bool keys_only, bool linearizable);

  uint64_t watch_head(uint32_t shard);
  /// @return {events, next_since}
  std::pair<std::vector<WatchEvent>, uint64_t> watch_poll(uint32_t shard, uint64_t since,
                                                         int limit = 64);

 private:
  std::string call(const std::string& line);
  [[noreturn]] void throw_txn_err(const std::string& resp, const char* op);

  std::string addr_;
  int timeout_ms_;
};

}  // namespace openkv::compute
