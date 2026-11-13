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

// @file scheduler.hpp
// Scheduler：按 key latch，串行冲突写；latch 可持到 apply。
// 热路径用 lock_async（不占 CQ）；sync lock 仅单测 / 旧 schedule_write。

#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace openkv::storage {

class Latches {
 public:
  /// 占有 keys；阻塞直到全部可用。返回 guard token。仅单测 / 旧路径。
  uint64_t lock(const std::vector<std::string>& keys);
  /// 非阻塞：成功返回 token，冲突返回 0。
  uint64_t try_lock(const std::vector<std::string>& keys);
  /// 可立即占锁则同步回调；否则入队，unlock 时回调（不在持锁线程上 cv.wait）。
  void lock_async(std::vector<std::string> keys, std::function<void(uint64_t token)> cb);
  void unlock(uint64_t token);

 private:
  bool can_hold_locked(const std::vector<std::string>& keys) const;
  uint64_t grant_locked(const std::vector<std::string>& keys);
  void drain_async_locked(std::vector<std::pair<uint64_t, std::function<void(uint64_t)>>>* ready);

  struct SyncWaiter {
    std::vector<std::string> keys;
  };
  struct AsyncWaiter {
    std::vector<std::string> keys;
    std::function<void(uint64_t)> cb;
  };

  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<std::string, uint64_t> held_;  // key -> token
  std::unordered_map<uint64_t, SyncWaiter> sync_waiters_;
  std::deque<AsyncWaiter> async_waiters_;
  uint64_t next_token_{1};
};

class Scheduler {
 public:
  using WriteFn = std::function<bool(uint32_t region_id, std::string data, uint64_t* idx,
                                     std::string* err)>;

  void set_write_fn(WriteFn fn) { write_fn_ = std::move(fn); }

  /// 占 latch → 写 → **立即**放 latch（旧语义；单测冲突串行仍用此）。
  bool schedule_write(uint32_t region_id, const std::string& latch_key, std::string data,
                      uint64_t* assigned_index, std::string* error);

  /// 占 latch → 写；成功时 *latch_token 非 0，调用方须在 apply 后 unlock。
  /// 写失败自动 unlock 并返回 false。
  bool schedule_write_hold(uint32_t region_id, const std::string& latch_key, std::string data,
                           uint64_t* assigned_index, uint64_t* latch_token, std::string* error);

  void unlock(uint64_t latch_token) {
    if (latch_token != 0) latches_.unlock(latch_token);
  }

  /// 直接占（阻塞）；仅单测。
  uint64_t lock_key(const std::string& latch_key) { return latches_.lock({latch_key}); }

  /// 热路径：不阻塞调用线程。
  void lock_key_async(const std::string& latch_key, std::function<void(uint64_t token)> cb) {
    latches_.lock_async({latch_key}, std::move(cb));
  }

 private:
  Latches latches_;
  WriteFn write_fn_;
};

}  // namespace openkv::storage
