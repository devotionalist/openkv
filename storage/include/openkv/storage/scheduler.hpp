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
// Scheduler / Latches：按 key 串行冲突写；唯一占锁入口为 lock_async（不 cv.wait）。
// Propose CQ 与 Admin 同步路径均用 lock_key_async；后者可在调用线程上 future.wait。

#pragma once

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
  /// 可立即占锁则同步回调；否则入队，unlock 时回调（调用方线程不阻塞）。
  void lock_async(std::vector<std::string> keys, std::function<void(uint64_t token)> cb);
  // 释放锁
  void unlock(uint64_t token);

 private:

  // 是否可以持有锁
  bool can_hold_locked(const std::vector<std::string>& keys) const;
  // 授予锁
  uint64_t grant_locked(const std::vector<std::string>& keys);
  // 排空异步等待者
  void drain_async_locked(std::vector<std::pair<uint64_t, std::function<void(uint64_t)>>>* ready);

  struct AsyncWaiter {
    std::vector<std::string> keys;
    std::function<void(uint64_t)> cb;
  };

  std::mutex mu_;
  std::unordered_map<std::string, uint64_t> held_;  // key -> token
  std::deque<AsyncWaiter> async_waiters_;
  uint64_t next_token_{1};
};

class Scheduler {
 public:

  // 释放锁  
  void unlock(uint64_t latch_token) {
    if (latch_token != 0) latches_.unlock(latch_token);
  }

  // 锁定键
  void lock_key_async(const std::string& latch_key, std::function<void(uint64_t token)> cb) {
    latches_.lock_async({latch_key}, std::move(cb));
  }

 private:
  Latches latches_;
};

}  // namespace openkv::storage
