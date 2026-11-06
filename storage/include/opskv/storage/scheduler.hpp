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
// Scheduler：按 key latch，串行冲突写。

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace opskv::storage {

class Latches {
 public:
  /// 占有 keys；阻塞直到全部可用。返回 guard token。
  uint64_t lock(const std::vector<std::string>& keys);
  void unlock(uint64_t token);

 private:
  struct Waiter {
    std::vector<std::string> keys;
    bool ready{false};
  };
  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<std::string, uint64_t> held_;  // key -> token
  std::unordered_map<uint64_t, Waiter> waiters_;
  uint64_t next_token_{1};
};

class Scheduler {
 public:
  using WriteFn = std::function<bool(uint32_t region_id, std::string data, uint64_t* idx,
                                     std::string* err)>;

  void set_write_fn(WriteFn fn) { write_fn_ = std::move(fn); }

  /// 对 command key 占 latch 后写。
  bool schedule_write(uint32_t region_id, const std::string& latch_key, std::string data,
                      uint64_t* assigned_index, std::string* error);

 private:
  Latches latches_;
  WriteFn write_fn_;
};

}  // namespace opskv::storage
