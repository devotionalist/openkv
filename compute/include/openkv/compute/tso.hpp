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

// @file tso.hpp
// Gateway 本地 TSO 批缓存（对齐 Go route.TSOracle）。

#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace openkv::compute {

inline constexpr uint32_t kDefaultTsoBatch = 512;

/// PD 风格本地批缓存：Alloc 先吃内存，耗尽再 GET_TS 补货。
class TSOracle {
 public:
  /// batch_size==0：读 OPENKV_TSO_BATCH（默认 512）；<=1 关闭缓存，每次打 Meta。
  explicit TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size = 0);

  /// 返回 n 个连续时间戳的起点 [start, start+n)；n==0 视为 1。
  uint64_t alloc(uint32_t n = 1);

  /// 单测注入已缓存区间 [next, end)。
  void inject_cache_for_test(uint64_t next, uint64_t end);

  uint32_t batch_size() const { return batch_size_; }

 private:
  void refill_locked(uint32_t need);

  std::vector<std::string> meta_addrs_;
  uint32_t batch_size_;
  mutable std::mutex mu_;
  uint64_t next_{0};
  uint64_t end_{0};
};

}  // namespace openkv::compute
