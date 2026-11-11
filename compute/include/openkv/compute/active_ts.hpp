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

// @file active_ts.hpp
// Gateway 活跃 start_ts 跟踪（对齐 Go activeTsTracker）。

#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <unordered_map>

namespace openkv::compute {

/// 本 Gateway 已知活跃 start_ts；过期自动剔除，避免挡住 GC。
class ActiveTsTracker {
 public:
  explicit ActiveTsTracker(std::chrono::milliseconds ttl = std::chrono::seconds(60));

  void add(uint64_t ts);
  void remove(uint64_t ts);

  /// 活跃最小 start_ts；无活跃则为 UINT64_MAX。
  uint64_t min();

 private:
  std::mutex mu_;
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> m_;
  std::chrono::milliseconds ttl_;
};

}  // namespace openkv::compute
