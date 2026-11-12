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
// Gateway 活跃 start_ts 跟踪：Put/Txn 占用区间，完成后 remove。
//
// min() 上报 Meta 作 GC 下界；TTL 过期条目自动剔除，避免永久挡 GC。
// 仅统计**本进程**已知 ts，非集群全局。

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
  /// 构造；ttl 内未 remove 的条目在 min()/扫描时剔除。
  explicit ActiveTsTracker(std::chrono::milliseconds ttl = std::chrono::seconds(60));

  /// 登记一个活跃 start_ts（Put/TxnBegin 占用时调用）。
  void add(uint64_t ts);
  /// 事务结束或 Put 完成后注销该 ts。
  void remove(uint64_t ts);

  /// 当前活跃集合中的最小 start_ts；无活跃则为 UINT64_MAX（上报 Meta 表示「无挡」）。
  uint64_t min();

 private:
  std::mutex mu_;
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> m_;
  std::chrono::milliseconds ttl_;
};

}  // namespace openkv::compute
