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
// Gateway 本地 TSO 批缓存（从 Meta GetTs 补货）。
//
// 不变量：对外发放的是半开区间 [next, end)；alloc / alloc_async 原子推进 next。
// CQ 热路径必须用 alloc_async；alloc 会在 miss 时同步打 Meta（仅 WorkPool/测试）。

#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace openkv::compute {

inline constexpr uint32_t kDefaultTsoBatch = 512;

/// 本地批缓存：Alloc 先吃内存，耗尽再 GetTs 补货。
class TSOracle {
 public:
  /// 构造；batch_size==0 读 OPENKV_TSO_BATCH（默认 512）；<=1 关闭缓存每次打 Meta。
  explicit TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size = 0);

  /// 同步分配 n 个连续 ts，返回起始值；miss 时阻塞打 Meta（仅 WorkPool/单测）。
  uint64_t alloc(uint32_t n = 1);

  /// 异步分配：缓存命中立即 cb(OK, start)；miss 则排队等补货后 cb（不堵 CQ）。
  void alloc_async(uint32_t n, std::function<void(grpc::Status, uint64_t start)> cb);

  /// 单测注入已缓存半开区间 [next, end)。
  void inject_cache_for_test(uint64_t next, uint64_t end);

  /// 当前配置的批大小（补货请求 count）。
  uint32_t batch_size() const { return batch_size_; }

 private:
  /// 等待补货完成的异步请求。
  struct Waiter {
    uint32_t n{1};
    std::function<void(grpc::Status, uint64_t)> cb;
  };

  /// 持锁：向 Meta 同步/异步补货，满足 need；唤醒 waiters_。
  void refill_locked(uint32_t need);

  std::vector<std::string> meta_addrs_;
  uint32_t batch_size_;
  mutable std::mutex mu_;
  uint64_t next_{0};
  uint64_t end_{0};
  bool refill_inflight_{false};
  std::vector<Waiter> waiters_;
};

}  // namespace openkv::compute
