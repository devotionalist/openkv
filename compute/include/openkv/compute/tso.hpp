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
// Gateway 本地 TSO：无锁区间环发放 + 低水位异步补货。
//
// 热路径 try_take（CAS，无 mutex）；硬 miss 才锁 waiters_ 排队。

#pragma once

#include "openkv/compute/ts_range_queue.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace openkv::compute {

inline constexpr uint32_t kDefaultTsoBatch = 5000;
inline constexpr uint32_t kDefaultTsoLowWatermark = 2000;

/// 无锁区间队列 TSO；miss 路径用 mutex 护 waiter 列表。
class TSOracle {
 public:
  explicit TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size = 0,
                    uint32_t low_watermark = 0);

  uint64_t alloc(uint32_t n = 1);
  void alloc_async(uint32_t n, std::function<void(grpc::Status, uint64_t start)> cb);

  void inject_cache_for_test(uint64_t next, uint64_t end);
  void inject_range_for_test(uint64_t next, uint64_t end);

  uint32_t batch_size() const { return batch_size_; }
  uint32_t low_watermark() const { return low_watermark_; }
  uint64_t remaining_for_test() const { return q_.remaining(); }

 private:
  struct Waiter {
    uint32_t n{1};
    std::function<void(grpc::Status, uint64_t)> cb;
  };

  /// 低于水位则 CAS 武装预取。
  bool arm_prefetch(uint32_t* out_req);

  void push_batch(uint64_t start, uint32_t got);
  void start_refill_async(uint32_t req);

  /// 硬 miss：入 waiters；若尚未 in-flight 则武装补货。已现场命中则直接 cb 并返回 false。
  bool enqueue_waiter_and_arm(uint32_t n, std::function<void(grpc::Status, uint64_t)> cb,
                              uint32_t* out_req);

  std::vector<std::string> meta_addrs_;
  uint32_t batch_size_;
  uint32_t low_watermark_;
  TsRangeQueue<64> q_;
  std::atomic<bool> refill_inflight_{false};

  std::mutex wait_mu_;
  std::vector<Waiter> waiters_;
};

}  // namespace openkv::compute
