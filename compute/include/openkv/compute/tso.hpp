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

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "openkv/compute/ts_range_queue.hpp"

namespace openkv::compute {

inline constexpr uint32_t kDefaultTsoBatch = 5000;         // 默认一次向 Meta 要的个数
inline constexpr uint32_t kDefaultTsoLowWatermark = 2000;  // 低于此触发预取

/* 无锁区间队列 TSO；miss 路径用 mutex 护 waiter 列表。 */
class TSOracle {
 public:
  /**
   * @param meta_addrs Meta 地址列表（可 failover）
   * @param batch_size 每次补货个数；0=默认
   * @param low_watermark 低水位；0=默认
   */
  explicit TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size = 0,
                    uint32_t low_watermark = 0);

  /**
   * 同步取 n 个连续 ts；硬 miss 会阻塞等到补货。
   * @param n 需要的个数
   * @return 区间起点（含）
   */
  uint64_t alloc(uint32_t n = 1);
  /**
   * 异步取 n 个；命中则当场 cb，否则入 waiters 等补货。
   * @param n 需要的个数
   * @param cb (status, start_ts)
   */
  void alloc_async(uint32_t n, std::function<void(grpc::Status, uint64_t start)> cb);

  /**
   * 每次向 Meta 申请的批大小。
   */
  uint32_t batch_size() const { return batch_size_; }
  /**
   * 触发预取的剩余水位。
   */
  uint32_t low_watermark() const { return low_watermark_; }

 private:
  /* 硬 miss 时挂起的异步请求。 */
  struct Waiter {
    uint32_t n{1};                                   // 需要的个数
    std::function<void(grpc::Status, uint64_t)> cb;  // 完成回调
  };

  /**
   * 低于水位则 CAS 武装预取。
   * @param out_req 若需补货，写出本次向 Meta 申请的个数
   * @return 是否需要发起补货
   */
  bool arm_prefetch(uint32_t* out_req);

  /**
   * 把 Meta 返回的 [start, start+got) 推进队列，并唤醒 waiters。
   * @param start 区间起点
   * @param got 个数
   */
  void push_batch(uint64_t start, uint32_t got);
  /**
   * 异步向 Meta 要一批 ts。
   * @param req 申请个数
   */
  void start_refill_async(uint32_t req);

  /**
   * 硬 miss：入 waiters；若尚未 in-flight 则武装补货。已现场命中则直接 cb 并返回 false。
   * @param n 数量
   * @param cb 完成回调
   * @param out_req 若需补货，写出本次向 Meta 申请的个数
   * @return true=已入队/需补货；false=已当场完成
   */
  bool enqueue_waiter_and_arm(uint32_t n, std::function<void(grpc::Status, uint64_t)> cb,
                              uint32_t* out_req);

  std::vector<std::string> meta_addrs_;       // Meta 列表
  uint32_t batch_size_;                       // 补货批大小
  uint32_t low_watermark_;                    // 预取水位
  TsRangeQueue<64> q_;                        // 无锁区间环
  std::atomic<bool> refill_inflight_{false};  // 是否已有补货在途

  std::mutex wait_mu_;           // 保护 waiters_
  std::vector<Waiter> waiters_;  // 硬 miss 排队
};

}  // namespace openkv::compute
