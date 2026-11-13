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

// @file tso.cpp
// TSO：热路径无锁 try_take；miss 锁 waiters + 异步/同步补货 push。

#include "openkv/compute/tso.hpp"

#include "openkv/compute/meta_client.hpp"

#include <cstdlib>
#include <stdexcept>
#include <utility>

namespace openkv::compute {
namespace {

uint32_t env_u32(const char* key, uint32_t fallback) {
  if (const char* e = std::getenv(key)) {
    char* end = nullptr;
    unsigned long n = std::strtoul(e, &end, 10);
    if (end != e) return static_cast<uint32_t>(n);
  }
  return fallback;
}

}  // namespace

TSOracle::TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size, uint32_t low_watermark)
    : meta_addrs_(std::move(meta_addrs)), batch_size_(batch_size), low_watermark_(low_watermark) {
  if (batch_size_ == 0) {
    batch_size_ = env_u32("OPENKV_TSO_BATCH", kDefaultTsoBatch);
  }
  if (low_watermark_ == 0) {
    low_watermark_ = env_u32("OPENKV_TSO_LOW_WATERMARK", kDefaultTsoLowWatermark);
  }
  if (batch_size_ > 1 && low_watermark_ >= batch_size_) {
    low_watermark_ = batch_size_ / 2;
    if (low_watermark_ == 0) low_watermark_ = 1;
  }
}

void TSOracle::inject_cache_for_test(uint64_t next, uint64_t end) {
  q_.reset_for_test();
  refill_inflight_.store(false, std::memory_order_relaxed);
  if (next < end) {
    if (!q_.push(next, static_cast<uint32_t>(end - next))) {
      throw std::runtime_error("tso inject: queue full");
    }
  }
}

void TSOracle::inject_range_for_test(uint64_t next, uint64_t end) {
  if (next >= end) return;
  if (!q_.push(next, static_cast<uint32_t>(end - next))) {
    throw std::runtime_error("tso inject_range: queue full");
  }
}

void TSOracle::push_batch(uint64_t start, uint32_t got) {
  if (got == 0) return;
  (void)q_.push(start, got);
}

bool TSOracle::arm_prefetch(uint32_t* out_req) {
  if (batch_size_ <= 1 || meta_addrs_.empty()) return false;
  if (q_.remaining() >= low_watermark_) return false;
  bool expected = false;
  if (!refill_inflight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return false;
  }
  if (out_req) *out_req = batch_size_;
  return true;
}

bool TSOracle::enqueue_waiter_and_arm(uint32_t n, std::function<void(grpc::Status, uint64_t)> cb,
                                      uint32_t* out_req) {
  std::lock_guard<std::mutex> lock(wait_mu_);
  // 入队前再试一次：补货可能刚好 push 完成。
  uint64_t start = 0;
  if (batch_size_ > 1 && q_.try_take(n, &start)) {
    if (cb) cb(grpc::Status::OK, start);
    return false;
  }

  Waiter w;
  w.n = n;
  w.cb = std::move(cb);
  waiters_.push_back(std::move(w));

  bool expected = false;
  if (!refill_inflight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return false;
  }
  uint32_t req = batch_size_ <= 1 ? n : batch_size_;
  if (n > req) req = n;
  for (const auto& waiter : waiters_) {
    if (waiter.n > req) req = waiter.n;
  }
  if (out_req) *out_req = req;
  return true;
}

void TSOracle::start_refill_async(uint32_t req) {
  meta_alloc_ts_range_async(
      meta_addrs_, req, 3000,
      [this](grpc::Status st, uint64_t start, uint32_t got) {
        if (st.ok() && got > 0) push_batch(start, got);
        refill_inflight_.store(false, std::memory_order_release);

        std::vector<Waiter> waiters;
        {
          std::lock_guard<std::mutex> lock(wait_mu_);
          waiters.swap(waiters_);
        }

        if (!st.ok() || got == 0) {
          if (waiters.empty()) return;
          const auto err =
              st.ok() ? grpc::Status(grpc::StatusCode::UNAVAILABLE, "tso: empty batch") : st;
          for (auto& w : waiters) {
            if (w.cb) w.cb(err, 0);
          }
          return;
        }

        std::vector<Waiter> retry;
        for (auto& w : waiters) {
          uint64_t s = 0;
          if (q_.try_take(w.n, &s)) {
            w.cb(grpc::Status::OK, s);
          } else {
            retry.push_back(std::move(w));
          }
        }
        if (retry.empty()) return;

        uint32_t need = batch_size_;
        {
          std::lock_guard<std::mutex> lock(wait_mu_);
          for (auto& w : retry) {
            if (w.n > need) need = w.n;
            waiters_.push_back(std::move(w));
          }
        }
        bool expected = false;
        if (refill_inflight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
          start_refill_async(need);
        }
      });
}

uint64_t TSOracle::alloc(uint32_t n) {
  if (n == 0) n = 1;
  if (batch_size_ <= 1) {
    return alloc_ts(meta_addrs_, n);
  }

  uint64_t start = 0;
  if (q_.try_take(n, &start)) {
    uint32_t prefetch_req = 0;
    if (arm_prefetch(&prefetch_req)) start_refill_async(prefetch_req);
    return start;
  }

  // 硬 miss：争抢同步补货，或自旋等他人 push。
  bool expected = false;
  if (refill_inflight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    try {
      uint32_t req = batch_size_;
      if (n > req) req = n;
      auto [s, got] = alloc_ts_range(meta_addrs_, req);
      if (got == 0) throw std::runtime_error("tso: empty batch");
      push_batch(s, got);
    } catch (...) {
      refill_inflight_.store(false, std::memory_order_release);
      throw;
    }
    refill_inflight_.store(false, std::memory_order_release);
  } else {
    for (int i = 0; i < 2000; ++i) {
      if (q_.try_take(n, &start)) {
        uint32_t prefetch_req = 0;
        if (arm_prefetch(&prefetch_req)) start_refill_async(prefetch_req);
        return start;
      }
    }
    auto [s, got] = alloc_ts_range(meta_addrs_, n > batch_size_ ? n : batch_size_);
    if (got == 0) throw std::runtime_error("tso: empty batch");
    push_batch(s, got);
  }

  if (!q_.try_take(n, &start)) {
    throw std::runtime_error("tso: batch short after refill");
  }
  uint32_t prefetch_req = 0;
  if (arm_prefetch(&prefetch_req)) start_refill_async(prefetch_req);
  return start;
}

void TSOracle::alloc_async(uint32_t n, std::function<void(grpc::Status, uint64_t start)> cb) {
  if (!cb) return;
  if (n == 0) n = 1;

  if (batch_size_ > 1) {
    uint64_t start = 0;
    if (q_.try_take(n, &start)) {
      cb(grpc::Status::OK, start);
      uint32_t prefetch_req = 0;
      if (arm_prefetch(&prefetch_req)) start_refill_async(prefetch_req);
      return;
    }
  }

  uint32_t refill_req = n;
  if (enqueue_waiter_and_arm(n, std::move(cb), &refill_req)) {
    start_refill_async(refill_req);
  } else {
    // 入队前已命中，或他人正在补货：仍尝试低水位预取。
    uint32_t prefetch_req = 0;
    if (arm_prefetch(&prefetch_req)) start_refill_async(prefetch_req);
  }
}

}  // namespace openkv::compute
