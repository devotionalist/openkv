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
// TSO 批缓存实现。

#include "openkv/compute/tso.hpp"

#include "openkv/compute/exec_line.hpp"
#include "openkv/compute/meta_client.hpp"

#include <cstdlib>
#include <stdexcept>
#include <utility>

namespace openkv::compute {

TSOracle::TSOracle(std::vector<std::string> meta_addrs, uint32_t batch_size)
    : meta_addrs_(std::move(meta_addrs)), batch_size_(batch_size) {
  if (batch_size_ == 0) {
    batch_size_ = kDefaultTsoBatch;
    if (const char* e = std::getenv("OPENKV_TSO_BATCH")) {
      char* end = nullptr;
      unsigned long n = std::strtoul(e, &end, 10);
      if (end != e) batch_size_ = static_cast<uint32_t>(n);
    }
  }
}

void TSOracle::inject_cache_for_test(uint64_t next, uint64_t end) {
  std::lock_guard<std::mutex> lock(mu_);
  next_ = next;
  end_ = end;
}

uint64_t TSOracle::alloc(uint32_t n) {
  if (n == 0) n = 1;
  std::lock_guard<std::mutex> lock(mu_);
  if (batch_size_ <= 1) {
    return alloc_ts(meta_addrs_, n);
  }
  if (next_ == 0 || next_ + static_cast<uint64_t>(n) > end_) {
    refill_locked(n);
  }
  const uint64_t start = next_;
  next_ += n;
  return start;
}

void TSOracle::refill_locked(uint32_t need) {
  uint32_t req = batch_size_;
  if (need > req) req = need;
  auto [start, got] = alloc_ts_range(meta_addrs_, req);
  if (got == 0) throw std::runtime_error("tso: empty batch");
  next_ = start;
  end_ = start + static_cast<uint64_t>(got);
}

void TSOracle::alloc_async(uint32_t n, std::function<void(grpc::Status, uint64_t start)> cb) {
  if (!cb) return;
  if (n == 0) n = 1;

  uint32_t refill_req = n;
  bool start_refill = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (batch_size_ > 1 && next_ != 0 && next_ + static_cast<uint64_t>(n) <= end_) {
      const uint64_t start = next_;
      next_ += n;
      cb(grpc::Status::OK, start);
      return;
    }
    Waiter w;
    w.n = n;
    w.cb = std::move(cb);
    waiters_.push_back(std::move(w));
    if (refill_inflight_) return;
    refill_inflight_ = true;
    start_refill = true;
    refill_req = batch_size_ <= 1 ? n : batch_size_;
    if (n > refill_req) refill_req = n;
    for (const auto& waiter : waiters_) {
      if (waiter.n > refill_req) refill_req = waiter.n;
    }
  }
  if (!start_refill) return;

  meta_alloc_ts_range_async(
      meta_addrs_, refill_req, 3000,
      [this](grpc::Status st, uint64_t start, uint32_t got) {
        std::vector<Waiter> waiters;
        {
          std::lock_guard<std::mutex> lock(mu_);
          refill_inflight_ = false;
          if (st.ok() && got > 0) {
            next_ = start;
            end_ = start + static_cast<uint64_t>(got);
          }
          waiters.swap(waiters_);
        }
        if (!st.ok() || got == 0) {
          const auto err =
              st.ok() ? grpc::Status(grpc::StatusCode::UNAVAILABLE, "tso: empty batch") : st;
          for (auto& w : waiters) {
            if (w.cb) w.cb(err, 0);
          }
          return;
        }
        for (auto& w : waiters) {
          uint64_t s = 0;
          bool ok = false;
          {
            std::lock_guard<std::mutex> lock(mu_);
            if (next_ != 0 && next_ + static_cast<uint64_t>(w.n) <= end_) {
              s = next_;
              next_ += w.n;
              ok = true;
            }
          }
          if (ok) {
            w.cb(grpc::Status::OK, s);
          } else {
            w.cb(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "tso: batch short"), 0);
          }
        }
      });
}

}  // namespace openkv::compute
