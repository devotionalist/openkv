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

// @file schedule_queue.hpp
// 有消息的 region 入队；worker 按调度处理，避免每轮扫全表。

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <unordered_set>

namespace openkv::raftstore {

class ScheduleQueue {
 public:
  void push(uint32_t region_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!pending_.insert(region_id).second) return;  // 已在队：去重
    q_.push_back(region_id);
    cv_.notify_one();
  }

  /// 取一个待处理 region；空则最多等 timeout_ms。running=false 时返回 nullopt。
  std::optional<uint32_t> pop(int timeout_ms, const std::atomic<bool>& running) {
    std::unique_lock<std::mutex> lk(mu_);
    if (q_.empty() && timeout_ms > 0 && running.load(std::memory_order_acquire)) {
      cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
        return !q_.empty() || !running.load(std::memory_order_acquire);
      });
    }
    if (q_.empty()) return std::nullopt;
    const uint32_t rid = q_.front();
    q_.pop_front();
    pending_.erase(rid);
    return rid;
  }

  void wake() { cv_.notify_all(); }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<uint32_t> q_;
  std::unordered_set<uint32_t> pending_;
};

}  // namespace openkv::raftstore
