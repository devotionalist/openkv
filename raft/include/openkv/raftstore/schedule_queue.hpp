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
// 职责：待处理 region_id 队列（去重）；worker pop 时等待，避免每轮扫描全部 Peer。
// 架构：仅头文件；每个 BatchSystem worker 各有一条 ScheduleQueue。
// 协作：Router NotifyFn / Mailbox 投递成功 → push(region_id)。

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

/* 待处理 region 唤醒队列：push 去重，pop 可超时等待。 */
class ScheduleQueue {
 public:
  /**
   * 把 region 排进本 worker；已在队则忽略。
   * @param region_id 待处理 Region
   */
  void push(uint32_t region_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!pending_.insert(region_id).second) {
      return;
    }
    q_.push_back(region_id);
    cv_.notify_one();
  }

  /**
   * 取一个待处理 region；空则最多等 timeout_ms。running=false 时返回 nullopt。
   * @param timeout_ms 空队列等待毫秒
   * @param running 外部生命周期标志；false 则立刻醒并不再等
   * @return region_id 或 nullopt
   */
  std::optional<uint32_t> pop(int timeout_ms, const std::atomic<bool>& running) {
    std::unique_lock<std::mutex> lk(mu_);
    if (q_.empty() && timeout_ms > 0 && running.load(std::memory_order_acquire)) {
      cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                   [&] { return !q_.empty() || !running.load(std::memory_order_acquire); });
    }
    if (q_.empty()) {
      return std::nullopt;
    }
    const uint32_t rid = q_.front();
    q_.pop_front();
    pending_.erase(rid);
    return rid;
  }

  /**
   * 唤醒所有阻塞在 pop 上的 worker（停机用）。
   */
  void wake() { cv_.notify_all(); }

  /**
   * 当前排队 region 数。
   * @return 队列长度
   */
  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }

 private:
  mutable std::mutex mu_;                 // 保护队列
  std::condition_variable cv_;            // 有活 / 停机
  std::deque<uint32_t> q_;                // FIFO region 列表
  std::unordered_set<uint32_t> pending_;  // 去重集合
};

}  // namespace openkv::raftstore
