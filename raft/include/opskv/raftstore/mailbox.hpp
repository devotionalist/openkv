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

// @file mailbox.hpp
// 每 Region 一个邮箱；BatchSystem worker 批量 poll。

#pragma once

#include "opskv/raftstore/msg.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace opskv::raftstore {

template <typename T>
class Mailbox {
 public:
  explicit Mailbox(size_t capacity = 4096) : capacity_(capacity == 0 ? 4096 : capacity) {}

  bool try_send(T msg) {
    std::lock_guard<std::mutex> lk(mu_);
    if (closed_ || q_.size() >= capacity_) {
      ++drop_count_;
      return false;
    }
    q_.push_back(std::move(msg));
    cv_.notify_one();
    return true;
  }

  /// 阻塞取最多 max_n 条；空且 timeout_ms>0 则等待。
  std::vector<T> recv_batch(size_t max_n, int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    if (q_.empty() && timeout_ms > 0 && !closed_) {
      cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                   [&] { return !q_.empty() || closed_; });
    }
    std::vector<T> out;
    out.reserve(std::min(max_n, q_.size()));
    while (!q_.empty() && out.size() < max_n) {
      out.push_back(std::move(q_.front()));
      q_.pop_front();
    }
    return out;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }
  uint64_t drop_count() const { return drop_count_; }
  void close() {
    std::lock_guard<std::mutex> lk(mu_);
    closed_ = true;
    cv_.notify_all();
  }
  bool closed() const {
    std::lock_guard<std::mutex> lk(mu_);
    return closed_;
  }

 private:
  size_t capacity_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> q_;
  bool closed_{false};
  uint64_t drop_count_{0};
};

}  // namespace opskv::raftstore
