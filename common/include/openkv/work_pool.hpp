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

// @file work_pool.hpp
// 通用有界工作线程池：gRPC 接收线程只投递，业务在 worker 上跑（或继续挂异步 RPC）。

#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace openkv {

class WorkPool {
 public:
  explicit WorkPool(size_t n) {
    if (n == 0) n = 1;
    workers_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      workers_.emplace_back([this] { loop(); });
    }
  }

  ~WorkPool() { stop(); }

  WorkPool(const WorkPool&) = delete;
  WorkPool& operator=(const WorkPool&) = delete;

  void post(std::function<void()> fn) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stop_) return;
      q_.push_back(std::move(fn));
    }
    cv_.notify_one();
  }

  void stop() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stop_) return;
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      if (t.joinable()) t.join();
    }
    workers_.clear();
  }

  size_t workers() const { return workers_.size(); }

 private:
  void loop() {
    for (;;) {
      std::function<void()> fn;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [&] { return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) return;
        fn = std::move(q_.front());
        q_.pop_front();
      }
      if (fn) fn();
    }
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> q_;
  bool stop_{false};
  std::vector<std::thread> workers_;
};

}  // namespace openkv
