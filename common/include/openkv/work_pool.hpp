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
// 有界工作线程池：卸掉 Callback 接收线程上的阻塞/长等。
// 现状：仅 Gateway Watch（间隔 sleep / write_ev）。
// Meta / Storage 已纯 Callback + apply / 出站 async stub，不再建池。

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

/* 固定 worker 数的任务队列；post 入队，stop 排空后 join。 */
class WorkPool {
 public:
  /**
   * 启动 n 个 worker（n==0 当作 1）。
   * @param n worker 个数
   */
  explicit WorkPool(size_t n) {
    if (n == 0) {
      n = 1;
    }
    workers_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      workers_.emplace_back([this] { loop(); });
    }
  }

  ~WorkPool() { stop(); }

  WorkPool(const WorkPool&) = delete;
  WorkPool& operator=(const WorkPool&) = delete;

  /**
   * 投递任务；已 stop 则丢弃。
   * @param fn 在 worker 线程执行的回调
   */
  void post(std::function<void()> fn) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stop_) {
        return;
      }
      q_.push_back(std::move(fn));
    }
    cv_.notify_one();
  }

  /**
   * 置 stop、唤醒并 join 全部 worker；幂等。
   */
  void stop() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stop_) {
        return;
      }
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      if (t.joinable()) {
        t.join();
      }
    }
    workers_.clear();
  }

  /**
   * 当前 worker 线程数（stop 后为 0）。
   * @return worker 个数
   */
  size_t workers() const { return workers_.size(); }

 private:
  /**
   * worker 主循环：等队列或 stop。
   */
  void loop() {
    for (;;) {
      std::function<void()> fn;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [&] { return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) {
          return;
        }
        fn = std::move(q_.front());
        q_.pop_front();
      }
      if (fn) {
        fn();
      }
    }
  }

  std::mutex mu_;                        // 保护 q_ / stop_
  std::condition_variable cv_;           // 任务到达 / stop
  std::deque<std::function<void()>> q_;  // 待执行任务
  bool stop_{false};                     // 生命周期结束标志
  std::vector<std::thread> workers_;     // worker 线程
};

}  // namespace openkv
