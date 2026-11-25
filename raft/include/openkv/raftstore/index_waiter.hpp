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

// @file index_waiter.hpp
// 按 Raft log index 挂 waiter：propose 后等 apply 唤醒（无业务/指标依赖）。

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openkv::raftstore {

/// propose → apply 等待器：按 index 登记，invoke 后唤醒（零轮询）。
class IndexWaiter {
 public:
  struct WaitOutcome {
    bool timed_out{false};
    std::string error;  // 空=成功；非空=apply 侧带回的错误串（如 CAS）
  };

  using CompleteCb = std::function<void(WaitOutcome)>;

  IndexWaiter();
  ~IndexWaiter();

  IndexWaiter(const IndexWaiter&) = delete;
  IndexWaiter& operator=(const IndexWaiter&) = delete;

  /// 阻塞等 index 被 invoke；tick 可选（等待切片间推进 Ready）。
  WaitOutcome wait(uint64_t index, std::chrono::milliseconds timeout,
                   const std::function<void()>& tick = {});

  /// 异步等：已完成则同步回调；否则由 invoke / 超时线程回调。
  void wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb);

  /// apply 完成：唤醒 waiters；error 空 = 成功。
  void invoke(uint64_t index, std::string error = {});

  uint64_t timeout_total() const { return timeout_total_.load(std::memory_order_relaxed); }

 private:
  struct Slot {
    std::condition_variable cv;
    bool done{false};
    std::string error;
    int waiters{0};
    uint64_t gen{0};
    std::vector<CompleteCb> async_cbs;
  };

  struct TimeoutItem {
    uint64_t index{0};
    uint64_t gen{0};
    std::chrono::steady_clock::time_point deadline;
  };

  mutable std::mutex mu_;
  std::unordered_map<uint64_t, Slot> slots_;
  std::atomic<uint64_t> timeout_total_{0};

  std::vector<TimeoutItem> timeouts_;
  std::condition_variable timeout_cv_;
  std::atomic<bool> stop_{false};
  std::thread timeout_thread_;
  bool timeout_thread_started_{false};

  static bool slot_idle(const Slot& slot) {
    return slot.done && slot.waiters == 0 && slot.async_cbs.empty();
  }

  void prune_locked(uint64_t newest_index);
  void ensure_timeout_thread_locked();
  void timeout_loop();
  std::vector<CompleteCb> take_async_cbs_locked(Slot& slot);
};

}  // namespace openkv::raftstore
