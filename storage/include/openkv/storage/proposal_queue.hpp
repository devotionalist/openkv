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

// @file proposal_queue.hpp
// TiKV-style propose → apply 等待：按 log index 挂 waiter，apply 完成后 invoke 唤醒（零轮询）。
// wait_async：不占调用线程；invoke / 超时线程回调。

#pragma once

#include "openkv/storage/latency_hist.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openkv::storage {

/// TiKV-style：按 log index 挂 waiter，apply 后 invoke 唤醒（无 last_applied 轮询）。
class ProposalQueue {
 public:
  struct WaitOutcome {
    bool timed_out{false};
    std::string error;  // 空 = 成功；非空 = apply 业务错误（如 CAS）
  };

  using CompleteCb = std::function<void(WaitOutcome)>;

  ProposalQueue();
  ~ProposalQueue();

  ProposalQueue(const ProposalQueue&) = delete;
  ProposalQueue& operator=(const ProposalQueue&) = delete;

  /// 等 index 被 invoke。tick 可选：等待切片间推进 Ready（RF=1）。
  WaitOutcome wait(uint64_t index, std::chrono::milliseconds timeout,
                   const std::function<void()>& tick = {});

  /// 异步等待：已完成则同步回调；否则 register，由 invoke / 超时线程回调。
  void wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb);

  /// apply 完成：唤醒 waiters；error 空 = 成功。
  void invoke(uint64_t index, std::string error);

  /// 非阻塞取走已完成结果（兼容旧 take_apply_result）。
  std::optional<std::string> take(uint64_t index);

  uint64_t timeout_total() const { return timeout_total_.load(std::memory_order_relaxed); }
  const LatencyHistogram& wait_hist() const { return wait_hist_; }
  /// Prometheus：apply_wait 直方图 + apply_timeout 计数。
  std::string prometheus_text() const;

 private:
  struct Slot {
    std::condition_variable cv;
    bool done{false};
    std::string error;
    int waiters{0};  // sync wait + pending async_cbs
    uint64_t gen{0};  // bump on invoke → invalidate stale timeouts
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
  LatencyHistogram wait_hist_;

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
  /// 取出 async 回调并扣减 waiters；调用方在锁外执行。
  std::vector<CompleteCb> take_async_cbs_locked(Slot& slot);
  void record_wait_us(uint64_t us);
  CompleteCb wrap_latency(std::chrono::steady_clock::time_point t0, CompleteCb cb);
};

}  // namespace openkv::storage
