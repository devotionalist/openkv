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
// propose → apply 等待：按 log index 挂 waiter，apply 完成后 invoke 唤醒（零轮询）。
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

// 按 log index 挂 waiter，apply 后 invoke 唤醒（无 last_applied 轮询）。
class ProposalQueue {
 public:
  struct WaitOutcome {// 等待结果
    bool timed_out{false}; // 是否超时
    std::string error;  // 空 = 成功；非空 = apply 业务错误（如 CAS）
  };

  using CompleteCb = std::function<void(WaitOutcome)>; // 完成回调

  ProposalQueue(); // 提议队列
  ~ProposalQueue(); // 销毁

  ProposalQueue(const ProposalQueue&) = delete; // 禁止拷贝构造
  ProposalQueue& operator=(const ProposalQueue&) = delete; // 禁止拷贝赋值  

  // 等 index 被 invoke。tick 可选：等待切片间推进 Ready（RF=1）。
  WaitOutcome wait(uint64_t index, std::chrono::milliseconds timeout, const std::function<void()>& tick = {});

  // 异步等待：已完成则同步回调；否则 register，由 invoke / 超时线程回调。
  void wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb);

  // apply 完成：唤醒 waiters；error 空 = 成功。
  void invoke(uint64_t index, std::string error);

  // 超时总数
  uint64_t timeout_total() const { return timeout_total_.load(std::memory_order_relaxed); }
  // 等待直方图
  const LatencyHistogram& wait_hist() const { return wait_hist_; }
  // Prometheus：apply_wait 直方图 + apply_timeout 计数。
  std::string prometheus_text() const;

 private:
  struct Slot {// 槽
    std::condition_variable cv;
    bool done{false}; // 是否完成
    std::string error; // 错误
    int waiters{0};  // sync wait + pending async_cbs
    uint64_t gen{0};  // bump on invoke → invalidate stale timeouts
    std::vector<CompleteCb> async_cbs;
  };

  struct TimeoutItem {// 超时项
    uint64_t index{0}; // 索引
    uint64_t gen{0}; // 生成
    std::chrono::steady_clock::time_point deadline; // 截止时间
  };

  mutable std::mutex mu_; // 互斥锁
  std::unordered_map<uint64_t, Slot> slots_; // 槽
  std::atomic<uint64_t> timeout_total_{0}; // 超时总数
  LatencyHistogram wait_hist_; // 等待直方图

  std::vector<TimeoutItem> timeouts_; // 超时项
  std::condition_variable timeout_cv_; // 超时条件变量
  std::atomic<bool> stop_{false}; // 停止
  std::thread timeout_thread_; // 超时线程
  bool timeout_thread_started_{false}; // 超时线程是否启动

  static bool slot_idle(const Slot& slot) { // 槽是否空闲
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
