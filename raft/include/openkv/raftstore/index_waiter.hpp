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

/* propose → apply 等待器：按 index 登记，invoke 后唤醒（零轮询）。 */
class IndexWaiter {
 public:
  /* 一次等待的结果：超时或 apply 带回的错误串。 */
  struct WaitOutcome {
    bool timed_out{false};  // true=超时未等到 invoke
    std::string error;      // 空=成功；非空=apply 侧带回的错误串（如 CAS）
  };

  using CompleteCb = std::function<void(WaitOutcome)>;

  /**
   * 构造；超时线程懒启动。
   */
  IndexWaiter();
  ~IndexWaiter();

  IndexWaiter(const IndexWaiter&) = delete;
  IndexWaiter& operator=(const IndexWaiter&) = delete;

  /**
   * 阻塞等 index 被 invoke；tick 可选（等待切片间推进 Ready）。
   * @param index 要等到的日志下标
   * @param timeout 最长等待
   * @param tick 每次切片醒来可调用的推进回调；默认空
   * @return 超时 / 错误 / 成功结果
   */
  WaitOutcome wait(uint64_t index, std::chrono::milliseconds timeout,
                   const std::function<void()>& tick = {});

  /**
   * 异步等：已完成则同步回调；否则由 invoke / 超时线程回调。
   * @param index 日志下标
   * @param timeout 超时
   * @param cb 完成回调（可能在超时线程或 invoke 线程）
   */
  void wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb);

  /**
   * apply 完成：唤醒 waiters；error 空 = 成功。
   * @param index 已 apply 的日志下标
   * @param error 业务错误串；默认空表示成功
   */
  void invoke(uint64_t index, std::string error = {});

  /**
   * 累计超时次数（观测）。
   * @return 超时总次数
   */
  uint64_t timeout_total() const { return timeout_total_.load(std::memory_order_relaxed); }

 private:
  /* 单个 index 上的同步/异步等待槽。 */
  struct Slot {
    std::condition_variable cv;         // 同步 wait 用
    bool done{false};                   // 是否已 invoke
    std::string error;                  // invoke 带回的错误
    int waiters{0};                     // 同步等待者数
    uint64_t gen{0};                    // 代数，防超时误伤新 waiter
    std::vector<CompleteCb> async_cbs;  // 异步回调
  };

  /* 超时线程扫描项。 */
  struct TimeoutItem {
    uint64_t index{0};                               // 等待的 index
    uint64_t gen{0};                                 // 登记时的 gen
    std::chrono::steady_clock::time_point deadline;  // 到期时刻
  };

  mutable std::mutex mu_;                     // 保护 slots_ / timeouts_
  std::unordered_map<uint64_t, Slot> slots_;  // index → 等待槽
  std::atomic<uint64_t> timeout_total_{0};    // 超时累计

  std::vector<TimeoutItem> timeouts_;   // 待扫描超时项
  std::condition_variable timeout_cv_;  // 超时线程唤醒
  std::atomic<bool> stop_{false};       // 停机
  std::thread timeout_thread_;          // 超时扫描线程
  bool timeout_thread_started_{false};  // 是否已懒启动

  /**
   * 槽是否可回收（已完成且无人等）。
   * @param slot 待检查槽
   * @return true 表示可 erase
   */
  static bool slot_idle(const Slot& slot) {
    return slot.done && slot.waiters == 0 && slot.async_cbs.empty();
  }

  /**
   * 丢掉远小于 newest_index 的空闲槽，防泄漏。
   * @param newest_index 最近完成的 index
   */
  void prune_locked(uint64_t newest_index);
  /**
   * 确保超时扫描线程已启动（持锁调用）。
   */
  void ensure_timeout_thread_locked();
  /**
   * 超时扫描主循环。
   */
  void timeout_loop();
  /**
   * 取出并清空异步回调列表。
   * @param slot 目标槽
   * @return 待调用的回调列表
   */
  std::vector<CompleteCb> take_async_cbs_locked(Slot& slot);
};

}  // namespace openkv::raftstore
