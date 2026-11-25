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

// @file scheduler.hpp
// Scheduler / Latches：按 key 串行冲突写；唯一占锁入口为 lock_async（不 cv.wait）。
// Propose CQ 与 Admin 同步路径均用 lock_key_async；后者可在调用线程上 future.wait。

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace openkv::storage {

/* 多 key 互斥锁：冲突写串行；异步排队，不在调用方 cv.wait。 */
class Latches {
 public:
  /**
   * 可立即占锁则同步回调；否则入队，unlock 时回调（调用方线程不阻塞）。
   * @param keys 要占的一组 key（任一对冲突则互斥）
   * @param cb 拿到 token 后的回调；token 用于 unlock
   */
  void lock_async(std::vector<std::string> keys, std::function<void(uint64_t token)> cb);
  /**
   * 释放 token 对应的一组锁，并唤醒可前进的等待者。
   * @param token lock_async 回调给出的令牌
   */
  void unlock(uint64_t token);

 private:
  /**
   * 当前 held_ 是否与 keys 无交集（已持 mu_）。
   * @param keys 候选 key 集
   * @return true 表示可立即授予
   */
  bool can_hold_locked(const std::vector<std::string>& keys) const;
  /**
   * 把 keys 记入 held_ 并分配新 token（已持 mu_）。
   * @param keys 要占用的 key
   * @return 新 token
   */
  uint64_t grant_locked(const std::vector<std::string>& keys);
  /**
   * 从队头起尽量授予等待者，收集可立即回调的 (token,cb)（已持 mu_）。
   * @param ready 输出：可在锁外回调的列表
   */
  void drain_async_locked(std::vector<std::pair<uint64_t, std::function<void(uint64_t)>>>* ready);

  /* 异步等待者：解锁后按 FIFO 尝试授予。 */
  struct AsyncWaiter {
    std::vector<std::string> keys;     // 等待的 key 集
    std::function<void(uint64_t)> cb;  // 授予后回调
  };

  std::mutex mu_;                                   // 保护 held_ / 队列
  std::unordered_map<std::string, uint64_t> held_;  // key → 占用 token
  std::deque<AsyncWaiter> async_waiters_;           // 等待队列（旧→新）
  uint64_t next_token_{1};                          // 下一个 token；0 保留为「无锁」
};

/* 门面：单 key latch；propose / admin 写路径占锁入口。 */
class Scheduler {
 public:
  /**
   * 释放 latch；token==0 视为无锁（空操作）。
   * @param latch_token lock_key_async 回调给出的令牌
   */
  void unlock(uint64_t latch_token) {
    if (latch_token != 0) {
      latches_.unlock(latch_token);
    }
  }

  /**
   * 异步占单个 latch key；与 Latches::lock_async 同语义。
   * @param latch_key 冲突域 key（通常即用户 key）
   * @param cb 拿到 token 后的回调
   */
  void lock_key_async(const std::string& latch_key, std::function<void(uint64_t token)> cb) {
    latches_.lock_async({latch_key}, std::move(cb));
  }

 private:
  Latches latches_;  // 底层多 key 锁表
};

}  // namespace openkv::storage
