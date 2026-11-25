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

// @file index_waiter.cpp
// IndexWaiter：按 log index 挂 waiter / invoke 唤醒 / 异步超时。

#include "openkv/raftstore/index_waiter.hpp"

#include <algorithm>
#include <utility>

namespace openkv::raftstore {
namespace {

void fire_cbs(std::vector<IndexWaiter::CompleteCb>& cbs, const IndexWaiter::WaitOutcome& out) {
  for (auto& cb : cbs) {
    if (cb) {
      cb(out);
    }
  }
}

}  // namespace

IndexWaiter::IndexWaiter() = default;

IndexWaiter::~IndexWaiter() {
  stop_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(mu_);
    timeout_cv_.notify_all();
  }
  if (timeout_thread_.joinable()) {
    timeout_thread_.join();
  }
}

void IndexWaiter::prune_locked(uint64_t newest_index) {
  if (slots_.size() <= 4096) {
    return;
  }
  for (auto it = slots_.begin(); it != slots_.end() && slots_.size() > 2048;) {
    if (slot_idle(it->second) && it->first + 1024 < newest_index) {
      it = slots_.erase(it);
    } else {
      ++it;
    }
  }
}

std::vector<IndexWaiter::CompleteCb> IndexWaiter::take_async_cbs_locked(Slot& slot) {
  auto cbs = std::move(slot.async_cbs);
  if (!cbs.empty()) {
    slot.waiters = std::max(0, slot.waiters - static_cast<int>(cbs.size()));
  }
  return cbs;
}

void IndexWaiter::ensure_timeout_thread_locked() {
  if (timeout_thread_started_) {
    return;
  }
  timeout_thread_started_ = true;
  timeout_thread_ = std::thread([this] { timeout_loop(); });
}

void IndexWaiter::timeout_loop() {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_.load(std::memory_order_acquire)) {
    if (timeouts_.empty()) {
      timeout_cv_.wait(lock,
                       [&] { return stop_.load(std::memory_order_acquire) || !timeouts_.empty(); });
      continue;
    }

    const auto deadline = std::min_element(timeouts_.begin(), timeouts_.end(),
                                           [](const TimeoutItem& a, const TimeoutItem& b) {
                                             return a.deadline < b.deadline;
                                           })
                              ->deadline;
    timeout_cv_.wait_until(lock, deadline);
    if (stop_.load(std::memory_order_acquire)) {
      break;
    }

    const auto now = std::chrono::steady_clock::now();
    std::vector<TimeoutItem> due;
    timeouts_.erase(std::remove_if(timeouts_.begin(), timeouts_.end(),
                                   [&](const TimeoutItem& t) {
                                     if (t.deadline > now) {
                                       return false;
                                     }
                                     due.push_back(t);
                                     return true;
                                   }),
                    timeouts_.end());

    for (const auto& t : due) {
      auto sit = slots_.find(t.index);
      if (sit == slots_.end()) {
        continue;
      }
      auto& slot = sit->second;
      if (slot.done || slot.gen != t.gen || slot.async_cbs.empty()) {
        continue;
      }

      timeout_total_.fetch_add(1, std::memory_order_relaxed);
      WaitOutcome out;
      out.timed_out = true;
      auto cbs = take_async_cbs_locked(slot);
      if (slot_idle(slot)) {
        slots_.erase(sit);
      }

      lock.unlock();
      fire_cbs(cbs, out);
      lock.lock();
    }
  }
}

void IndexWaiter::invoke(uint64_t index, std::string error) {
  std::vector<CompleteCb> cbs;
  WaitOutcome out;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto& slot = slots_[index];
    slot.done = true;
    slot.error = std::move(error);
    ++slot.gen;
    out.error = slot.error;
    cbs = take_async_cbs_locked(slot);
    slot.cv.notify_all();
    prune_locked(index);
  }
  fire_cbs(cbs, out);
}

void IndexWaiter::wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb) {
  if (!cb) {
    return;
  }
  const auto t0 = std::chrono::steady_clock::now();
  std::unique_lock<std::mutex> lock(mu_);
  auto& slot = slots_[index];
  if (slot.done) {
    WaitOutcome out;
    out.error = slot.error;
    if (slot_idle(slot)) {
      slots_.erase(index);
    }
    lock.unlock();
    cb(std::move(out));
    return;
  }

  slot.async_cbs.push_back(std::move(cb));
  ++slot.waiters;
  timeouts_.push_back(TimeoutItem{index, slot.gen, t0 + timeout});
  ensure_timeout_thread_locked();
  timeout_cv_.notify_one();
}

IndexWaiter::WaitOutcome IndexWaiter::wait(uint64_t index, std::chrono::milliseconds timeout,
                                           const std::function<void()>& tick) {
  const auto t0 = std::chrono::steady_clock::now();
  const auto deadline = t0 + timeout;
  WaitOutcome out;

  std::unique_lock<std::mutex> lock(mu_);
  {
    auto& slot = slots_[index];
    if (slot.done) {
      out.error = slot.error;
      if (slot_idle(slot)) {
        slots_.erase(index);
      }
      return out;
    }
    ++slot.waiters;
  }

  while (true) {
    auto it = slots_.find(index);
    if (it == slots_.end()) {
      slots_[index].waiters = 1;
      it = slots_.find(index);
    }
    if (it->second.done) {
      out.error = it->second.error;
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      out.timed_out = true;
      timeout_total_.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    if (!tick) {
      it->second.cv.wait_until(lock, deadline);
      continue;
    }
    it->second.cv.wait_until(lock, std::min(now + std::chrono::milliseconds(5), deadline));
    it = slots_.find(index);
    if (it != slots_.end() && it->second.done) {
      out.error = it->second.error;
      break;
    }
    lock.unlock();
    tick();
    lock.lock();
  }

  auto it = slots_.find(index);
  if (it != slots_.end()) {
    if (it->second.waiters > 0) {
      --it->second.waiters;
    }
    if (slot_idle(it->second)) {
      out.error = it->second.error;
      slots_.erase(it);
    }
  }
  return out;
}

}  // namespace openkv::raftstore
