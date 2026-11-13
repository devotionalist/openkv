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

// @file proposal_queue.cpp

#include "openkv/storage/proposal_queue.hpp"

#include <algorithm>
#include <sstream>
#include <utility>

namespace openkv::storage {
namespace {

uint64_t elapsed_us(std::chrono::steady_clock::time_point t0) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0)
          .count());
}

void fire_cbs(std::vector<ProposalQueue::CompleteCb>& cbs, const ProposalQueue::WaitOutcome& out) {
  for (auto& cb : cbs) {
    if (cb) cb(out);
  }
}

}  // namespace

ProposalQueue::ProposalQueue() = default;

ProposalQueue::~ProposalQueue() {
  stop_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(mu_);
    timeout_cv_.notify_all();
  }
  if (timeout_thread_.joinable()) timeout_thread_.join();
}

std::string ProposalQueue::prometheus_text() const {
  std::ostringstream oss;
  oss << "# HELP skv_apply_wait_us Propose-to-apply wait latency in microseconds\n";
  oss << "# TYPE skv_apply_wait_us histogram\n";
  oss << wait_hist_.encode_prometheus("skv_apply_wait_us", "component=\"apply\"");
  oss << "# HELP skv_apply_timeout_total Apply wait timeouts\n";
  oss << "# TYPE skv_apply_timeout_total counter\n";
  oss << "skv_apply_timeout_total " << timeout_total() << '\n';
  return oss.str();
}

void ProposalQueue::record_wait_us(uint64_t us) { wait_hist_.observe(us); }

ProposalQueue::CompleteCb ProposalQueue::wrap_latency(std::chrono::steady_clock::time_point t0,
                                                      CompleteCb cb) {
  return [this, t0, user = std::move(cb)](WaitOutcome out) mutable {
    record_wait_us(elapsed_us(t0));
    user(std::move(out));
  };
}

void ProposalQueue::prune_locked(uint64_t newest_index) {
  if (slots_.size() <= 4096) return;
  for (auto it = slots_.begin(); it != slots_.end() && slots_.size() > 2048;) {
    if (slot_idle(it->second) && it->first + 1024 < newest_index) {
      it = slots_.erase(it);
    } else {
      ++it;
    }
  }
}

std::vector<ProposalQueue::CompleteCb> ProposalQueue::take_async_cbs_locked(Slot& slot) {
  auto cbs = std::move(slot.async_cbs);
  if (!cbs.empty()) {
    slot.waiters = std::max(0, slot.waiters - static_cast<int>(cbs.size()));
  }
  return cbs;
}

void ProposalQueue::ensure_timeout_thread_locked() {
  if (timeout_thread_started_) return;
  timeout_thread_started_ = true;
  timeout_thread_ = std::thread([this] { timeout_loop(); });
}

void ProposalQueue::timeout_loop() {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_.load(std::memory_order_acquire)) {
    if (timeouts_.empty()) {
      timeout_cv_.wait(lock, [&] {
        return stop_.load(std::memory_order_acquire) || !timeouts_.empty();
      });
      continue;
    }

    const auto deadline =
        std::min_element(timeouts_.begin(), timeouts_.end(),
                         [](const TimeoutItem& a, const TimeoutItem& b) {
                           return a.deadline < b.deadline;
                         })
            ->deadline;
    timeout_cv_.wait_until(lock, deadline);
    if (stop_.load(std::memory_order_acquire)) break;

    const auto now = std::chrono::steady_clock::now();
    std::vector<TimeoutItem> due;
    timeouts_.erase(std::remove_if(timeouts_.begin(), timeouts_.end(),
                                   [&](const TimeoutItem& t) {
                                     if (t.deadline > now) return false;
                                     due.push_back(t);
                                     return true;
                                   }),
                    timeouts_.end());

    for (const auto& t : due) {
      auto sit = slots_.find(t.index);
      if (sit == slots_.end()) continue;
      auto& slot = sit->second;
      // gen 不匹配：已被 invoke；async_cbs 空：已完成或已被别处取走。
      if (slot.done || slot.gen != t.gen || slot.async_cbs.empty()) continue;

      timeout_total_.fetch_add(1, std::memory_order_relaxed);
      WaitOutcome out;
      out.timed_out = true;
      auto cbs = take_async_cbs_locked(slot);
      if (slot_idle(slot)) slots_.erase(sit);

      lock.unlock();
      fire_cbs(cbs, out);
      lock.lock();
    }
  }
}


// 调用
void ProposalQueue::invoke(uint64_t index, std::string error) {
  std::vector<CompleteCb> cbs;
  WaitOutcome out;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto& slot = slots_[index];
    slot.done = true;
    slot.error = std::move(error);
    ++slot.gen;  // 使未触发的 timeout 条目失效
    out.error = slot.error;
    cbs = take_async_cbs_locked(slot);
    slot.cv.notify_all();
    prune_locked(index);
  }
  fire_cbs(cbs, out);
}

// 异步等待
void ProposalQueue::wait_async(uint64_t index, std::chrono::milliseconds timeout, CompleteCb cb) {
  if (!cb) return;
  const auto t0 = std::chrono::steady_clock::now();
  std::unique_lock<std::mutex> lock(mu_);
  auto& slot = slots_[index];
  if (slot.done) {
    WaitOutcome out;
    out.error = slot.error;
    if (slot_idle(slot)) slots_.erase(index);
    lock.unlock();
    record_wait_us(elapsed_us(t0));
    cb(std::move(out));
    return;
  }

  slot.async_cbs.push_back(wrap_latency(t0, std::move(cb)));
  ++slot.waiters;
  timeouts_.push_back(TimeoutItem{index, slot.gen, t0 + timeout});
  ensure_timeout_thread_locked();
  timeout_cv_.notify_one();
}

ProposalQueue::WaitOutcome ProposalQueue::wait(uint64_t index, std::chrono::milliseconds timeout,
                                               const std::function<void()>& tick) {
  const auto t0 = std::chrono::steady_clock::now(); // 开始时间
  const auto deadline = t0 + timeout; // 截止时间
  WaitOutcome out; // 等待结果

  std::unique_lock<std::mutex> lock(mu_); // 互斥锁
  {
    auto& slot = slots_[index]; // 槽
    if (slot.done) {
      out.error = slot.error; // 错误
      if (slot_idle(slot)) slots_.erase(index); // 删除槽
      record_wait_us(elapsed_us(t0)); // 记录等待时间
      return out; // 返回等待结果
    }
    ++slot.waiters; // 增加等待者
  }

  while (true) {// 循环
    auto it = slots_.find(index); // 查找槽
    if (it == slots_.end()) {
      slots_[index].waiters = 1; // 设置等待者
      it = slots_.find(index); // 查找槽
    }
    if (it->second.done) { // 是否完成
      out.error = it->second.error; // 错误
      break; // 中断
    }
    const auto now = std::chrono::steady_clock::now(); // 当前时间
    if (now >= deadline) { // 是否超时
      out.timed_out = true;
      timeout_total_.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    if (!tick) {// 是否需要tick
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

  if (auto it = slots_.find(index); it != slots_.end()) {
    if (it->second.waiters > 0) --it->second.waiters;
    if (slot_idle(it->second)) {
      out.error = it->second.error;
      slots_.erase(it);
    }
  }
  record_wait_us(elapsed_us(t0));
  return out;
}

}  // namespace openkv::storage
