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

// @file persist_worker.cpp
// PersistWorker：IO 线程执行 RaftEngine::consume / persist_fn。

#include "openkv/raftstore/persist_worker.hpp"

namespace openkv::raftstore {

PersistWorker::PersistWorker(size_t workers) : n_(workers == 0 ? 1 : workers) {}

PersistWorker::~PersistWorker() { stop(); }

/**
 * @brief 启动持久化线程：启动持久化线程是启动持久化线程的。
 *
 *
 * 如果使用外部io，则启动持久化线程。
 * 否则，启动持久化线程。
 */
void PersistWorker::start() {
  if (running_) return;  // 如果线程已运行，则返回
  running_ = true;
  threads_.reserve(n_);                         // 预留线程数量
  for (size_t i = 0; i < n_; ++i) {             // 创建线程
    threads_.emplace_back([this] { loop(); });  // 创建线程
  }
}

/**
 * @brief 停止持久化线程：停止持久化线程是停止持久化线程的外部io，用于停止持久化线程的外部io。
 *
 * 如果线程已运行，则停止线程。
 * 否则，停止线程。
 */
void PersistWorker::stop() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!running_) return;
    running_ = false;
  }
  cv_.notify_all();
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
  threads_.clear();
  // 排空残留：同步跑完，避免 stop 丢盘。
  for (;;) {
    Job job;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (q_.empty()) break;
      job = std::move(q_.front());
      q_.pop_front();
    }
    if (job) job();
  }
}

// 提交持久化任务
void PersistWorker::submit(Job job) {
  if (!job) return;
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (!running_ || q_.size() >= kMaxQueue) {
      lk.unlock();
      job();  // 背压：调用方线程同步落盘
      return;
    }
    q_.push_back(std::move(job));
  }
  cv_.notify_one();
}

//
void PersistWorker::loop() {
  for (;;) {
    Job job;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return !running_ || !q_.empty(); });  // 等待条件
      if (!running_ && q_.empty()) return;  // 如果线程未运行，或者队列为空，则返回
      if (q_.empty()) continue;             // 如果队列为空，则继续执行
      job = std::move(q_.front());          // 获取任务
      q_.pop_front();                       // 移除任务
    }
    if (job) job();  // 执行任务
  }
}

}  // namespace openkv::raftstore
