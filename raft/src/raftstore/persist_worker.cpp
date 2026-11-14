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

void PersistWorker::start() {
  if (running_) return;
  running_ = true;
  threads_.reserve(n_);
  for (size_t i = 0; i < n_; ++i) {
    threads_.emplace_back([this] { loop(); });
  }
}

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

void PersistWorker::loop() {
  for (;;) {
    Job job;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return !running_ || !q_.empty(); });
      if (!running_ && q_.empty()) return;
      if (q_.empty()) continue;
      job = std::move(q_.front());
      q_.pop_front();
    }
    if (job) job();
  }
}

}  // namespace openkv::raftstore
