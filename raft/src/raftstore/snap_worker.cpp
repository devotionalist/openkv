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

// @file snap_worker.cpp

#include "opskv/raftstore/snap_worker.hpp"

#include "opskv/logging.hpp"

#include <chrono>

namespace opskv::raftstore {

SnapWorker::SnapWorker(Options opts, Provider provider, Sender sender)
    : opts_(opts), provider_(std::move(provider)), sender_(std::move(sender)) {
  if (opts_.queue_capacity == 0) opts_.queue_capacity = 64;
}

SnapWorker::~SnapWorker() { stop(); }

void SnapWorker::start() {
  if (running_.exchange(true)) return;
  th_ = std::thread([this] { loop(); });
}

void SnapWorker::stop() {
  if (!running_.exchange(false)) return;
  cv_.notify_all();
  if (th_.joinable()) th_.join();
}

void SnapWorker::requeue_locked(SnapTask task) {
  if (q_.size() >= opts_.queue_capacity) {
    ++stats_.dropped;
    SKV_WARN << "snap_worker requeue full drop region=" << task.region_id
             << " attempt=" << task.attempt;
    return;
  }
  q_.push_front(std::move(task));
  ++stats_.retried;
}

bool SnapWorker::enqueue(SnapTask task) {
  std::lock_guard<std::mutex> lk(mu_);
  if (opts_.coalesce_region) {
    for (auto& existing : q_) {
      if (existing.region_id == task.region_id) {
        existing.meta = std::move(task.meta);
        if (!task.data.empty()) existing.data = std::move(task.data);
        // 合并后保留较小 attempt，避免已接近上限的任务被顶掉后立刻耗尽
        if (task.attempt < existing.attempt) existing.attempt = task.attempt;
        ++stats_.coalesced;
        ++stats_.enqueued;
        cv_.notify_one();
        return true;
      }
    }
  }
  if (q_.size() >= opts_.queue_capacity) {
    ++stats_.dropped;
    SKV_WARN << "snap_worker queue full drop region=" << task.region_id;
    return false;
  }
  q_.push_back(std::move(task));
  ++stats_.enqueued;
  cv_.notify_one();
  return true;
}

size_t SnapWorker::depth() const {
  std::lock_guard<std::mutex> lk(mu_);
  return q_.size();
}

SnapWorker::Stats SnapWorker::stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  return stats_;
}

void SnapWorker::loop() {
  while (running_) {
    SnapTask task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_for(lk, std::chrono::milliseconds(50),
                   [&] { return !q_.empty() || !running_; });
      if (!running_ && q_.empty()) break;
      if (q_.empty()) continue;
      task = std::move(q_.front());
      q_.pop_front();
    }
    try {
      if (task.data.empty() && provider_) {
        task.data = provider_(task.region_id, &task.meta);
      }
      if (sender_) sender_(task.region_id, task.meta, task.data);
      {
        std::lock_guard<std::mutex> lk(mu_);
        ++stats_.sent;
      }
    } catch (const std::exception& e) {
      SKV_WARN << "snap_worker region=" << task.region_id << " attempt=" << task.attempt
               << " err=" << e.what();
      std::lock_guard<std::mutex> lk(mu_);
      if (task.attempt < opts_.max_retries) {
        ++task.attempt;
        requeue_locked(std::move(task));
      } else {
        ++stats_.failed;
      }
    }
  }
}

}  // namespace opskv::raftstore
