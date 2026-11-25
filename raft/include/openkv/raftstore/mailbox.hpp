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

// @file mailbox.hpp
// 职责：每 Region 有界邮箱模板；try_send / recv_batch；满则丢弃并计数。
// 架构：仅头文件；PeerMailbox=Mailbox<PeerMsg>，ApplyMailbox=Mailbox<ApplyControl>。
// 协作：Router 持有 shared_ptr<Mailbox>；BatchSystem worker poll 批量取消息。

#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

#include "openkv/raftstore/msg.hpp"

namespace openkv::raftstore {

template <typename T>
class Mailbox {
 public:
  explicit Mailbox(size_t capacity = 4096) : capacity_(capacity == 0 ? 4096 : capacity) {}

  /// 【非网络】只入本机邮箱队列；满则丢。真正出网见 Store::transport_fn_ → GrpcRaftTransport。
  bool try_send(T msg) {
    std::lock_guard<std::mutex> lk(mu_);
    if (closed_ || q_.size() >= capacity_) {
      ++drop_count_;
      return false;
    }
    q_.push_back(std::move(msg));
    cv_.notify_one();
    return true;
  }

  /// 阻塞取最多 max_n 条；空且 timeout_ms>0 则等待。
  std::vector<T> recv_batch(size_t max_n, int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    if (q_.empty() && timeout_ms > 0 && !closed_) {
      cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                   [&] { return !q_.empty() || closed_; });
    }
    std::vector<T> out;
    out.reserve(std::min(max_n, q_.size()));
    while (!q_.empty() && out.size() < max_n) {
      out.push_back(std::move(q_.front()));
      q_.pop_front();
    }
    return out;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }
  uint64_t drop_count() const { return drop_count_; }
  void close() {
    std::lock_guard<std::mutex> lk(mu_);
    closed_ = true;
    cv_.notify_all();
  }
  bool closed() const {
    std::lock_guard<std::mutex> lk(mu_);
    return closed_;
  }

 private:
  size_t capacity_;             // 容量
  mutable std::mutex mu_;       // 互斥锁
  std::condition_variable cv_;  // 条件变量
  std::deque<T> q_;             // 队列
  bool closed_{false};          // 是否关闭
  uint64_t drop_count_{0};      // 丢弃计数
};

}  // namespace openkv::raftstore
