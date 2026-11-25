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

// @file persist_worker.hpp
// 职责：Raft Ready 落盘 IO 线程池；raft worker 提交 job 后可处理其它 Region。
// 架构：有界队列 + N worker；同 Region 串行由 Store 的 persist_inflight 保证。
// 协作：Store::process_ready 提交 consume；完成后投递 SignificantMsg::kPersistDone。

#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace openkv::raftstore {

/**
 * @brief
 *
 * 持久化线程是持久化线程的外部io，用于持久化线程的外部io。
 */
class PersistWorker {
 public:
  using Job = std::function<void()>;

  explicit PersistWorker(size_t workers);
  ~PersistWorker();

  void start();
  void stop();
  bool running() const { return running_; }

  /// 投递落盘任务；队列满时在调用方线程同步执行（背压，不丢）。
  void submit(Job job);

 private:
  void loop();

  size_t n_{1};    // 线程数量
  std::mutex mu_;  // 互斥锁
  std::condition_variable cv_;
  std::deque<Job> q_;                        // 队列
  std::vector<std::thread> threads_;         // 线程
  bool running_{false};                      // 是否运行
  static constexpr size_t kMaxQueue = 4096;  // 最大队列大小
};

}  // namespace openkv::raftstore
