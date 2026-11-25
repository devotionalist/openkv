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

/* Ready 落盘后台线程池：Store 把 persist 丢进来，主路径不被 fsync 卡住。 */
class PersistWorker {
 public:
  using Job = std::function<void()>;

  /**
   * 构造未启动的线程池。
   * @param workers worker 个数；至少按 1 使用
   */
  explicit PersistWorker(size_t workers);
  ~PersistWorker();

  /**
   * 拉起 worker 线程；重复调用无害。
   */
  void start();
  /**
   * 停线程并排空队列；析构前应调用。
   */
  void stop();
  /**
   * 是否已 start 且未 stop。
   * @return true 表示可 submit
   */
  bool running() const { return running_; }

  /**
   * 投递落盘任务；队列满时在调用方线程同步执行（背压，不丢）。
   * @param job 落盘回调（在 PersistWorker 线程或调用方线程执行）
   */
  void submit(Job job);

 private:
  /**
   * 单个 worker 的取任务循环。
   */
  void loop();

  size_t n_{1};                              // worker 个数
  std::mutex mu_;                            // 保护队列
  std::condition_variable cv_;               // 有任务 / 停机唤醒
  std::deque<Job> q_;                        // 待落盘任务
  std::vector<std::thread> threads_;         // worker 线程
  bool running_{false};                      // start/stop 生命周期
  static constexpr size_t kMaxQueue = 4096;  // 有界队列上限；满则同步执行
};

}  // namespace openkv::raftstore
