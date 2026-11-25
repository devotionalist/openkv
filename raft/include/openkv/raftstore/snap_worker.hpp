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

// @file snap_worker.hpp
// 职责：出站快照**独立通道**——与 AE/心跳隔离；同 Region 合并、失败重试、Stats。
// 架构：单独线程消费 SnapTask；Provider 拉快照，Sender 发出 InstallSnapshot。
// 协作：Store / Apply 路径投递 SnapTask；勿在 raft worker 里同步生成大快照。

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

#include "openkv/raftstore/msg.hpp"

namespace openkv::raftstore {

/* 出站快照独立通道：与 AE/心跳隔离；同 Region 合并、失败重试。 */
class SnapWorker {
 public:
  using Provider = std::function<std::string(uint32_t region_id, raft::SnapshotMeta* meta)>;
  using Sender = std::function<void(uint32_t region_id, const raft::SnapshotMeta& meta,
                                    const std::string& data)>;

  /* 队列容量、重试与同 Region 合并策略。 */
  struct Options {
    size_t queue_capacity{64};   // 待发任务上限；满则 enqueue 失败
    uint32_t max_retries{2};     // 发送失败后再试次数（不含首次）；0=不重试
    bool coalesce_region{true};  // 队列中同 region 未发任务合并为最新
  };

  /* 可观测计数；用于运维与压测。 */
  struct Stats {
    uint64_t enqueued{0};   // 成功入队次数
    uint64_t dropped{0};    // 队列满丢弃次数
    uint64_t coalesced{0};  // 同 Region 合并掉的次数
    uint64_t sent{0};       // 发送成功次数
    uint64_t failed{0};     // 最终失败（含耗尽重试）
    uint64_t retried{0};    // 实际重试次数
  };

  /**
   * 构造未启动的快照通道。
   * @param opts 队列容量 / 重试 / 合并选项
   * @param provider 按 region 生成快照字节与 meta
   * @param sender 把快照发给对端（InstallSnapshot）
   */
  SnapWorker(Options opts, Provider provider, Sender sender);
  ~SnapWorker();

  /**
   * 拉起消费线程。
   */
  void start();
  /**
   * 停线程；队列内未发任务丢弃。
   */
  void stop();

  /**
   * 投递出站快照任务；队列满可能丢弃（见 Stats::dropped）。
   * @param task 目标 region 与可选预生成数据
   * @return 是否入队成功
   */
  bool enqueue(SnapTask task);
  /**
   * 当前待发队列深度。
   * @return 队列中任务数
   */
  size_t depth() const;
  /**
   * 拷贝一份计数快照。
   * @return Stats 副本
   */
  Stats stats() const;

 private:
  /**
   * 单线程消费循环：取任务 → Provider → Sender → 失败重试。
   */
  void loop();
  /**
   * 持锁把失败任务按重试策略塞回队列。
   * @param task 刚失败的任务（attempt 已递增）
   */
  void requeue_locked(SnapTask task);

  Options opts_;                      // 容量 / 重试 / 合并
  Provider provider_;                 // 生成快照字节
  Sender sender_;                     // 出站 InstallSnapshot
  mutable std::mutex mu_;             // 保护队列与 Stats
  std::condition_variable cv_;        // 有任务 / 停机
  std::deque<SnapTask> q_;            // 待发快照
  Stats stats_{};                     // 计数
  std::atomic<bool> running_{false};  // 生命周期
  std::thread th_;                    // 消费线程
};

}  // namespace openkv::raftstore
