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

#include "openkv/raftstore/msg.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace openkv::raftstore {

class SnapWorker {
 public:
  using Provider = std::function<std::string(uint32_t region_id, raft::SnapshotMeta* meta)>;
  using Sender = std::function<void(uint32_t region_id, const raft::SnapshotMeta& meta,
                                    const std::string& data)>;

  struct Options {
    size_t queue_capacity{64};
    /// 发送失败后最多再试次数（不含首次）；0=不重试。
    uint32_t max_retries{2};
    /// 队列中已有同 region 未发任务时合并（保留最新 meta/data）。
    bool coalesce_region{true};
  };

  struct Stats {
    uint64_t enqueued{0};
    uint64_t dropped{0};
    uint64_t coalesced{0};
    uint64_t sent{0};
    uint64_t failed{0};
    uint64_t retried{0};
  };

  SnapWorker(Options opts, Provider provider, Sender sender);
  ~SnapWorker();

  void start();
  void stop();
  bool enqueue(SnapTask task);
  size_t depth() const;
  Stats stats() const;

 private:
  void loop();
  void requeue_locked(SnapTask task);

  Options opts_;
  Provider provider_;
  Sender sender_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<SnapTask> q_;
  Stats stats_{};
  std::atomic<bool> running_{false};
  std::thread th_;
};

}  // namespace openkv::raftstore
