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

// @file batch_system.hpp
// 职责：Raft / Apply **双 worker 池**；有活 Region 经 ScheduleQueue 唤醒，mailbox
// drain-until-empty。 架构：region_id % N 映射到固定 worker，保证同 Region 串行；跨 Region 并行。
// 协作：Router 投递成功后 notify → schedule；Store 注册 RaftHandler / ApplyHandler。
// 注意：单 Region 热点时多 raft worker 帮不上（region 钉死）；persist 可经 PersistWorker
// 与其它 Region 的 raft 步进重叠（见 Store persist_workers）。

#pragma once

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

#include "openkv/raftstore/router.hpp"
#include "openkv/raftstore/schedule_queue.hpp"

namespace openkv::raftstore {

/* 双池调度器：Raft 池跑协议/Ready，Apply 池跑状态机；同 Region 固定 worker 串行。 */
class BatchSystem {
 public:
  /* worker 数、批大小、轮询超时与 Apply/Snap 深度限流。 */
  struct Options {
    size_t raft_workers{8};              // Raft worker 线程数
    size_t apply_workers{8};             // Apply worker 线程数
    size_t batch_size{256};              // 每轮从邮箱最多取多少条
    int poll_timeout_ms{5};              // ScheduleQueue 空闲等待 ms
    size_t max_apply_queue_depth{8192};  // Apply 邮箱深度上限；0=不限
    size_t max_snap_queue_depth{64};     // 在途快照上限；0=不限
  };

  using RaftHandler = std::function<void(uint32_t region_id, std::vector<PeerMsg>)>;
  using ApplyHandler = std::function<void(uint32_t region_id, std::vector<ApplyControl>)>;

  /**
   * 构造未启动的双池；须 set_*_handler 后 start。
   * @param router 共享邮箱路由
   * @param opts worker / 限流参数
   */
  BatchSystem(std::shared_ptr<Router> router, Options opts);
  ~BatchSystem();

  /**
   * 注册 Raft 批处理回调（通常 Store::on_raft_batch）。
   * @param h region + 本批 PeerMsg
   */
  void set_raft_handler(RaftHandler h) { raft_handler_ = std::move(h); }
  /**
   * 注册 Apply 批处理回调（通常 Store::on_apply_batch）。
   * @param h region + 本批 ApplyControl
   */
  void set_apply_handler(ApplyHandler h) { apply_handler_ = std::move(h); }

  /**
   * 拉起全部 Raft/Apply worker。
   */
  void start();
  /**
   * 停机并 join 线程。
   */
  void stop();
  /**
   * 是否已启动。
   * @return true 表示 worker 在跑
   */
  bool running() const { return running_.load(std::memory_order_relaxed); }

  /**
   * Router 投递成功后调用：把 region 交给对应 Raft worker（去重）。
   * @param region_id Region id
   */
  void schedule_raft(uint32_t region_id);
  /**
   * 把 region 交给对应 Apply worker（去重）。
   * @param region_id Region id
   */
  void schedule_apply(uint32_t region_id);

  /**
   * 该 Region Apply 邮箱是否超过 max_apply_queue_depth。
   * @param region_id Region id
   * @return true 表示过载
   */
  bool apply_overloaded(uint32_t region_id) const;
  /**
   * 在途快照是否超过 max_snap_queue_depth。
   * @return true 表示过载
   */
  bool snap_overloaded() const;
  /**
   * 快照入队时 +1（与 SnapWorker 配对）。
   */
  void note_snap_enqueued() { ++snap_inflight_; }
  /**
   * 快照发送结束时 -1。
   */
  void note_snap_done() {
    if (snap_inflight_ > 0) {
      --snap_inflight_;
    }
  }

 private:
  /**
   * 单个 Raft worker 主循环。
   * @param worker_id 池内下标（region_id % N）
   */
  void raft_worker_loop(size_t worker_id);
  /**
   * 单个 Apply worker 主循环。
   * @param worker_id 池内下标
   */
  void apply_worker_loop(size_t worker_id);

  std::shared_ptr<Router> router_;          // 邮箱路由
  Options opts_;                            // 池参数
  RaftHandler raft_handler_;                // Raft 批回调
  ApplyHandler apply_handler_;              // Apply 批回调
  std::atomic<bool> running_{false};        // 生命周期
  std::vector<std::thread> raft_threads_;   // Raft 线程
  std::vector<std::thread> apply_threads_;  // Apply 线程
  std::vector<ScheduleQueue> raft_sched_;   // 每 Raft worker 一条唤醒队列
  std::vector<ScheduleQueue> apply_sched_;  // 每 Apply worker 一条唤醒队列
  std::atomic<uint64_t> snap_inflight_{0};  // 在途出站快照数
};

}  // namespace openkv::raftstore
