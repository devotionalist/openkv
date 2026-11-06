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

// @file scaler.hpp
// 在线扩容编排器（S1）：Learner 加入 → 追赶 → 提升 Voter → 路由版本 bump。
//
// 架构位置：Meta Leader 后台 worker 调用 Storage Leader 的 ConfChange 钩子
//（AddLearnerFn / CatchUpFn / PromoteFn），状态机阶段写回 ScaleTask；
// ClusterState 的 Learner/Promote/Bump 经 Meta Raft 提交。

#pragma once

#include "opskv/meta/cluster_state.hpp"

#include <atomic>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace opskv::meta {

/// 扩容任务生命周期阶段（worker 按序推进）。
enum class ScaleTaskState {
  kPending,         ///< 已创建，等待 worker 拾取
  kAddingLearner,   ///< 正在向各 Region 添加 Learner
  kCatchingUp,      ///< 等待 Learner 日志追平（match ≥ commit）
  kPromotingVoter,  ///< 正在 Promote Learner → Voter
  kUpdatingRoute,   ///< 递增 route_ver，通知 Gateway
  kCompleted,       ///< 成功结束
  kFailed           ///< 失败（见 ScaleTask::error）
};

/// 一次 scale-out 任务的运行时视图（内存；不入 ClusterState 快照）。
struct ScaleTask {
  std::string task_id;                           ///< 任务唯一 ID（seq 生成）
  ScaleTaskState state{ScaleTaskState::kPending};///< 当前阶段
  std::string new_node_id;                       ///< 新加入的 Storage 节点 ID
  std::vector<uint32_t> shard_ids;               ///< 要扩的 Region 列表（空=全部分片）
  std::string error;                             ///< 失败原因（仅 kFailed）
  double progress{0.0};                          ///< 0~1 进度提示（UI/运维）
};

/// 向某 Region 的 Storage Leader 下发 AddLearner ConfChange。
using AddLearnerFn = std::function<bool(uint32_t shard_id, const Replica& learner)>;
/// 将 Learner 提升为 Voter（Storage ConfChange + Meta kPromote）。
using PromoteFn = std::function<bool(uint32_t shard_id, const std::string& node_id)>;
/// 扩容完成后 bump 路由版本（Meta kBumpRoute）。
using BumpRouteFn = std::function<bool()>;
/// S1：查询 Learner 是否已追平；返回 true 才进入 Promote。空钩子则跳过等待（单测）。
using CatchUpFn = std::function<bool(uint32_t shard_id, const std::string& node_id)>;

/// 在线扩容编排器：后台线程推进 tasks_。
class Scaler {
 public:
  /// @param state        共享 ClusterState（读节点地址 / 写经钩子 Raft）
  /// @param add_learner  Storage ADMIN_ADD_LEARNER + Meta set_learner
  /// @param promote      Storage ADMIN_PROMOTE + Meta promote
  /// @param bump         可选：完成后 bump route
  /// @param catch_up     可选：追赶门闩
  /// @param is_leader    可选：仅 Leader 推进；空则总是推进
  Scaler(ClusterState& state, AddLearnerFn add_learner, PromoteFn promote,
         BumpRouteFn bump = nullptr, CatchUpFn catch_up = nullptr,
         std::function<bool()> is_leader = nullptr);
  ~Scaler();

  /// 启动对 new_storage_node_id 的扩容；shard_ids 为空时对全部 Region。
  /// 已有进行中任务时入队排队（不立刻 Failed），worker 串行推进避免 Joint 撞车。
  ScaleTask start_scale_out(const std::string& new_storage_node_id,
                            std::vector<uint32_t> shard_ids = {});
  /// 按 task_id 查询；不存在返回 nullopt。
  std::optional<ScaleTask> get_task(const std::string& task_id) const;
  /// 列出全部扩容任务（含已完成）。
  std::vector<ScaleTask> list_tasks() const;
  /// 是否有未完成的 SCALE_OUT（供 Scheduler 暂停 auto-balance）。
  bool has_inflight() const;
  /// 取消未完成任务（error=cancelled）。task_id 空则按 node[+shard] 匹配。
  int cancel_inflight(const std::string& node_id, std::optional<uint32_t> shard_id = std::nullopt);

 private:
  /// 后台循环：定期扫描未完成任务并 advance。
  void worker_loop();
  /// 按当前 state 推进一个任务（可能多 Region 循环）。
  void advance(ScaleTask& task);
  /// 是否与进行中任务在同一 node / 重叠 Region。
  bool overlaps_inflight_locked(const std::string& node_id,
                                const std::vector<uint32_t>& shard_ids) const;

  ClusterState& state_;              ///< 非拥有：节点/路由只读查询
  AddLearnerFn add_learner_;         ///< ConfChange：加 Learner
  PromoteFn promote_;                ///< ConfChange：提升 Voter
  BumpRouteFn bump_;                 ///< 完成后 bump 路由
  CatchUpFn catch_up_;               ///< 追赶检查（可空）
  std::function<bool()> is_leader_;  ///< Leader 门闩（可空）
  mutable std::mutex mu_;            ///< 保护 tasks_ / queue_
  std::unordered_map<std::string, ScaleTask> tasks_;  ///< task_id → 任务
  std::vector<std::string> queue_;   ///< FIFO：未完成 task_id 顺序
  std::atomic<bool> running_{false}; ///< worker 运行标志
  std::thread worker_;               ///< 后台推进线程
  std::atomic<uint64_t> task_seq_{0};///< 生成 task_id 的序号
};

}  // namespace opskv::meta
