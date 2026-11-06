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

// @file region_scheduler.hpp
// Region 迁移调度器（M2/S2）：后台扫描 draining/offline 上有副本或欠副本（voter < RF），
// 提交 MigrateTask，经 Place → CommitPlace 推进；任务可持久化到 ClusterState。

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

/// 迁移任务状态机阶段。
enum class MigrateState {
  kPending,        ///< 已提交，等待执行
  kPlacing,        ///< 正在对 Storage 做 ENSURE/learner/promote；place 失败仍可进 UpdatingRoute（尽力而为）
  kUpdatingRoute,  ///< 提交 Meta PLACE_REGION 更新路由（即使 place 未完全成功也会尝试）
  kCompleted,      ///< 成功
  kFailed,         ///< 失败（见 MigrateTask::error）
  kCancelled,      ///< 运维取消
};

/// 运行时迁移任务（内存视图；持久化子集见 PersistMigrate）。
struct MigrateTask {
  std::string task_id;                      ///< 任务唯一 ID
  MigrateState state{MigrateState::kPending};///< 当前阶段
  uint32_t region_id{0};                    ///< 目标 Region
  std::vector<std::string> desired_peers;   ///< 期望最终副本 node_id 列表
  std::string error;                        ///< 失败原因
  double progress{0.0};                     ///< 0~1 进度提示
  uint32_t place_attempts{0};               ///< 数据面 place 失败次数；超限 → Failed
};

/// 数据面编排：对 Storage 执行 place（ENSURE / learner / promote / …）；成功返回 true。
using PlaceRegionFn =
    std::function<bool(uint32_t region_id, const std::vector<std::string>& desired)>;
/// 更新 Meta 路由：Raft kPlaceRegion。
using CommitPlaceFn =
    std::function<bool(uint32_t region_id, const std::vector<std::string>& desired)>;
/// 仅 Leader 推进调度。
using IsLeaderFn = std::function<bool()>;
/// S2：将任务写入 Meta Raft / ClusterState 快照。
using PersistMigrateFn = std::function<bool(const PersistMigrate&)>;
/// S2：任务结束时从快照擦除。
using EraseMigrateFn = std::function<bool(const std::string& task_id)>;

/// Region 副本迁移调度器。
class RegionScheduler {
 public:
  /// @param state        共享 ClusterState
  /// @param place        数据面 place 钩子
  /// @param commit       Meta 路由 PLACE 钩子
  /// @param is_leader    Leader 门闩
  /// @param max_inflight 同时进行中的迁移上限
  /// @param persist/erase 可选持久化钩子（空则仅内存）
  RegionScheduler(ClusterState& state, PlaceRegionFn place, CommitPlaceFn commit,
                  IsLeaderFn is_leader, uint32_t max_inflight = 2,
                  PersistMigrateFn persist = nullptr, EraseMigrateFn erase = nullptr);
  ~RegionScheduler();

  /// 显式提交迁移；desired 空则用 suggest_placement 生成目标副本集。
  MigrateTask submit(uint32_t region_id, std::vector<std::string> desired = {});
  /// 取消未完成任务；已完成/失败则返回 false。
  bool cancel(const std::string& task_id);
  /// 按 ID 查询任务。
  std::optional<MigrateTask> get_task(const std::string& task_id) const;
  /// 列出全部任务。
  std::vector<MigrateTask> list_tasks() const;

  /// S2：从 ClusterState 快照恢复未完成任务（Meta 重启后调用）。
  /// Placing/UpdatingRoute 会重置为 Pending，由 worker 重新推进。
  void restore_persisted(const std::vector<PersistMigrate>& persisted);

  /// 扫描候选 Region：draining 上仍有副本，或副本数 < RF。
  /// （不再用全局 ideal 误触发全量迁移。）
  std::vector<uint32_t> scan_candidates() const;

  /// 显式触发一轮 balance 入队（RF 不变 Replace）；返回入队任务数。
  size_t enqueue_balance(size_t max_new = 1);

  /// Draining 且已无 Region → Tombstone（经 set_tombstone_ 钩子写 Raft）。
  using SetTombstoneFn = std::function<bool(const std::string& node_id)>;
  void set_tombstone_hook(SetTombstoneFn fn) { set_tombstone_ = std::move(fn); }

  /// 返回 true 时跳过 auto-balance（SCALE_OUT 进行中、运维窗口等）。
  using PauseBalanceFn = std::function<bool()>;
  void set_pause_balance_hook(PauseBalanceFn fn) { pause_balance_ = std::move(fn); }

  /// 关闭扫描里的自动 balance（缩容 Replace 仍走 scan_candidates）；显式 BALANCE 仍可用。
  void set_auto_balance(bool on) { auto_balance_ = on; }

 private:
  /// 后台循环：扫候选、推进 inflight 任务。
  void worker_loop();
  /// 若 inflight < max，从 scan_candidates 入队新任务。
  void maybe_enqueue_from_scan();
  /// 推进单个任务的状态机（调用方**不**持 mu_；内部自行加锁，place_ 在锁外执行）。
  void advance(const std::string& task_id);
  /// 当前 Pending/Placing/UpdatingRoute 数量（调用方持 mu_）。
  size_t inflight_count_locked() const;
  /// 将任务快照写入 persist_（持锁）。
  void persist_locked(const MigrateTask& task);
  /// 完成后 erase 持久化记录。
  void erase_persisted(const std::string& task_id);

  static constexpr uint32_t kMaxPlaceAttempts = 8;

  ClusterState& state_;           ///< 非拥有：读拓扑 / suggest
  PlaceRegionFn place_;           ///< Storage 数据面编排
  CommitPlaceFn commit_;          ///< Meta PLACE_REGION
  IsLeaderFn is_leader_;          ///< 仅 Leader 工作
  PersistMigrateFn persist_;      ///< 可选：Raft upsert 迁移
  EraseMigrateFn erase_;          ///< 可选：Raft erase 迁移
  SetTombstoneFn set_tombstone_;  ///< 可选：空 Draining → Tombstone
  PauseBalanceFn pause_balance_;  ///< 可选：暂停 auto-balance
  bool auto_balance_{false};      ///< 默认关：避免 Join 后立刻抢 Region 与手工加副本打架
  uint32_t max_inflight_;         ///< 并发迁移上限
  bool was_leader_{false};        ///< 上一轮 worker 是否为 Leader（用于换主 restore）

  mutable std::mutex mu_;         ///< 保护 tasks_ / region_to_task_
  std::unordered_map<std::string, MigrateTask> tasks_;           ///< task_id → 任务
  std::unordered_map<uint32_t, std::string> region_to_task_;     ///< Region→task，防重复入队
  std::atomic<bool> running_{false};  ///< worker 运行标志
  std::thread worker_;                ///< 后台线程
  std::atomic<uint64_t> task_seq_{0}; ///< 生成 task_id
};

}  // namespace opskv::meta
