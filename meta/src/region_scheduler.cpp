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

// @file region_scheduler.cpp
// RegionScheduler 实现：
// - scan_candidates：draining/offline 上有副本，或 voter 数 < RF
// - maybe_enqueue_from_scan：inflight < max 时自动 submit
// - advance：Pending→Placing(place_)→UpdatingRoute(commit_)→Completed
// - persist_/erase_：任务变更写入 Meta Raft（S2 可恢复）

#include "opskv/meta/region_scheduler.hpp"

#include "opskv/logging.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <set>

namespace opskv::meta {
namespace {

PersistMigrate to_persist(const MigrateTask& t) {
  PersistMigrate pm;
  pm.task_id = t.task_id;
  pm.region_id = t.region_id;
  pm.state = static_cast<int>(t.state);
  pm.desired_peers = t.desired_peers;
  return pm;
}

}  // namespace

RegionScheduler::RegionScheduler(ClusterState& state, PlaceRegionFn place, CommitPlaceFn commit,
                                 IsLeaderFn is_leader, uint32_t max_inflight,
                                 PersistMigrateFn persist, EraseMigrateFn erase)
    : state_(state),
      place_(std::move(place)),
      commit_(std::move(commit)),
      is_leader_(std::move(is_leader)),
      persist_(std::move(persist)),
      erase_(std::move(erase)),
      max_inflight_(max_inflight == 0 ? 2 : max_inflight) {
  running_ = true;
  worker_ = std::thread([this] { worker_loop(); });
}

RegionScheduler::~RegionScheduler() {
  running_ = false;
  if (worker_.joinable()) worker_.join();
}

void RegionScheduler::persist_locked(const MigrateTask& task) {
  if (!persist_) return;
  persist_(to_persist(task));
}

void RegionScheduler::erase_persisted(const std::string& task_id) {
  if (erase_) erase_(task_id);
}

size_t RegionScheduler::inflight_count_locked() const {
  size_t n = 0;
  for (const auto& [_, t] : tasks_) {
    if (t.state == MigrateState::kPending || t.state == MigrateState::kPlacing ||
        t.state == MigrateState::kUpdatingRoute) {
      ++n;
    }
  }
  return n;
}

// 返回需要迁移的 region_id 列表（调用方再 submit）。
std::vector<uint32_t> RegionScheduler::scan_candidates() const {
  std::vector<uint32_t> out;
  auto route = state_.routing_table();
  auto nodes = state_.list_nodes();
  std::set<std::string> draining;
  std::set<std::string> offline;
  for (const auto& n : nodes) {
    if (n.role != NodeRole::kStorage) continue;
    if (n.state == NodeState::kDraining) draining.insert(n.node_id);
    if (n.state == NodeState::kOffline) offline.insert(n.node_id);
  }
  const size_t rf = state_.replica_factor();
  for (const auto& region : route.regions) {
    size_t voters = 0;
    bool on_drain = false;
    bool on_offline = false;
    for (const auto& r : region.replicas) {
      if (!r.is_learner) ++voters;
      if (draining.count(r.node_id)) on_drain = true;
      if (offline.count(r.node_id)) on_offline = true;
    }
    const bool under_replicated = voters < rf;
    if (on_drain || on_offline || under_replicated) {
      out.push_back(region.region_id);
    }
  }
  return out;
}

// Meta 重启：把快照中未终态任务灌回 tasks_ / region_to_task_。
void RegionScheduler::restore_persisted(const std::vector<PersistMigrate>& persisted) {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& pm : persisted) {
    if (pm.task_id.empty()) continue;
    const auto st = static_cast<MigrateState>(pm.state);
    if (st == MigrateState::kCompleted || st == MigrateState::kFailed ||
        st == MigrateState::kCancelled) {
      continue;
    }
    if (region_to_task_.count(pm.region_id)) continue;
    MigrateTask task;
    task.task_id = pm.task_id;
    task.region_id = pm.region_id;
    task.state = st;
    task.desired_peers = pm.desired_peers;
    task.progress = 0.1;
    // 恢复后从 Pending 重试，避免卡在半完成状态
    if (task.state == MigrateState::kPlacing || task.state == MigrateState::kUpdatingRoute) {
      task.state = MigrateState::kPending;
    }
    region_to_task_[task.region_id] = task.task_id;
    tasks_[task.task_id] = task;
    // 推进 task_seq_，避免 id 冲突
    if (task.task_id.rfind("mig-", 0) == 0) {
      try {
        uint64_t n = std::stoull(task.task_id.substr(4));
        uint64_t cur = task_seq_.load();
        while (n > cur && !task_seq_.compare_exchange_weak(cur, n)) {
        }
      } catch (...) {
      }
    }
    SKV_INFO << "migrate restore id=" << task.task_id << " region=" << task.region_id;
  }
}

// desired 空则 suggest_placement；同一 region 已有任务则返回已有（防重）。
MigrateTask RegionScheduler::submit(uint32_t region_id, std::vector<std::string> desired) {
  MigrateTask task;
  task.task_id = "mig-" + std::to_string(++task_seq_);
  task.region_id = region_id;
  task.state = MigrateState::kPending;
  if (desired.empty()) {
    desired = state_.suggest_placement({});
  }
  task.desired_peers = std::move(desired);
  if (task.desired_peers.empty()) {
    task.state = MigrateState::kFailed;
    task.error = "no_placement";
    std::lock_guard<std::mutex> lock(mu_);
    tasks_[task.task_id] = task;
    return task;
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (region_to_task_.count(region_id)) {
    task.state = MigrateState::kFailed;
    task.error = "already_migrating";
    tasks_[task.task_id] = task;
    return task;
  }
  region_to_task_[region_id] = task.task_id;
  tasks_[task.task_id] = task;
  persist_locked(task);
  SKV_INFO << "migrate submit id=" << task.task_id << " region=" << region_id
           << " peers=" << task.desired_peers.size();
  return task;
}

bool RegionScheduler::cancel(const std::string& task_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = tasks_.find(task_id);
  if (it == tasks_.end()) return false;
  if (it->second.state == MigrateState::kCompleted || it->second.state == MigrateState::kFailed) {
    return false;
  }
  it->second.state = MigrateState::kCancelled;
  region_to_task_.erase(it->second.region_id);
  erase_persisted(task_id);
  return true;
}

std::optional<MigrateTask> RegionScheduler::get_task(const std::string& task_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = tasks_.find(task_id);
  if (it == tasks_.end()) return std::nullopt;
  return it->second;
}

std::vector<MigrateTask> RegionScheduler::list_tasks() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<MigrateTask> out;
  out.reserve(tasks_.size());
  for (const auto& [_, t] : tasks_) out.push_back(t);
  return out;
}

// Leader 周期：扫候选入队 + 推进 inflight 任务。
void RegionScheduler::worker_loop() {
  while (running_) {
    const bool leader = is_leader_ && is_leader_();
    if (leader) {
      // Meta 换主：Raft 里仍有 migrates_，但本进程 tasks_ 为空 → 从快照灌回。
      if (!was_leader_) {
        restore_persisted(state_.list_migrates());
        SKV_INFO << "migrate scheduler became leader; restored from ClusterState";
      }
      maybe_enqueue_from_scan();
      std::vector<std::string> active;
      {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& [id, t] : tasks_) {
          if (t.state == MigrateState::kPending || t.state == MigrateState::kPlacing ||
              t.state == MigrateState::kUpdatingRoute) {
            active.push_back(id);
          }
        }
      }
      for (const auto& id : active) {
        advance(id);
      }
    }
    was_leader_ = leader;
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
  }
}

// 跳过已有 region_to_task_ 映射的 Region，避免重复迁移。
void RegionScheduler::maybe_enqueue_from_scan() {
  // 先处理缩容腾空 → Tombstone（经 Raft）
  if (set_tombstone_) {
    for (const auto& id : state_.list_empty_draining()) {
      set_tombstone_(id);
    }
  }

  std::vector<MigrateTask> pending_persist;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (inflight_count_locked() < max_inflight_) {
    std::vector<std::string> avoid;
    for (const auto& n : state_.list_nodes(NodeRole::kStorage)) {
      if (n.state == NodeState::kDraining || n.state == NodeState::kOffline ||
          n.state == NodeState::kTombstone || n.state == NodeState::kJoining) {
        avoid.push_back(n.node_id);
      }
    }

    auto cands = scan_candidates();
    for (uint32_t rid : cands) {
      if (inflight_count_locked() >= max_inflight_) break;
      if (region_to_task_.count(rid)) continue;
      MigrateTask task;
      task.task_id = "mig-" + std::to_string(++task_seq_);
      task.region_id = rid;
      task.state = MigrateState::kPending;
      // Replace：尽量保留健康 voter，只换掉 draining/offline 上的 peer
      task.desired_peers = state_.suggest_replace_away(rid, avoid);
      if (task.desired_peers.size() < state_.replica_factor()) {
        task.desired_peers = state_.suggest_placement({});
      }
      if (task.desired_peers.size() < state_.replica_factor()) continue;
      region_to_task_[rid] = task.task_id;
      tasks_[task.task_id] = task;
      pending_persist.push_back(task);
      SKV_INFO << "migrate auto-enqueue id=" << task.task_id << " region=" << rid;
    }

    // Balance：过载 → 轻载（RF 不变）。默认关闭 auto；显式 BALANCE / set_auto_balance(true)。
    // SCALE_OUT 进行中也必须跳过，否则与加副本抢同一空 Store，路由/Raft 会乱。
    if (auto_balance_ && inflight_count_locked() < max_inflight_) {
      if (!(pause_balance_ && pause_balance_())) {
        if (auto mv = state_.suggest_balance_move()) {
          if (!region_to_task_.count(mv->first)) {
            MigrateTask task;
            task.task_id = "mig-" + std::to_string(++task_seq_);
            task.region_id = mv->first;
            task.state = MigrateState::kPending;
            task.desired_peers = mv->second;
            region_to_task_[task.region_id] = task.task_id;
            tasks_[task.task_id] = task;
            pending_persist.push_back(task);
            SKV_INFO << "migrate balance-enqueue id=" << task.task_id
                     << " region=" << task.region_id;
          }
        }
      }
    }
    }  // inflight < max
  }
  // Raft persist 在锁外，避免与 BALANCE / place_ / apply 交叉死锁
  for (const auto& t : pending_persist) {
    persist_locked(t);
  }
}

size_t RegionScheduler::enqueue_balance(size_t max_new) {
  size_t n = 0;
  std::vector<MigrateTask> pending_persist;
  {
    // 仅短持锁入队；Raft persist 在锁外（避免与 place_ 交叉死锁）
    std::lock_guard<std::mutex> lock(mu_);
    // suggest_balance_move 每次返回同一最优 Region；已有任务时必须 break，
    // 不可 continue（否则自旋占住 mu_，堵死 worker / BALANCE / apply）。
    while (n < max_new && inflight_count_locked() < max_inflight_) {
      auto mv = state_.suggest_balance_move();
      if (!mv) break;
      if (region_to_task_.count(mv->first)) break;
      MigrateTask task;
      task.task_id = "mig-" + std::to_string(++task_seq_);
      task.region_id = mv->first;
      task.state = MigrateState::kPending;
      task.desired_peers = mv->second;
      region_to_task_[task.region_id] = task.task_id;
      tasks_[task.task_id] = task;
      pending_persist.push_back(task);
      ++n;
      SKV_INFO << "migrate balance submit id=" << task.task_id << " region=" << task.region_id;
    }
  }
  for (const auto& t : pending_persist) {
    if (persist_) persist_(to_persist(t));
  }
  return n;
}

// Placing 调 place_（锁外）；Raft persist/erase 也在锁外，避免与 BALANCE 死锁。
void RegionScheduler::advance(const std::string& task_id) {
  uint32_t region_id = 0;
  std::vector<std::string> desired;
  MigrateState st = MigrateState::kPending;
  std::optional<PersistMigrate> upsert;
  std::optional<std::string> erase_id;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tasks_.find(task_id);
    if (it == tasks_.end()) return;
    auto& task = it->second;
    if (task.state == MigrateState::kCancelled) {
      region_to_task_.erase(task.region_id);
      erase_id = task.task_id;
    } else if (task.state == MigrateState::kPending) {
      task.state = MigrateState::kPlacing;
      task.progress = 0.2;
      upsert = to_persist(task);
    } else if (task.state == MigrateState::kPlacing) {
      region_id = task.region_id;
      desired = task.desired_peers;
      st = MigrateState::kPlacing;
    } else if (task.state == MigrateState::kUpdatingRoute) {
      region_id = task.region_id;
      desired = task.desired_peers;
      st = MigrateState::kUpdatingRoute;
    } else {
      return;
    }
  }
  if (erase_id) {
    if (erase_) erase_(*erase_id);
    return;
  }
  if (upsert) {
    if (persist_) persist_(*upsert);
    return;
  }

  if (st == MigrateState::kPlacing) {
    bool ok = place_ && place_(region_id, desired);
    upsert.reset();
    erase_id.reset();
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = tasks_.find(task_id);
      if (it == tasks_.end()) return;
      auto& task = it->second;
      // 取消/终态：place_ 可能已改数据面；不再 commit，避免用陈旧 desired 覆盖路由
      if (task.state != MigrateState::kPlacing) {
        SKV_WARN << "migrate place finished but task no longer Placing id=" << task_id
                 << " state=" << static_cast<int>(task.state) << " (skip commit)";
        return;
      }
      if (!ok) {
        ++task.place_attempts;
        SKV_WARN << "migrate place data-plane fail region=" << region_id
                 << " attempts=" << task.place_attempts;
        if (task.place_attempts >= kMaxPlaceAttempts) {
          task.state = MigrateState::kFailed;
          task.error = "place_exhausted";
          region_to_task_.erase(task.region_id);
          erase_id = task.task_id;
        } else {
          upsert = to_persist(task);
        }
      } else {
        task.state = MigrateState::kUpdatingRoute;
        task.progress = 0.7;
        upsert = to_persist(task);
      }
    }
    if (erase_id && erase_) erase_(*erase_id);
    else if (upsert && persist_) persist_(*upsert);
    return;
  }

  if (st == MigrateState::kUpdatingRoute) {
    bool ok = commit_ && commit_(region_id, desired);
    erase_id.reset();
    bool completed = false;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = tasks_.find(task_id);
      if (it == tasks_.end()) return;
      auto& task = it->second;
      if (task.state != MigrateState::kUpdatingRoute) return;
      if (!ok) {
        task.state = MigrateState::kFailed;
        task.error = "commit_place_failed";
        region_to_task_.erase(task.region_id);
        erase_id = task.task_id;
      } else {
        task.state = MigrateState::kCompleted;
        task.progress = 1.0;
        region_to_task_.erase(task.region_id);
        erase_id = task.task_id;
        completed = true;
      }
    }
    if (erase_id && erase_) erase_(*erase_id);
    if (completed) {
      SKV_INFO << "migrate completed id=" << task_id << " region=" << region_id;
    }
  }
}

}  // namespace opskv::meta
