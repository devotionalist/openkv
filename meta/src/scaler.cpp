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

// @file scaler.cpp
// Scaler 实现：后台 worker 每 ~500ms 扫描未完成 ScaleTask（FIFO 串行）。
// 状态机：Pending → AddingLearner → CatchingUp → PromotingVoter → UpdatingRoute → Completed。
// 数据面经 add_learner_/catch_up_/promote_ 钩子打到 Storage；Meta 状态经钩子内 replicate。

#include "opskv/meta/scaler.hpp"

#include "opskv/logging.hpp"

#include <algorithm>
#include <chrono>

namespace opskv::meta {

Scaler::Scaler(ClusterState& state, AddLearnerFn add_learner, PromoteFn promote, BumpRouteFn bump,
               CatchUpFn catch_up, std::function<bool()> is_leader)
    : state_(state),
      add_learner_(std::move(add_learner)),
      promote_(std::move(promote)),
      bump_(std::move(bump)),
      catch_up_(std::move(catch_up)),
      is_leader_(std::move(is_leader)) {
  running_ = true;
  worker_ = std::thread([this] { worker_loop(); });
}

Scaler::~Scaler() {
  running_ = false;
  if (worker_.joinable()) {
    worker_.join();
  }
}

bool Scaler::overlaps_inflight_locked(const std::string& node_id,
                                      const std::vector<uint32_t>& shard_ids) const {
  for (const auto& tid : queue_) {
    auto it = tasks_.find(tid);
    if (it == tasks_.end()) continue;
    const auto& t = it->second;
    if (t.state == ScaleTaskState::kCompleted || t.state == ScaleTaskState::kFailed) continue;
    if (t.new_node_id != node_id) continue;
    if (shard_ids.empty() || t.shard_ids.empty()) return true;
    for (uint32_t a : shard_ids) {
      for (uint32_t b : t.shard_ids) {
        if (a == b) return true;
      }
    }
  }
  return false;
}

// 拒绝 Offline/Draining；shard_ids 空则扩全部 Region。忙时入队排队，不 Failed。
ScaleTask Scaler::start_scale_out(const std::string& new_storage_node_id,
                                  std::vector<uint32_t> shard_ids) {
  ScaleTask task;
  task.task_id = "scale-" + std::to_string(++task_seq_);
  task.new_node_id = new_storage_node_id;
  task.state = ScaleTaskState::kPending;

  auto node = state_.get_node(new_storage_node_id);
  if (!node) {
    task.state = ScaleTaskState::kFailed;
    task.error = "unknown_node";
    std::lock_guard<std::mutex> lock(mu_);
    tasks_[task.task_id] = task;
    return task;
  }
  if (node->state != NodeState::kActive) {
    task.state = ScaleTaskState::kFailed;
    task.error = "node_not_active";  // Joining 须先 JOIN/ACTIVATE
    std::lock_guard<std::mutex> lock(mu_);
    tasks_[task.task_id] = task;
    return task;
  }

  if (shard_ids.empty()) {
    auto route = state_.routing_table();
    for (const auto& s : route.regions) {
      shard_ids.push_back(s.region_id);
    }
  }
  task.shard_ids = std::move(shard_ids);

  std::lock_guard<std::mutex> lock(mu_);
  // 同 node+Region 已在排队/推进：拒绝重复，避免双 Learner ConfChange。
  if (overlaps_inflight_locked(task.new_node_id, task.shard_ids)) {
    task.state = ScaleTaskState::kFailed;
    task.error = "duplicate_inflight";
    tasks_[task.task_id] = task;
    return task;
  }

  SKV_INFO << "scale-out task=" << task.task_id << " node=" << new_storage_node_id
           << " shards=" << task.shard_ids.size()
           << (queue_.empty() ? "" : " queued_behind=" + std::to_string(queue_.size()));
  tasks_[task.task_id] = task;
  queue_.push_back(task.task_id);
  return task;
}

std::optional<ScaleTask> Scaler::get_task(const std::string& task_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = tasks_.find(task_id);
  if (it == tasks_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<ScaleTask> Scaler::list_tasks() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<ScaleTask> out;
  out.reserve(tasks_.size());
  for (const auto& [_, t] : tasks_) out.push_back(t);
  return out;
}

bool Scaler::has_inflight() const {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& [_, t] : tasks_) {
    if (t.state != ScaleTaskState::kCompleted && t.state != ScaleTaskState::kFailed) {
      return true;
    }
  }
  return false;
}

int Scaler::cancel_inflight(const std::string& node_id, std::optional<uint32_t> shard_id) {
  std::lock_guard<std::mutex> lock(mu_);
  int n = 0;
  for (auto& [id, t] : tasks_) {
    if (t.state == ScaleTaskState::kCompleted || t.state == ScaleTaskState::kFailed) continue;
    if (!node_id.empty() && t.new_node_id != node_id) continue;
    if (shard_id) {
      bool hit = t.shard_ids.empty();
      for (uint32_t s : t.shard_ids) {
        if (s == *shard_id) {
          hit = true;
          break;
        }
      }
      if (!hit) continue;
    }
    t.state = ScaleTaskState::kFailed;
    t.error = "cancelled";
    t.progress = t.progress;
    ++n;
    SKV_INFO << "scale-out cancelled task=" << id << " node=" << t.new_node_id;
  }
  queue_.erase(std::remove_if(queue_.begin(), queue_.end(),
                               [&](const std::string& tid) {
                                 auto it = tasks_.find(tid);
                                 if (it == tasks_.end()) return true;
                                 return it->second.state == ScaleTaskState::kCompleted ||
                                        it->second.state == ScaleTaskState::kFailed;
                               }),
              queue_.end());
  return n;
}

// 仅 Leader（若配置了 is_leader_）推进；每次只推进 queue_ 队首，串行避 Joint。
void Scaler::worker_loop() {
  while (running_) {
    if (is_leader_ && !is_leader_()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }
    std::string head;
    {
      std::lock_guard<std::mutex> lock(mu_);
      while (!queue_.empty()) {
        const auto& tid = queue_.front();
        auto it = tasks_.find(tid);
        if (it == tasks_.end() || it->second.state == ScaleTaskState::kCompleted ||
            it->second.state == ScaleTaskState::kFailed) {
          queue_.erase(queue_.begin());
          continue;
        }
        head = tid;
        break;
      }
      if (!head.empty()) {
        advance(tasks_[head]);
        auto it = tasks_.find(head);
        if (it != tasks_.end() && (it->second.state == ScaleTaskState::kCompleted ||
                                   it->second.state == ScaleTaskState::kFailed)) {
          if (!queue_.empty() && queue_.front() == head) queue_.erase(queue_.begin());
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

// 单步推进：每阶段对 shard_ids 循环调用对应钩子；失败写 error→kFailed。
void Scaler::advance(ScaleTask& task) {
  if (task.state == ScaleTaskState::kFailed || task.state == ScaleTaskState::kCompleted) return;
  auto node = state_.get_node(task.new_node_id);
  if (!node) {
    task.state = ScaleTaskState::kFailed;
    task.error = "node_gone";
    return;
  }

  switch (task.state) {
    case ScaleTaskState::kPending:
      task.state = ScaleTaskState::kAddingLearner;
      task.progress = 0.1;
      break;
    case ScaleTaskState::kAddingLearner: {
      Replica learner;
      learner.node_id = node->node_id;
      learner.address = node->address;
      learner.is_learner = true;
      size_t ok = 0;
      for (uint32_t sid : task.shard_ids) {
        if (add_learner_ && add_learner_(sid, learner)) {
          ++ok;
        } else if (!add_learner_) {
          if (state_.set_learner(sid, learner)) {
            ++ok;
          }
        }
      }
      if (ok == 0) {
        task.state = ScaleTaskState::kFailed;
        task.error = "add_learner_failed";
        return;
      }
      task.state = ScaleTaskState::kCatchingUp;
      task.progress = 0.4;
      break;
    }
    case ScaleTaskState::kCatchingUp: {
      // S1：有 CatchUpFn 时轮询直到全部追平；否则立即推进（单测 / Meta-only）
      if (catch_up_) {
        size_t ready = 0;
        for (uint32_t sid : task.shard_ids) {
          if (catch_up_(sid, task.new_node_id)) ++ready;
        }
        task.progress = 0.4 + 0.3 * (static_cast<double>(ready) /
                                     static_cast<double>(std::max<size_t>(1, task.shard_ids.size())));
        if (ready < task.shard_ids.size()) {
          return;  // 继续等
        }
      }
      task.state = ScaleTaskState::kPromotingVoter;
      task.progress = 0.7;
      break;
    }
    case ScaleTaskState::kPromotingVoter: {
      size_t ok = 0;
      for (uint32_t sid : task.shard_ids) {
        if (promote_) {
          if (promote_(sid, task.new_node_id)) ++ok;
        } else if (state_.promote_learner(sid, task.new_node_id)) {
          ++ok;
        }
      }
      if (ok == 0 && !task.shard_ids.empty()) {
        task.state = ScaleTaskState::kFailed;
        task.error = "promote_failed";
        return;
      }
      task.state = ScaleTaskState::kUpdatingRoute;
      task.progress = 0.9;
      break;
    }
    case ScaleTaskState::kUpdatingRoute:
      if (bump_) {
        if (!bump_()) {
          task.state = ScaleTaskState::kFailed;
          task.error = "bump_route_failed";
          return;
        }
      } else {
        state_.bump_route_version();
      }
      task.state = ScaleTaskState::kCompleted;
      task.progress = 1.0;
      SKV_INFO << "scale-out completed task=" << task.task_id
               << " route_ver=" << state_.routing_table().version;
      break;
    default:
      break;
  }
}

}  // namespace opskv::meta
