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

// @file cluster_state.cpp
// ClusterState 实现：Range bootstrap、心跳刷 Leader、Region 分裂/合并、
// 副本放置、TSO/GC、迁移持久化、MSK1 快照序列化。
// 所有公开写方法内部加 mu_；须由 Meta Raft apply 路径调用以保证多副本一致。

#include "opskv/meta/cluster_state.hpp"
#include "opskv/codec.hpp"

#include "opskv/logging.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <limits>
#include <sstream>
#include <unordered_set>

namespace opskv::meta {
namespace {

using opskv::from_hex;
using opskv::to_hex;

int64_t now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}



std::vector<std::string> split_pipe(std::string_view s) {
  std::vector<std::string> parts;
  std::string cur;
  for (char c : s) {
    if (c == '|') {
      parts.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  parts.push_back(cur);
  return parts;
}

}  // namespace

// 清空并重建：1 个全空间 Region + 前 RF 个 Storage 作 Active voter；
// 其余 Storage 记为 Joining（待加入，不参与调度，须 JOIN_NODE）。
void ClusterState::bootstrap_range(uint32_t replica_factor,
                                   const std::vector<NodeInfo>& storage_nodes) {
  std::lock_guard<std::mutex> lock(mu_);
  replica_factor_ = replica_factor == 0 ? 3 : replica_factor;
  nodes_.clear();
  size_t joining = 0;
  for (size_t i = 0; i < storage_nodes.size(); ++i) {
    auto n = storage_nodes[i];
    n.state = (i < replica_factor_) ? NodeState::kActive : NodeState::kJoining;
    if (n.state == NodeState::kJoining) ++joining;
    n.last_heartbeat_ms = now_ms();
    nodes_[n.node_id] = n;
  }
  route_.version = 1;
  route_.regions.clear();
  next_region_id_ = 1;

  RegionInfo r;
  r.region_id = 0;
  r.range.start_key.clear();
  r.range.end_key.clear();  // +inf
  for (size_t i = 0; i < storage_nodes.size() && i < replica_factor_; ++i) {
    Replica rep;
    rep.node_id = storage_nodes[i].node_id;
    rep.address = storage_nodes[i].address;
    rep.is_leader = (i == 0);
    r.replicas.push_back(rep);
  }
  route_.regions.push_back(std::move(r));
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  SKV_INFO << "cluster bootstrap RANGE regions=1 active=" << replica_factor_
           << " joining=" << joining << " rf=" << replica_factor_
           << " route_ver=" << route_.version;
}

// 插入或更新节点；保留已有 state/心跳若仅刷新地址。
std::string ClusterState::register_node(NodeInfo node) {
  std::lock_guard<std::mutex> lock(mu_);
  if (node.node_id.empty()) {
    node.node_id = (node.role == NodeRole::kCompute ? "compute-" : "storage-") +
                   std::to_string(nodes_.size());
  }
  auto it = nodes_.find(node.node_id);
  if (it != nodes_.end()) {
    it->second.address = node.address;
    it->second.role = node.role;
    it->second.last_heartbeat_ms = now_ms();
    if (it->second.state == NodeState::kOffline) {
      it->second.state = NodeState::kJoining;
    }
    // Tombstone / Draining：拒绝因 REGISTER 复活（须 ACTIVATE / 清 tomb 后重建）
    if (it->second.state == NodeState::kTombstone || it->second.state == NodeState::kDraining) {
      it->second.last_heartbeat_ms = now_ms();
      return node.node_id;
    }
    SKV_INFO << "re-register node=" << node.node_id << " addr=" << node.address
             << " state=" << static_cast<int>(it->second.state);
    return node.node_id;
  }
  node.state = NodeState::kJoining;
  node.last_heartbeat_ms = now_ms();
  nodes_[node.node_id] = node;
  SKV_INFO << "register node=" << node.node_id << " addr=" << node.address;
  return node.node_id;
}

bool ClusterState::touch_lease(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return false;
  it->second.last_heartbeat_ms = now_ms();
  if (it->second.state == NodeState::kTombstone || it->second.state == NodeState::kDraining ||
      it->second.state == NodeState::kJoining) {
    return true;  // Joining 待运维「加入」；续租不升 Active
  }
  // Offline → Active：已准入节点心跳恢复
  if (it->second.state == NodeState::kOffline) {
    it->second.state = NodeState::kActive;
  }
  return true;
}

bool ClusterState::heartbeat_changes_leaders(const std::string& node_id,
                                             const std::vector<uint32_t>& leader_regions) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (nodes_.find(node_id) == nodes_.end()) return false;
  const std::unordered_set<uint32_t> want(leader_regions.begin(), leader_regions.end());
  for (const auto& region : route_.regions) {
    for (const auto& rep : region.replicas) {
      if (rep.node_id == node_id) {
        const bool should = want.count(region.region_id) > 0;
        if (rep.is_leader != should) return true;
      } else if (want.count(region.region_id) > 0 && rep.is_leader) {
        return true;
      }
    }
  }
  return false;
}

// 刷新 last_heartbeat_ms；按 leader_regions 重刷各 Region 的 is_leader（H3）。
bool ClusterState::heartbeat(const std::string& node_id,
                             const std::vector<uint32_t>& leader_regions) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return false;
  it->second.last_heartbeat_ms = now_ms();
  if (it->second.state == NodeState::kTombstone) {
    return true;  // tomb 节点心跳忽略升 Active
  }
  // Joining：待运维 JOIN/ACTIVATE，心跳只续租，不自动升 Active
  if (it->second.state == NodeState::kOffline) {
    it->second.state = NodeState::kActive;  // 已准入节点恢复
  }
  // Draining / Joining：仍刷新 Leader 位，但保持当前态
  const std::unordered_set<uint32_t> want(leader_regions.begin(), leader_regions.end());
  bool leader_changed = false;
  for (auto& region : route_.regions) {
    for (auto& rep : region.replicas) {
      if (rep.node_id == node_id) {
        const bool should = want.count(region.region_id) > 0;
        if (rep.is_leader != should) {
          rep.is_leader = should;
          leader_changed = true;
        }
      } else if (want.count(region.region_id) > 0 && rep.is_leader) {
        // 本节点宣称该 Region Leader → 摘掉其它副本的 *L
        rep.is_leader = false;
        leader_changed = true;
      }
    }
  }
  // H3：Leader 变化 bump route_ver，便于客户端感知
  if (leader_changed) {
    ++route_.version;
    route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  }
  return true;
}

std::vector<std::string> ClusterState::list_stale_storage(int64_t ttl_ms) const {
  if (ttl_ms <= 0) ttl_ms = 10000;
  std::lock_guard<std::mutex> lock(mu_);
  const int64_t now = now_ms();
  std::vector<std::string> out;
  for (const auto& [id, node] : nodes_) {
    if (node.role != NodeRole::kStorage) continue;
    // Joining=待加入：不标 Offline（否则心跳/续租会误升 Active，JOIN 报 not_pending）
    if (node.state == NodeState::kOffline || node.state == NodeState::kDraining ||
        node.state == NodeState::kTombstone || node.state == NodeState::kJoining) {
      continue;
    }
    if (node.last_heartbeat_ms <= 0) continue;
    if (now - node.last_heartbeat_ms <= ttl_ms) continue;
    out.push_back(id);
  }
  return out;
}

// S3：运维显式改状态；Draining/Offline/Tombstone 时清该节点上的 Leader 标记。
bool ClusterState::set_node_state(const std::string& node_id, NodeState state) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return false;
  if (it->second.state == state) return true;
  it->second.state = state;
  if (state == NodeState::kDraining || state == NodeState::kOffline ||
      state == NodeState::kTombstone) {
    for (auto& region : route_.regions) {
      for (auto& rep : region.replicas) {
        if (rep.node_id == node_id) rep.is_leader = false;
      }
    }
  }
  if (state == NodeState::kActive || state == NodeState::kJoining) {
    it->second.last_heartbeat_ms = now_ms();
  }
  ++route_.version;
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  SKV_INFO << "set_node_state id=" << node_id << " state=" << static_cast<int>(state);
  return true;
}

size_t ClusterState::region_count_on(const std::string& node_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  size_t n = 0;
  for (const auto& region : route_.regions) {
    for (const auto& r : region.replicas) {
      if (r.node_id == node_id) {
        ++n;
        break;
      }
    }
  }
  return n;
}

size_t ClusterState::leader_count_on(const std::string& node_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  size_t n = 0;
  for (const auto& region : route_.regions) {
    for (const auto& r : region.replicas) {
      if (r.node_id == node_id && r.is_leader) ++n;
    }
  }
  return n;
}

std::vector<std::string> ClusterState::list_empty_draining() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> out;
  for (const auto& [id, node] : nodes_) {
    if (node.role != NodeRole::kStorage) continue;
    if (node.state != NodeState::kDraining) continue;
    size_t regions = 0, leaders = 0;
    for (const auto& region : route_.regions) {
      for (const auto& r : region.replicas) {
        if (r.node_id != id) continue;
        ++regions;
        if (r.is_leader) ++leaders;
        break;
      }
    }
    if (regions == 0 && leaders == 0) out.push_back(id);
  }
  return out;
}

bool ClusterState::erase_tombstone(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return false;
  // 踢出：禁止仍 Active；须已下线（Tombstone / Draining / Offline / Joining）且路由无副本
  if (it->second.state == NodeState::kActive) return false;
  for (const auto& region : route_.regions) {
    for (const auto& r : region.replicas) {
      if (r.node_id == node_id) return false;
    }
  }
  nodes_.erase(it);
  ++route_.version;
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  SKV_INFO << "erase_node id=" << node_id;
  return true;
}

// S2：按 task_id 覆盖写入 migrates_（随 serialize 落快照）。
void ClusterState::upsert_migrate(PersistMigrate task) {
  std::lock_guard<std::mutex> lock(mu_);
  if (task.task_id.empty()) return;
  migrates_[task.task_id] = std::move(task);
}

void ClusterState::erase_migrate(const std::string& task_id) {
  std::lock_guard<std::mutex> lock(mu_);
  migrates_.erase(task_id);
}

std::vector<PersistMigrate> ClusterState::list_migrates() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<PersistMigrate> out;
  out.reserve(migrates_.size());
  for (const auto& [_, t] : migrates_) out.push_back(t);
  return out;
}

// H2：超时未心跳 → Offline，并清 Leader；返回新下线个数。
size_t ClusterState::sweep_stale(int64_t ttl_ms) {
  if (ttl_ms <= 0) ttl_ms = 10000;
  std::lock_guard<std::mutex> lock(mu_);
  const int64_t now = now_ms();
  size_t n = 0;
  for (auto& [id, node] : nodes_) {
    if (node.role != NodeRole::kStorage) continue;
    if (node.state == NodeState::kOffline || node.state == NodeState::kDraining ||
        node.state == NodeState::kTombstone || node.state == NodeState::kJoining) {
      continue;
    }
    if (node.last_heartbeat_ms <= 0) continue;
    if (now - node.last_heartbeat_ms <= ttl_ms) continue;
    node.state = NodeState::kOffline;
    for (auto& region : route_.regions) {
      for (auto& rep : region.replicas) {
        if (rep.node_id == id) rep.is_leader = false;
      }
    }
    ++n;
    SKV_WARN << "store stale → Offline id=" << id << " last_hb_age_ms="
             << (now - node.last_heartbeat_ms);
  }
  if (n > 0) {
    ++route_.version;
    route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  }
  return n;
}

RoutingTable ClusterState::routing_table() const {
  std::lock_guard<std::mutex> lock(mu_);
  return route_;
}

std::vector<NodeInfo> ClusterState::list_nodes(std::optional<NodeRole> role) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<NodeInfo> out;
  for (const auto& [_, n] : nodes_) {
    if (!role || *role == n.role) out.push_back(n);
  }
  return out;
}

RegionInfo* ClusterState::mutable_region(opskv::RegionId id) {
  for (auto& r : route_.regions) {
    if (r.region_id == id) return &r;
  }
  return nullptr;
}

// 扩容：将 learner 挂到 Region；同 node_id 已存在则跳过不改。
bool ClusterState::set_learner(uint32_t region_id, const Replica& learner) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* region = mutable_region(region_id);
  if (!region) return false;
  for (const auto& r : region->replicas) {
    if (r.node_id == learner.node_id) return true;
  }
  Replica l = learner;
  l.is_learner = true;
  l.is_leader = false;
  region->replicas.push_back(l);
  ++route_.version;
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  return true;
}

// Learner → Voter：is_learner=false。
bool ClusterState::promote_learner(uint32_t region_id, const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* region = mutable_region(region_id);
  if (!region) return false;
  for (auto& r : region->replicas) {
    if (r.node_id == node_id) {
      r.is_learner = false;
      ++route_.version;
      route_.shard_count = static_cast<uint32_t>(route_.regions.size());
      return true;
    }
  }
  return false;
}

bool ClusterState::remove_replica(uint32_t region_id, const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* region = mutable_region(region_id);
  if (!region) return false;
  auto& reps = region->replicas;
  const auto before = reps.size();
  reps.erase(std::remove_if(reps.begin(), reps.end(),
                            [&](const Replica& r) { return r.node_id == node_id; }),
             reps.end());
  if (reps.size() < before) {
    ++route_.version;
    route_.shard_count = static_cast<uint32_t>(route_.regions.size());
    return true;
  }
  return false;
}

// PLACE 完成：整表替换副本并 ++epoch、bump route_ver。
bool ClusterState::place_replicas(uint32_t region_id, std::vector<Replica> replicas) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* region = mutable_region(region_id);
  if (!region) return false;
  if (replicas.empty()) return false;
  // 保留仍在目标集内的原 Leader；若 Leader 被摘掉则用新集首副本占位 *L（心跳会纠正）。
  // 禁止在「Leader 仍在集内」时强行改成 replicas[0]（会显示错节点）。
  std::string keep_leader;
  for (const auto& old : region->replicas) {
    if (!old.is_leader) continue;
    for (const auto& n : replicas) {
      if (n.node_id == old.node_id) {
        keep_leader = old.node_id;
        break;
      }
    }
  }
  if (keep_leader.empty() && !replicas.empty()) {
    keep_leader = replicas[0].node_id;
  }
  for (auto& r : replicas) {
    auto it = nodes_.find(r.node_id);
    if (it == nodes_.end()) return false;
    if (r.address.empty()) r.address = it->second.address;
    r.is_leader = (r.node_id == keep_leader);
    r.is_learner = false;
  }
  region->replicas = std::move(replicas);
  ++region->epoch;
  ++route_.version;
  SKV_INFO << "place region=" << region_id << " replicas=" << region->replicas.size()
           << " epoch=" << region->epoch << " route_ver=" << route_.version;
  return true;
}

void ClusterState::bump_route_version() {
  std::lock_guard<std::mutex> lock(mu_);
  ++route_.version;
}

// 左保留 [start,split)，右新建 [split,end)；右副本默认继承或用 peer_ids。
std::optional<opskv::RegionId> ClusterState::split_region(
    opskv::RegionId region_id, std::string split_key,
    const std::vector<std::string>& peer_ids) {
  std::lock_guard<std::mutex> lock(mu_);
  auto* left = mutable_region(region_id);
  if (!left) return std::nullopt;
  if (!left->range.can_split_at(split_key)) return std::nullopt;

  RegionInfo right;
  right.region_id = next_region_id_++;
  right.range.start_key = split_key;
  right.range.end_key = left->range.end_key;
  if (peer_ids.empty()) {
    right.replicas = left->replicas;
  } else {
    for (const auto& id : peer_ids) {
      auto it = nodes_.find(id);
      if (it == nodes_.end()) return std::nullopt;
      Replica rep;
      rep.node_id = id;
      rep.address = it->second.address;
      right.replicas.push_back(rep);
    }
  }
  for (auto& rep : right.replicas) {
    rep.is_leader = false;
    rep.is_learner = false;
  }

  left->range.end_key = split_key;
  ++left->epoch;
  // 与 Storage ADMIN_SPLIT 对齐：左右共享 bump 后的 epoch（勿写死 right=1）。
  right.epoch = left->epoch;
  route_.regions.push_back(std::move(right));
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  ++route_.version;

  SKV_INFO << "split region=" << region_id << " at key_len=" << split_key.size()
           << " -> right=" << (next_region_id_ - 1) << " route_ver=" << route_.version
           << " left_epoch=" << left->epoch;
  return next_region_id_ - 1;
}

// 要求相邻；删除 right，扩展 left.end，副本沿用 left。
bool ClusterState::merge_regions(opskv::RegionId left_id, opskv::RegionId right_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if (left_id == right_id) return false;
  RegionInfo* left = mutable_region(left_id);
  RegionInfo* right = mutable_region(right_id);
  if (!left || !right) return false;
  // 相邻：left.end == right.start（都为空仅当全空间单 region，不可自合并）
  if (left->range.end_key != right->range.start_key) return false;

  left->range.end_key = right->range.end_key;
  ++left->epoch;
  route_.regions.erase(
      std::remove_if(route_.regions.begin(), route_.regions.end(),
                     [&](const RegionInfo& r) { return r.region_id == right_id; }),
      route_.regions.end());
  route_.shard_count = static_cast<uint32_t>(route_.regions.size());
  ++route_.version;
  SKV_INFO << "merge regions left=" << left_id << " right=" << right_id
           << " epoch=" << left->epoch << " route_ver=" << route_.version;
  return true;
}

std::optional<NodeInfo> ClusterState::get_node(const std::string& id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = nodes_.find(id);
  if (it == nodes_.end()) return std::nullopt;
  return it->second;
}

std::optional<RegionInfo> ClusterState::find_region(opskv::RegionId id) const {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& r : route_.regions) {
    if (r.region_id == id) return r;
  }
  return std::nullopt;
}

std::optional<opskv::RegionId> ClusterState::locate(std::string_view key) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<opskv::RegionMeta> metas;
  metas.reserve(route_.regions.size());
  for (const auto& r : route_.regions) {
    metas.push_back({r.region_id, r.range});
  }
  opskv::sort_regions(metas);
  return opskv::locate_region(metas, key);
}

// 按「已持有 Region 数」升序选 RF 个 Active Storage；avoid 降权。
std::vector<std::string> ClusterState::suggest_placement(
    const std::vector<std::string>& avoid) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::unordered_map<std::string, int> load;
  std::unordered_map<std::string, std::string> addrs;
  for (const auto& [id, n] : nodes_) {
    if (n.role != NodeRole::kStorage) continue;
    // 仅 Active 可调度；Joining=待加入，不参与放置
    if (n.state != NodeState::kActive) continue;
    load[id] = 0;
    addrs[id] = n.address;
  }
  for (const auto& region : route_.regions) {
    for (const auto& r : region.replicas) {
      if (load.count(r.node_id)) ++load[r.node_id];
    }
  }
  for (const auto& a : avoid) {
    if (load.count(a)) load[a] += 1000;  // 强降权，优先换一批 Store
  }
  std::vector<std::pair<int, std::string>> ranked;
  ranked.reserve(load.size());
  for (const auto& [id, c] : load) {
    ranked.push_back({c, id});
  }
  std::sort(ranked.begin(), ranked.end());
  std::vector<std::string> out;
  for (size_t i = 0; i < ranked.size() && out.size() < replica_factor_; ++i) {
    out.push_back(ranked[i].second);
  }
  return out;
}

std::vector<std::string> ClusterState::suggest_replace_away(
    uint32_t region_id, const std::vector<std::string>& avoid) const {
  std::lock_guard<std::mutex> lock(mu_);
  const RegionInfo* region = nullptr;
  for (const auto& r : route_.regions) {
    if (r.region_id == region_id) {
      region = &r;
      break;
    }
  }
  if (!region) return {};

  std::unordered_set<std::string> avoid_set(avoid.begin(), avoid.end());
  std::vector<std::string> desired;
  for (const auto& rep : region->replicas) {
    if (rep.is_learner) continue;
    if (avoid_set.count(rep.node_id)) continue;
    auto nit = nodes_.find(rep.node_id);
    if (nit == nodes_.end() || nit->second.role != NodeRole::kStorage) continue;
    if (nit->second.state != NodeState::kActive) continue;
    desired.push_back(rep.node_id);
  }

  // 补齐到 RF：优先轻载且不在 desired / avoid（仅 Active）
  std::unordered_map<std::string, int> load;
  for (const auto& [id, n] : nodes_) {
    if (n.role != NodeRole::kStorage) continue;
    if (n.state != NodeState::kActive) continue;
    load[id] = 0;
  }
  for (const auto& reg : route_.regions) {
    for (const auto& r : reg.replicas) {
      if (load.count(r.node_id)) ++load[r.node_id];
    }
  }
  std::vector<std::pair<int, std::string>> ranked;
  for (const auto& [id, c] : load) {
    bool skip = avoid_set.count(id) > 0;
    for (const auto& d : desired)
      if (d == id) skip = true;
    if (skip) continue;
    ranked.push_back({c, id});
  }
  std::sort(ranked.begin(), ranked.end());
  for (const auto& [_, id] : ranked) {
    if (desired.size() >= replica_factor_) break;
    desired.push_back(id);
  }
  if (desired.size() < replica_factor_) return {};
  if (desired.size() > replica_factor_) desired.resize(replica_factor_);
  return desired;
}

std::optional<std::pair<uint32_t, std::vector<std::string>>> ClusterState::suggest_balance_move()
    const {
  std::lock_guard<std::mutex> lock(mu_);
  std::unordered_map<std::string, int> load;
  std::vector<std::string> active;
  for (const auto& [id, n] : nodes_) {
    if (n.role != NodeRole::kStorage) continue;
    if (n.state != NodeState::kActive) continue;
    load[id] = 0;
    active.push_back(id);
  }
  if (active.size() <= replica_factor_) return std::nullopt;

  for (const auto& reg : route_.regions) {
    for (const auto& r : reg.replicas) {
      if (load.count(r.node_id)) ++load[r.node_id];
    }
  }
  std::string heavy, light;
  int heavy_n = -1, light_n = INT_MAX;
  for (const auto& [id, c] : load) {
    if (c > heavy_n) {
      heavy_n = c;
      heavy = id;
    }
    if (c < light_n) {
      light_n = c;
      light = id;
    }
  }
  // 过载差至少 2，避免抖动
  if (heavy.empty() || light.empty() || heavy == light || heavy_n - light_n < 2) {
    return std::nullopt;
  }

  for (const auto& reg : route_.regions) {
    bool on_heavy = false;
    bool on_light = false;
    std::vector<std::string> voters;
    for (const auto& r : reg.replicas) {
      if (r.is_learner) continue;
      voters.push_back(r.node_id);
      if (r.node_id == heavy) on_heavy = true;
      if (r.node_id == light) on_light = true;
    }
    if (!on_heavy || on_light) continue;
    // desired = voters - heavy + light
    std::vector<std::string> desired;
    for (const auto& id : voters) {
      if (id == heavy) continue;
      desired.push_back(id);
    }
    desired.push_back(light);
    if (desired.size() != replica_factor_) continue;
    return std::make_pair(reg.region_id, desired);
  }
  return std::nullopt;
}

std::optional<opskv::RegionId> ClusterState::adjacent_right(
    opskv::RegionId left_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const RegionInfo* left = nullptr;
  for (const auto& r : route_.regions) {
    if (r.region_id == left_id) {
      left = &r;
      break;
    }
  }
  if (!left) return std::nullopt;
  for (const auto& r : route_.regions) {
    if (r.region_id == left_id) continue;
    if (left->range.end_key == r.range.start_key && !left->range.end_key.empty()) {
      return r.region_id;
    }
  }
  return std::nullopt;
}

uint32_t ClusterState::replica_factor() const {
  std::lock_guard<std::mutex> lock(mu_);
  return replica_factor_;
}

// 返回 [start, start+count)；next_ts_ 前进 count。
uint64_t ClusterState::alloc_ts(uint32_t count) {
  std::lock_guard<std::mutex> lock(mu_);
  if (count == 0) count = 1;
  if (count > 10000) count = 10000;  // T1：单次批上限，防误配打爆
  // Meta 硬重启丢快照时 next_ts_ 可能回退到 1，而 Storage MVCC 仍留有更高
  // commit_ts → 全面 write_conflict。用墙钟 ms 作下界（只升不降）。
  const uint64_t wall = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  if (next_ts_ < wall) next_ts_ = wall;
  const uint64_t start = next_ts_;
  next_ts_ += count;
  return start;
}

uint64_t ClusterState::next_ts() const {
  std::lock_guard<std::mutex> lock(mu_);
  return next_ts_;
}

uint64_t ClusterState::gc_safe_ts() const {
  std::lock_guard<std::mutex> lock(mu_);
  return gc_safe_ts_;
}

// T2：写入 reporters_ 后重算全局 min_start_ts_hint_。
void ClusterState::report_min_start_ts(uint64_t min_start_ts, const std::string& reporter) {
  std::lock_guard<std::mutex> lock(mu_);
  const std::string key = reporter.empty() ? "_" : reporter;
  min_start_reporters_[key] = min_start_ts;
  uint64_t agg = std::numeric_limits<uint64_t>::max();
  for (const auto& [_, v] : min_start_reporters_) {
    if (v < agg) agg = v;
  }
  min_start_ts_hint_ = agg;
}

uint64_t ClusterState::min_start_ts_hint() const {
  std::lock_guard<std::mutex> lock(mu_);
  return min_start_ts_hint_;
}

// safe = min(next_ts - retain, min_start_hint)；只升不降。
uint64_t ClusterState::advance_gc_safe_ts(uint64_t retain_ts) {
  std::lock_guard<std::mutex> lock(mu_);
  if (retain_ts == 0) retain_ts = gc_retain_ts_;
  // candidate：落后于 TSO 的水位，保证「很旧」的快照才被 GC
  uint64_t candidate = 0;
  if (next_ts_ > retain_ts) {
    candidate = next_ts_ - retain_ts;
  }
  // 不能超过活跃事务最小 start_ts（否则进行中事务读不到需要的版本）
  if (min_start_ts_hint_ != std::numeric_limits<uint64_t>::max() &&
      candidate > min_start_ts_hint_) {
    candidate = min_start_ts_hint_;
  }
  // 不能超过 next_ts（尚未发出的 ts）
  if (candidate >= next_ts_) {
    candidate = next_ts_ > 0 ? next_ts_ - 1 : 0;
  }
  if (candidate > gc_safe_ts_) {
    gc_safe_ts_ = candidate;
  }
  return gc_safe_ts_;
}

void ClusterState::set_gc_retain_ts(uint64_t retain) {
  std::lock_guard<std::mutex> lock(mu_);
  gc_retain_ts_ = retain == 0 ? 1 : retain;
}

uint64_t ClusterState::gc_retain_ts() const {
  std::lock_guard<std::mutex> lock(mu_);
  return gc_retain_ts_;
}

// 快照格式（文本，字段用 | 分隔；key 用 hex）：
// MSK1|rf|next_region|next_ts|gc_safe|min_start|gc_retain|route_ver
//   |n_nodes|(id|role|addr|state|hb)*
//   |n_regions|(rid|start_hex|end_hex|epoch|nrep|(nid|addr|L|N)*)*
// MSK1 文本快照：节点 | 路由 | TSO/GC | 迁移任务。
std::string ClusterState::serialize() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::ostringstream oss;
  oss << "MSK1|" << replica_factor_ << '|' << next_region_id_ << '|' << next_ts_ << '|'
      << gc_safe_ts_ << '|' << min_start_ts_hint_ << '|' << gc_retain_ts_ << '|' << route_.version
      << '|' << nodes_.size();
  for (const auto& [_, n] : nodes_) {
    oss << '|' << n.node_id << '|' << static_cast<int>(n.role) << '|' << n.address << '|'
        << static_cast<int>(n.state) << '|' << n.last_heartbeat_ms;
  }
  oss << '|' << route_.regions.size();
  for (const auto& r : route_.regions) {
    oss << '|' << r.region_id << '|' << to_hex(r.range.start_key) << '|' << to_hex(r.range.end_key)
        << '|' << r.epoch << '|' << r.replicas.size();
    for (const auto& rep : r.replicas) {
      oss << '|' << rep.node_id << '|' << rep.address << '|' << (rep.is_leader ? 1 : 0) << '|'
          << (rep.is_learner ? 1 : 0);
    }
  }
  // S2：尾部可选迁移任务（旧 MSK1 无此段也能 load）
  oss << '|' << migrates_.size();
  for (const auto& [_, m] : migrates_) {
    oss << '|' << m.task_id << '|' << m.region_id << '|' << m.state << '|' << m.desired_peers.size();
    for (const auto& p : m.desired_peers) oss << '|' << p;
  }
  return oss.str();
}

// 解析 MSK1；失败则状态不变（调用方应处理 false）。
bool ClusterState::load_snapshot(std::string_view data) {
  if (data.size() < 4 || data.substr(0, 4) != "MSK1") {
    SKV_WARN << "cluster_state snapshot bad magic";
    return false;
  }
  auto parts = split_pipe(data);
  // MSK1 rf next_region next_ts gc_safe min_start gc_retain route_ver n_nodes ...
  if (parts.size() < 9) return false;
  try {
    size_t i = 1;
    uint32_t rf = static_cast<uint32_t>(std::stoul(parts[i++]));
    RegionId next_rid = static_cast<RegionId>(std::stoul(parts[i++]));
    uint64_t next_ts = std::stoull(parts[i++]);
    uint64_t gc_safe = std::stoull(parts[i++]);
    uint64_t min_start = std::stoull(parts[i++]);
    uint64_t gc_retain = std::stoull(parts[i++]);
    uint64_t route_ver = std::stoull(parts[i++]);
    size_t n_nodes = static_cast<size_t>(std::stoull(parts[i++]));

    std::unordered_map<std::string, NodeInfo> nodes;
    for (size_t n = 0; n < n_nodes; ++n) {
      if (i + 5 > parts.size()) return false;
      NodeInfo info;
      info.node_id = parts[i++];
      info.role = static_cast<NodeRole>(std::stoi(parts[i++]));
      info.address = parts[i++];
      info.state = static_cast<NodeState>(std::stoi(parts[i++]));
      info.last_heartbeat_ms = std::stoll(parts[i++]);
      nodes[info.node_id] = std::move(info);
    }
    if (i >= parts.size()) return false;
    size_t n_regions = static_cast<size_t>(std::stoull(parts[i++]));
    std::vector<RegionInfo> regions;
    regions.reserve(n_regions);
    for (size_t r = 0; r < n_regions; ++r) {
      if (i + 5 > parts.size()) return false;
      RegionInfo info;
      info.region_id = static_cast<RegionId>(std::stoul(parts[i++]));
      if (!from_hex(parts[i++], &info.range.start_key)) return false;
      if (!from_hex(parts[i++], &info.range.end_key)) return false;
      info.epoch = std::stoull(parts[i++]);
      size_t nrep = static_cast<size_t>(std::stoull(parts[i++]));
      for (size_t k = 0; k < nrep; ++k) {
        if (i + 4 > parts.size()) return false;
        Replica rep;
        rep.node_id = parts[i++];
        rep.address = parts[i++];
        rep.is_leader = parts[i++] != "0";
        rep.is_learner = parts[i++] != "0";
        info.replicas.push_back(std::move(rep));
      }
      regions.push_back(std::move(info));
    }

    std::unordered_map<std::string, PersistMigrate> migrates;
    if (i < parts.size()) {
      size_t n_mig = static_cast<size_t>(std::stoull(parts[i++]));
      for (size_t m = 0; m < n_mig; ++m) {
        if (i + 4 > parts.size()) return false;
        PersistMigrate pm;
        pm.task_id = parts[i++];
        pm.region_id = static_cast<uint32_t>(std::stoul(parts[i++]));
        pm.state = std::stoi(parts[i++]);
        size_t np = static_cast<size_t>(std::stoull(parts[i++]));
        for (size_t p = 0; p < np; ++p) {
          if (i >= parts.size()) return false;
          pm.desired_peers.push_back(parts[i++]);
        }
        migrates[pm.task_id] = std::move(pm);
      }
    }

    std::lock_guard<std::mutex> lock(mu_);
    replica_factor_ = rf == 0 ? 3 : rf;
    next_region_id_ = next_rid;
    next_ts_ = next_ts == 0 ? 1 : next_ts;
    gc_safe_ts_ = gc_safe;
    min_start_ts_hint_ = min_start;
    gc_retain_ts_ = gc_retain == 0 ? 1 : gc_retain;
    nodes_ = std::move(nodes);
    route_.version = route_ver;
    route_.regions = std::move(regions);
    route_.shard_count = static_cast<uint32_t>(route_.regions.size());
    migrates_ = std::move(migrates);
    SKV_INFO << "cluster_state snapshot loaded nodes=" << nodes_.size()
             << " regions=" << route_.regions.size() << " next_ts=" << next_ts_
             << " route_ver=" << route_.version << " migrates=" << migrates_.size();
    return true;
  } catch (...) {
    SKV_WARN << "cluster_state snapshot parse failed";
    return false;
  }
}

}  // namespace opskv::meta
