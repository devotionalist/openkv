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

// @file router.hpp
// 无状态计算层：Key → Region（有序 Range）→ Leader / replicas。
//
// update 仅接受 version ≥ 当前；epoch 可由 bump_epoch 被 Storage 回包抬高。
// shard_of 为历史别名，语义 = region_of（未命中返回 0）。

#pragma once

#include "openkv/region.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace openkv::compute {

/// 单个副本端点（节点 ID + 地址 + 是否 leader）。
struct ReplicaEndpoint {
  std::string node_id;
  std::string address;
  bool is_leader{false};
};

/// 一个 Region 的路由条目：range、epoch、副本列表。
struct RegionRoute {
  openkv::RegionId region_id{0};
  openkv::KeyRange range;
  uint64_t epoch{1};
  std::vector<ReplicaEndpoint> replicas;
};

/// Meta 下发的整表：version + 全部 RegionRoute。
struct RoutingTable {
  uint64_t version{0};
  uint32_t shard_count{0};  // == regions.size()
  std::unordered_map<openkv::RegionId, RegionRoute> regions;
};

class Router {
 public:
  /// 用新表替换本地缓存；仅当 table.version ≥ 当前 version 才生效。
  void update(RoutingTable table);
  /// 当前已接受的路由表 version。
  uint64_t version() const;

  /// 按 key 做 Range 二分定位 Region；未命中返回 nullopt。
  std::optional<openkv::RegionId> region_of(std::string_view key) const;
  /// 兼容旧名：等同 region_of，未命中返回 0。
  uint32_t shard_of(std::string_view key) const;

  /// 返回该 Region 当前标记为 leader 的地址；无则 nullopt。
  std::optional<std::string> leader_addr(openkv::RegionId region_id) const;
  /// key → region → leader 地址；任一步失败返回 nullopt。
  std::optional<std::string> leader_addr_for_key(std::string_view key) const;
  /// 副本地址列表；leader 排在最前（Gateway try_order 用）。
  std::vector<std::string> replica_addrs(openkv::RegionId region_id) const;

  /// Region conf epoch；未知 Region 返回 0。
  uint64_t epoch(openkv::RegionId region_id) const;
  /// 将本地缓存 epoch 抬到至少 to（Storage 返回 expected= 时用）。
  void bump_epoch(openkv::RegionId region_id, uint64_t to);

  /// 拷贝当前整表快照（供 sync_epochs_async 等）。
  RoutingTable snapshot() const;

  /// 单机 demo：建一个覆盖全 key 空间的 Region 0，指向 storage_addr。
  void bootstrap_single(const std::string& storage_addr, uint32_t /*ignored_shard_count*/ = 1);

 private:
  mutable std::mutex mu_;
  RoutingTable table_;
  std::vector<openkv::RegionMeta> sorted_;  // 与 table_ 同步，便于二分
};

}  // namespace openkv::compute
