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
// 无状态计算层：Key → Region（有序 Range）→ Leader。

#pragma once

#include "opskv/region.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace opskv::compute {

struct ReplicaEndpoint {
  std::string node_id;
  std::string address;
  bool is_leader{false};
};

struct RegionRoute {
  opskv::RegionId region_id{0};
  opskv::KeyRange range;
  std::vector<ReplicaEndpoint> replicas;
};

struct RoutingTable {
  uint64_t version{0};
  uint32_t shard_count{0};  // == regions.size()
  std::unordered_map<opskv::RegionId, RegionRoute> regions;
};

class Router {
 public:
  void update(RoutingTable table);
  uint64_t version() const;

  /// Range 定位；未命中返回 0 并可能无路由（调用方应检查）。
  std::optional<opskv::RegionId> region_of(std::string_view key) const;
  /// 兼容旧名：等同 region_of，未命中返回 0。
  uint32_t shard_of(std::string_view key) const;

  std::optional<std::string> leader_addr(opskv::RegionId region_id) const;
  std::optional<std::string> leader_addr_for_key(std::string_view key) const;
  std::vector<std::string> replica_addrs(opskv::RegionId region_id) const;

  /// 单机 demo：一个覆盖全空间的 Region 0。
  void bootstrap_single(const std::string& storage_addr, uint32_t /*ignored_shard_count*/ = 1);

 private:
  mutable std::mutex mu_;
  RoutingTable table_;
  std::vector<opskv::RegionMeta> sorted_;  // 与 table_ 同步，便于二分
};

}  // namespace opskv::compute
