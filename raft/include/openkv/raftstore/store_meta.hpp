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

// @file store_meta.hpp
// 职责：Store 级 Region 本地元数据（range / epoch / leader / destroyed）。
// 架构：内存表；与 Meta 集群路由互补，供本机查找与运维展示。
// 协作：Store::update_peer_meta / split-merge 路径更新。

#pragma once

#include "openkv/region.hpp"

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace openkv::raftstore {

struct RegionLocalState {
  uint32_t region_id{0};
  openkv::KeyRange range;
  uint64_t epoch{0};
  std::string leader_id;
  bool destroyed{false};
};

class StoreMeta {
 public:
  void upsert(RegionLocalState st);
  void erase(uint32_t region_id);
  std::optional<RegionLocalState> get(uint32_t region_id) const;
  void set_leader(uint32_t region_id, std::string leader);
  void set_epoch(uint32_t region_id, uint64_t epoch);
  void set_range(uint32_t region_id, openkv::KeyRange range);
  std::vector<uint32_t> all_regions() const;
  std::vector<uint32_t> leader_regions(const std::string& self_id) const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<uint32_t, RegionLocalState> regions_;
};

}  // namespace openkv::raftstore
