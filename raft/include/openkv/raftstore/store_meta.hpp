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

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "openkv/region.hpp"

namespace openkv::raftstore {

/* 本机视角的单个 Region 状态（非集群权威源）。 */
struct RegionLocalState {
  uint32_t region_id{0};   // Region id
  openkv::KeyRange range;  // key 范围
  uint64_t epoch{0};       // 路由纪元；拆合递增
  std::string leader_id;   // 已知 Leader peer id
  bool destroyed{false};   // 已销毁，勿再服务
};

/* Store 内 Region 元数据表：upsert / 查 / 改 leader·epoch·range。 */
class StoreMeta {
 public:
  /**
   * 插入或覆盖整条本地状态。
   * @param st 完整 RegionLocalState
   */
  void upsert(RegionLocalState st);
  /**
   * 删除一条本地记录。
   * @param region_id Region id
   */
  void erase(uint32_t region_id);
  /**
   * 按 id 查询；不存在返回 nullopt。
   * @param region_id Region id
   * @return 状态副本或空
   */
  std::optional<RegionLocalState> get(uint32_t region_id) const;
  /**
   * 只更新已知 Leader。
   * @param region_id Region id
   * @param leader Leader peer id
   */
  void set_leader(uint32_t region_id, std::string leader);
  /**
   * 只更新 epoch。
   * @param region_id Region id
   * @param epoch 新纪元
   */
  void set_epoch(uint32_t region_id, uint64_t epoch);
  /**
   * 只更新 key 范围。
   * @param region_id Region id
   * @param range 新范围
   */
  void set_range(uint32_t region_id, openkv::KeyRange range);
  /**
   * 全部已登记 Region id。
   * @return id 列表
   */
  std::vector<uint32_t> all_regions() const;
  /**
   * leader_id 等于 self_id 的 Region（本机当家）。
   * @param self_id 本节点 peer id
   * @return region_id 列表
   */
  std::vector<uint32_t> leader_regions(const std::string& self_id) const;

 private:
  mutable std::mutex mu_;                                   // 保护 regions_
  std::unordered_map<uint32_t, RegionLocalState> regions_;  // region → 本地状态
};

}  // namespace openkv::raftstore
