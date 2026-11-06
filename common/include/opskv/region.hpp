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

// @file region.hpp
// Range 分片：有序 key 空间上的半开区间 [start, end)。
//
// 约定：
//   - start 为空串：从 key 空间起点
//   - end 为空串：+∞（无上界）
//   - 定位：start <= key < end（end 空则仅检查 start）

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace opskv {

using RegionId = uint32_t;

struct KeyRange {
  std::string start_key;  // inclusive
  std::string end_key;    // exclusive; empty = +inf

  bool contains(std::string_view key) const {
    if (!start_key.empty() && key < start_key) return false;
    if (!end_key.empty() && key >= end_key) return false;
    return true;
  }

  /// 是否可在 split_key 处分裂（split_key 必须严格落在开区间内）。
  bool can_split_at(std::string_view split_key) const {
    if (split_key.empty()) return false;
    if (!start_key.empty() && split_key <= start_key) return false;
    if (!end_key.empty() && split_key >= end_key) return false;
    return true;
  }
};

struct RegionMeta {
  RegionId id{0};
  KeyRange range;
};

/// 在按 start_key 升序排列的 region 列表中二分定位；未命中返回 nullopt。
inline std::optional<RegionId> locate_region(const std::vector<RegionMeta>& regions,
                                             std::string_view key) {
  if (regions.empty()) return std::nullopt;
  // regions 应按 start_key 排序；用 upper_bound 找最后一个 start <= key
  auto it = std::upper_bound(
      regions.begin(), regions.end(), key,
      [](std::string_view k, const RegionMeta& r) { return k < r.range.start_key; });
  if (it != regions.begin()) --it;
  else if (!regions.front().range.start_key.empty() && key < regions.front().range.start_key) {
    return std::nullopt;
  }
  if (it->range.contains(key)) return it->id;
  // 兜底线性（乱序表）
  for (const auto& r : regions) {
    if (r.range.contains(key)) return r.id;
  }
  return std::nullopt;
}

inline void sort_regions(std::vector<RegionMeta>& regions) {
  std::sort(regions.begin(), regions.end(),
            [](const RegionMeta& a, const RegionMeta& b) {
              return a.range.start_key < b.range.start_key;
            });
}

}  // namespace opskv
