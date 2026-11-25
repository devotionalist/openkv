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

// @file write_batch_merge.hpp
// 职责：把多 Region 的 Apply 切片展平，便于外层一次 RocksDB WriteBatch。
// 架构：仅头文件辅助；真正合并写在 Storage KvEngine / Apply 回调中完成。

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "openkv/raft/core/types.hpp"

namespace openkv::raftstore {

/* 单个 Region 的一批待 apply 日志。 */
struct RegionApplySlice {
  uint32_t region_id{0};                // 哪个 Region
  std::vector<raft::LogEntry> entries;  // 已提交条目
};

/**
 * 将多 Region 的 Apply 切片展平为 (region_id, entry) 有序列表，供引擎合并写。
 * @param slices 各 Region 的 Apply 切片
 * @return 展平后的列表（保持切片内顺序、切片间按输入顺序）
 */
inline std::vector<std::pair<uint32_t, raft::LogEntry>> flatten_apply_slices(
    const std::vector<RegionApplySlice>& slices) {
  std::vector<std::pair<uint32_t, raft::LogEntry>> out;
  size_t n = 0;
  for (const auto& s : slices) {
    n += s.entries.size();
  }
  out.reserve(n);
  for (const auto& s : slices) {
    for (const auto& e : s.entries) {
      out.emplace_back(s.region_id, e);
    }
  }
  return out;
}

}  // namespace openkv::raftstore
