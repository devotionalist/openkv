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
// Phase 3：跨 Region Apply 条目合并缓冲（外层一次 RocksDB WriteBatch）。

#pragma once

#include "opskv/raft/types.hpp"

#include <cstdint>
#include <utility>
#include <vector>

namespace opskv::raftstore {

struct RegionApplySlice {
  uint32_t region_id{0};
  std::vector<raft::LogEntry> entries;
};

/// 将多 Region 的 ApplyMsg 展平为有序列表，供引擎合并写。
inline std::vector<std::pair<uint32_t, raft::LogEntry>> flatten_apply_slices(
    const std::vector<RegionApplySlice>& slices) {
  std::vector<std::pair<uint32_t, raft::LogEntry>> out;
  size_t n = 0;
  for (const auto& s : slices) n += s.entries.size();
  out.reserve(n);
  for (const auto& s : slices) {
    for (const auto& e : s.entries) out.emplace_back(s.region_id, e);
  }
  return out;
}

}  // namespace opskv::raftstore
