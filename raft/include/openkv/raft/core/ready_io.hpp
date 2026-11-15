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

// @file ready_io.hpp
// Ready 缓冲：HardState / unstable / committed / snaps（不含锁，由 RaftNode::mu_ 保护）。

#pragma once

#include "openkv/raft/core/ready.hpp"

#include <cstdint>
#include <vector>

namespace openkv::raft {

/// external_io 下协议核攒批、等 Store take_ready 外提的缓冲区。
struct ReadyIo {
  void clear_batch() {
    ready_hs_dirty_ = false;
    ready_unstable_.clear();
    ready_committed_.clear();
    ready_snaps_.clear();
  }

  bool empty() const {
    return !ready_hs_dirty_ && ready_unstable_.empty() && ready_committed_.empty() &&
           ready_snaps_.empty();
  }

  bool ready_hs_dirty_{false};            ///< HardState 有变更、尚未放进 Ready
  HardState ready_hs_{};                  ///< 待外提的最新 HardState 快照
  std::vector<LogEntry> ready_unstable_;  ///< 新 append、待写 WAL 的条目
  std::vector<LogEntry> ready_committed_; ///< 已 commit、待 apply 的条目
  std::vector<SnapshotMeta> ready_snaps_; ///< 待发/待装快照元数据
  uint64_t ready_applied_watermark_{0};   ///< Ready 侧已报告的 apply 水位
};

}  // namespace openkv::raft
