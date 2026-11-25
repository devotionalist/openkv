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

// @file progress_tracker.hpp
// Leader 对各 peer 的复制进度：next/match/inflight（不含锁，由 RaftNode::mu_ 保护）。

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace openkv::raft {

/// 跟踪每个 peer 的复制水位与在途消息配额。
class ProgressTracker {
 public:
  // 清除所有进度信息
  void clear() {
    next_index_.clear();
    match_index_.clear();
    inflight_.clear();
    snap_inflight_.clear();
    probe_inflight_.clear();
    peer_send_blocked_.clear();
  }

  // 初始化领导者进度信息
  void init_leader(const std::string& peer_id, uint64_t last_index) {
    next_index_[peer_id] = last_index + 1;
    match_index_[peer_id] = 0;
    inflight_[peer_id] = 0;
  }

  // 删除指定peer的进度信息
  void erase(const std::string& peer_id) {
    next_index_.erase(peer_id);
    match_index_.erase(peer_id);
    inflight_.erase(peer_id);
    snap_inflight_.erase(peer_id);
    probe_inflight_.erase(peer_id);
    peer_send_blocked_.erase(peer_id);
  }

  // 获取指定peer的匹配索引
  uint64_t match_of(const std::string& peer_id) const {
    auto it = match_index_.find(peer_id);
    return it == match_index_.end() ? 0 : it->second;
  }

  // 尝试增加指定peer的未确认消息数
  bool try_inc_inflight(const std::string& peer_id, uint32_t max_inflight) {
    auto& n = inflight_[peer_id];
    if (n >= max_inflight) return false;
    ++n;
    return true;
  }

  // 减少指定peer的未确认消息数
  void dec_inflight(const std::string& peer_id) {
    auto it = inflight_.find(peer_id);
    if (it == inflight_.end() || it->second == 0) return;
    --it->second;
  }

  std::unordered_map<std::string, uint64_t> next_index_;   // next索引
  std::unordered_map<std::string, uint64_t> match_index_;  // 匹配索引
  std::unordered_map<std::string, uint32_t> inflight_;     // 未确认消息数
  std::unordered_set<std::string> snap_inflight_;          // 快照未确认消息集合
  std::unordered_set<std::string> probe_inflight_;         // 探测未确认消息集合
  std::unordered_set<std::string> peer_send_blocked_;      // peer发送阻塞集合
};

}  // namespace openkv::raft
