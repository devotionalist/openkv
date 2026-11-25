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

/* 跟踪每个 peer 的复制水位与在途消息配额。 */
class ProgressTracker {
 public:
  /**
   * 清空全部 peer 进度（换 Leader / 重建时）。
   */
  void clear() {
    next_index_.clear();
    match_index_.clear();
    inflight_.clear();
    snap_inflight_.clear();
    probe_inflight_.clear();
    peer_send_blocked_.clear();
  }

  /**
   * 当选后为 peer 初始化 next=last+1、match=0。
   * @param peer_id 对端
   * @param last_index Leader 当前日志末下标
   */
  void init_leader(const std::string& peer_id, uint64_t last_index) {
    next_index_[peer_id] = last_index + 1;
    match_index_[peer_id] = 0;
    inflight_[peer_id] = 0;
  }

  /**
   * 成员删除时清掉该 peer 全部进度项。
   * @param peer_id 对端
   */
  void erase(const std::string& peer_id) {
    next_index_.erase(peer_id);
    match_index_.erase(peer_id);
    inflight_.erase(peer_id);
    snap_inflight_.erase(peer_id);
    probe_inflight_.erase(peer_id);
    peer_send_blocked_.erase(peer_id);
  }

  /**
   * 查 match_index；未知返回 0。
   * @param peer_id 对端
   * @return 已匹配下标
   */
  uint64_t match_of(const std::string& peer_id) const {
    auto it = match_index_.find(peer_id);
    return it == match_index_.end() ? 0 : it->second;
  }

  /**
   * 尝试占用一个在途 AE 名额。
   * @param peer_id 对端
   * @param max_inflight 该 peer 允许的最大在途数
   * @return 是否占用成功
   */
  bool try_inc_inflight(const std::string& peer_id, uint32_t max_inflight) {
    auto& n = inflight_[peer_id];
    if (n >= max_inflight) {
      return false;
    }
    ++n;
    return true;
  }

  /**
   * 释放一个在途 AE 名额（收到应答或失败）。
   * @param peer_id 对端
   */
  void dec_inflight(const std::string& peer_id) {
    auto it = inflight_.find(peer_id);
    if (it == inflight_.end() || it->second == 0) {
      return;
    }
    --it->second;
  }

  std::unordered_map<std::string, uint64_t> next_index_;   // 下次要发给 peer 的 index
  std::unordered_map<std::string, uint64_t> match_index_;  // peer 已确认匹配到的 index
  std::unordered_map<std::string, uint32_t> inflight_;     // 在途 AE 条数
  std::unordered_set<std::string> snap_inflight_;          // 正在等快照应答的 peer
  std::unordered_set<std::string> probe_inflight_;         // 正在 probe（探 next）的 peer
  std::unordered_set<std::string> peer_send_blocked_;      // 发送失败暂堵的 peer
};

}  // namespace openkv::raft
