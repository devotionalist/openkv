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

// @file replication.hpp
// 日志复制与追赶：心跳/AE、抬 commit、InstallSnapshot（无锁，由 RaftNode::mu_ 保护）。

#pragma once

#include "openkv/raft/core/types.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace openkv::raft {

class RaftNode;

/// 管「日志怎么同步给别人、commit 怎么抬、落后了怎么发快照」。
class Replication {
 public:
  explicit Replication(RaftNode& node) : n_(node) {}

  void broadcast_heartbeat();
  void maybe_broadcast();
  void maybe_commit();

  void dec_inflight(const std::string& peer_id);
  bool try_inc_inflight(const std::string& peer_id);

  /// 入站 AE（已持锁）。
  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  /// 入站 AE 应答（已持锁；调用方负责 flush/notify）。
  void on_append_entries_reply_locked(const std::string& from, uint64_t term, bool success,
                                      uint64_t match_index);
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  void on_install_snapshot_reply_locked(const std::string& from, uint64_t term, bool success,
                                        uint64_t match_index);

  bool need_bcast_{false};  ///< 有人请求尽快广播一轮
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_snap_sent_;
  uint64_t snapshot_send_count_{0};

 private:
  RaftNode& n_;
};

}  // namespace openkv::raft
