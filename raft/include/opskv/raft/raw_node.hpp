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

// @file raw_node.hpp
// raft-rs 同构 RawNode：纯协议状态机，IO（盘/网/apply）由外层处理 Ready。

#pragma once

#include "opskv/raft/ready.hpp"
#include "opskv/raft/types.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::raft {

class RaftNode;

/// 协议层节点（对齐 raft-rs RawNode）。不落盘、不发 RPC、不调用业务 apply。
class RawNode {
 public:
  struct Options {
    uint32_t shard_id{0};
    std::string self_id;
    std::vector<Peer> peers;
    uint32_t election_timeout_ms{1000};
    uint32_t heartbeat_ms{100};
    size_t compact_threshold{256};
    uint32_t snapshot_min_interval_ms{1000};
    bool enable_pre_vote{true};
    bool check_quorum{true};
    uint32_t max_inflight_msgs{4};
    size_t max_ae_entries{64};
    size_t max_ae_bytes{256 * 1024};
    bool coalesce_replication{true};
    uint32_t leader_lease_ms{500};
    bool enable_lease_read{true};
  };

  explicit RawNode(Options opts);
  ~RawNode();

  RawNode(const RawNode&) = delete;
  RawNode& operator=(const RawNode&) = delete;
  RawNode(RawNode&&) noexcept;
  RawNode& operator=(RawNode&&) noexcept;

  SoftState soft_state() const;
  bool is_leader() const;
  bool in_joint() const;
  bool self_is_learner() const;
  uint64_t last_index() const;
  uint64_t match_index_of(const std::string& peer_id) const;
  const std::vector<Peer>& peers() const;
  uint32_t shard_id() const;
  std::string leader_hint() const;  // leader_id

  void tick();
  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  bool propose_conf_change(const ConfChange& cc);
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  bool abdicate();
  bool force_election();

  /// 入站 RPC（对齐 raft-rs Step）。
  void step_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                           uint64_t prev_term, const std::vector<LogEntry>& entries,
                           uint64_t leader_commit, bool* success, uint64_t* match_index);
  void step_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                         uint64_t last_term, bool* granted);
  void step_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                             uint64_t last_term, bool* granted);
  void step_append_entries_reply(const std::string& from, uint64_t term, bool success,
                                 uint64_t match_index);
  void step_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  void step_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  void step_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                             uint64_t last_term, std::string_view data, bool* success);
  void step_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                   uint64_t match_index);
  void step_transport_fail(const std::string& peer_id);

  void add_peer(const Peer& p);
  void remove_peer(const std::string& id);

  /// 加载已持久化状态（外层读盘后注入）。
  void bootstrap_from_disk(uint64_t term, std::string voted_for, uint64_t commit,
                           uint64_t last_applied, const std::vector<LogEntry>& entries,
                           uint64_t first_index, uint64_t snap_term);

  bool has_ready() const;
  /// 取出一批 Ready；调用方处理后必须 advance。
  Ready ready();
  void advance(const Ready& rd);

  bool consume_activity_flag();

  /// 过渡期：暴露内部 external_io RaftNode（Store Ready 驱动；禁止外层再设 IO 回调）。
  RaftNode* raft_node();
  const RaftNode* raft_node() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace opskv::raft
