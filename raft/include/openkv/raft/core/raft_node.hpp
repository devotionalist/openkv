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

// @file raft_node.hpp
// 单分片 Raft 节点门面：编排协作类。
// Election · Membership · Replication · LogPersist · Apply · LinearRead · ReadyPump · Progress · ReadyIo。

#pragma once

#include "openkv/raft/core/apply.hpp"
#include "openkv/raft/core/election.hpp"
#include "openkv/raft/core/linear_read.hpp"
#include "openkv/raft/core/log_persist.hpp"
#include "openkv/raft/core/membership.hpp"
#include "openkv/raft/core/progress_tracker.hpp"
#include "openkv/raft/core/ready.hpp"
#include "openkv/raft/core/ready_io.hpp"
#include "openkv/raft/core/ready_pump.hpp"
#include "openkv/raft/core/replication.hpp"
#include "openkv/raft/engine/file_raft_engine.hpp"
#include "openkv/raft/engine/raft_engine.hpp"
#include "openkv/raft/engine/sync_mode.hpp"
#include "openkv/raft/log/raft_log.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace openkv::raft {

using SnapshotProvider =
    std::function<std::string(uint64_t* last_included_index, uint64_t* last_included_term)>;
using SnapshotApplier =
    std::function<bool(uint64_t last_included_index, uint64_t last_included_term,
                       std::string_view data)>;

class RaftNode {
 public:
  friend class Election;
  friend class Membership;
  friend class Replication;
  friend class LogPersist;
  friend class Apply;
  friend class LinearRead;
  friend class ReadyPump;

  struct Options {
    uint32_t shard_id{0};
    std::string self_id;
    std::vector<Peer> peers;
    uint32_t election_timeout_ms{1000};
    uint32_t heartbeat_ms{100};
    std::string persist_dir;
    std::shared_ptr<RaftEngine> raft_engine;
    uint64_t raft_group_id{0};
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
    SyncMode persist_sync{SyncMode::kData};
    uint32_t persist_batch_n{1};
    std::function<void(const std::string& id, const std::string& addr)> on_peer_addr;
    bool external_io{true};
  };

  RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
           SnapshotProvider snap_provider = nullptr, SnapshotApplier snap_applier = nullptr);
  ~RaftNode() = default;

  SoftState soft_state() const;
  bool is_leader() const;

  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  bool propose_conf_change(const ConfChange& cc);
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  bool wait_applied(uint64_t index, uint32_t timeout_ms = 400,
                    const std::function<void()>& tick = {});
  void tick();

  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);
  void on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                               uint64_t match_index);
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  void on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                 uint64_t match_index);
  void on_rpc_transport_fail(const std::string& peer_id);

  void add_peer(const Peer& p);
  void remove_peer(const std::string& id);

  uint32_t shard_id() const { return opts_.shard_id; }
  std::string leader_address() const;
  const std::vector<Peer>& peers() const { return opts_.peers; }
  bool self_is_learner() const;
  uint64_t match_index_of(const std::string& peer_id) const;
  uint64_t last_index() const;
  bool abdicate();
  bool force_election();
  void recover_apply();
  bool consume_activity_flag();
  bool in_joint() const;
  bool conf_change_busy() const;

  bool has_ready() const { return ready_pump_.has_ready(); }
  Ready take_ready() { return ready_pump_.take_ready(); }
  void advance(const Ready& rd) { ready_pump_.advance(rd); }
  void persist_ready(const Ready& rd);
  RaftEngine* raft_engine() { return engine_.get(); }
  uint64_t raft_group_id() const { return group_id_; }
  void report_applied(uint64_t index) { apply_.report_applied(index); }
  void load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries);
  void set_ready_notify(std::function<void()> cb) { ready_pump_.set_ready_notify(std::move(cb)); }

 private:
  void persist_locked() { log_persist_.persist_locked(); }
  void load_from_disk();
  void mark_activity();

  Options opts_;
  ApplyCallback on_apply_;
  TransportSend send_;
  SnapshotProvider snap_provider_;
  SnapshotApplier snap_applier_;
  std::shared_ptr<RaftEngine> engine_;
  uint64_t group_id_{0};
  std::shared_ptr<FileRaftEngine> owned_file_engine_;

  mutable std::mutex mu_;
  Role role_{Role::kFollower};
  uint64_t term_{0};
  std::string voted_for_;
  std::string leader_id_;
  RaftLog log_;
  uint64_t commit_index_{0};
  uint64_t last_applied_{0};
  uint64_t apply_scheduled_{0};

  // 协作类
  ProgressTracker progress_;
  Election election_;
  Membership membership_;
  Replication replication_;
  LogPersist log_persist_;
  Apply apply_;
  LinearRead linear_read_;
  ReadyPump ready_pump_;
  ReadyIo ready_io_;

  bool activity_flag_{false};
  std::atomic<bool> in_on_apply_{false};
};

}  // namespace openkv::raft
