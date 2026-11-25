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
// Election · Membership · Replication · LogPersist · Apply · LinearRead · ReadyPump · Progress ·
// ReadyIo。

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

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

namespace openkv::raft {

using SnapshotProvider =
    std::function<std::string(uint64_t* last_included_index, uint64_t* last_included_term)>;
using SnapshotApplier = std::function<bool(uint64_t last_included_index,
                                           uint64_t last_included_term, std::string_view data)>;

class RaftNode {  //
 public:
  friend class Election;     // 选举
  friend class Membership;   // 成员管理
  friend class Replication;  // 复制
  friend class LogPersist;   // 日志持久化
  friend class Apply;        // 应用
  friend class LinearRead;   // 线性读
  friend class ReadyPump;    // 准备泵

  struct Options {
    uint32_t shard_id{0};                     // 分片id
    std::string self_id;                      // 自身id
    std::vector<Peer> peers;                  // peers列表
    uint32_t election_timeout_ms{1000};       // 选举超时时间
    uint32_t heartbeat_ms{100};               // 心跳时间
    std::string persist_dir;                  // 持久化目录
    std::shared_ptr<RaftEngine> raft_engine;  // raft引擎
    uint64_t raft_group_id{0};                // raft组id
    size_t compact_threshold{256};            // 压缩阈值
    uint32_t snapshot_min_interval_ms{1000};  // 快照最小间隔时间
    bool enable_pre_vote{true};               // 启用预投票
    bool check_quorum{true};                  // 检查法定人数
    uint32_t max_inflight_msgs{4};            // 最大未确认消息数
    size_t max_ae_entries{64};                // 最大追加条目数
    size_t max_ae_bytes{256 * 1024};          // 最大追加字节数
    bool coalesce_replication{true};          // 合并复制
    uint32_t leader_lease_ms{500};            // 领导者租约时间
    bool enable_lease_read{true};             // 启用租约读
    SyncMode persist_sync{SyncMode::kData};   // 持久化同步模式
    uint32_t persist_batch_n{1};              // 持久化批量数
    std::function<void(const std::string& id, const std::string& addr)>
        on_peer_addr;        // peer地址回调函数
    bool external_io{true};  // 外部io，是否使用外部io
  };

  RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
           SnapshotProvider snap_provider = nullptr, SnapshotApplier snap_applier = nullptr);
  ~RaftNode() = default;

  SoftState soft_state() const;
  bool is_leader() const;

  // 提议新日志
  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  // 提议配置变更
  bool propose_conf_change(const ConfChange& cc);
  // 读取索引
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  // 等待应用
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
  // 持久化相关操作
  void persist_locked() { log_persist_.persist_locked(); }
  void load_from_disk();  // 从磁盘加载
  void mark_activity();   // 标记活动

  Options opts_;
  ApplyCallback on_apply_;
  TransportSend send_;
  SnapshotProvider snap_provider_;
  SnapshotApplier snap_applier_;
  std::shared_ptr<RaftEngine> engine_;                 // raft引擎
  uint64_t group_id_{0};                               // raft组id
  std::shared_ptr<FileRaftEngine> owned_file_engine_;  // 文件raft引擎

  mutable std::mutex mu_;        // 互斥锁
  Role role_{Role::kFollower};   // 角色
  uint64_t term_{0};             // 任期
  std::string voted_for_;        // 投票
  std::string leader_id_;        // 领导者id
  RaftLog log_;                  // raft日志
  uint64_t commit_index_{0};     // 提交索引
  uint64_t last_applied_{0};     // 最后应用索引
  uint64_t apply_scheduled_{0};  // 应用调度索引

  // 协作类
  ProgressTracker progress_;  // 进度跟踪器
  Election election_;         // 选举
  Membership membership_;     // 成员管理
  Replication replication_;   // 复制
  LogPersist log_persist_;    // 日志持久化
  Apply apply_;               // 应用
  LinearRead linear_read_;    // 线性读
  ReadyPump ready_pump_;      // 准备泵
  ReadyIo ready_io_;          // 准备io

  bool activity_flag_{false};
  std::atomic<bool> in_on_apply_{false};
};

}  // namespace openkv::raft
