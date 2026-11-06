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
// 单分片 Raft：选举（PreVote）、复制、提交、CheckQuorum、InstallSnapshot、
// Joint Consensus、异步 Apply、AE inflight / 批处理。

#pragma once

#include "opskv/raft/file_raft_engine.hpp"
#include "opskv/raft/raft_engine.hpp"
#include "opskv/raft/raft_log.hpp"
#include "opskv/raft/raft_persist.hpp"
#include "opskv/raft/ready.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace opskv::raft {

/// 生成状态机快照；返回数据，并通过参数给出 last_included index/term。
using SnapshotProvider =
    std::function<std::string(uint64_t* last_included_index, uint64_t* last_included_term)>;
/// 安装快照到状态机（替换本地数据）；返回 false 则拒绝 InstallSnapshot。
using SnapshotApplier =
    std::function<bool(uint64_t last_included_index, uint64_t last_included_term,
                       std::string_view data)>;

class RaftNode {
 public:
  struct Options {
    uint32_t shard_id{0};
    std::string self_id;
    std::vector<Peer> peers;
    uint32_t election_timeout_ms{1000};
    uint32_t heartbeat_ms{100};
    /// 非空则启用 hard state + 日志落盘（宕机恢复）。
    /// 若未设置 raft_engine，则在该目录创建 FileRaftEngine（group=raft_group_id）。
    std::string persist_dir;
    /// 共享引擎；与 persist_dir 二选一优先本字段。
    std::shared_ptr<RaftEngine> raft_engine;
    /// Multi-Raft group id；0 表示使用 shard_id。
    uint64_t raft_group_id{0};
    /// 日志条数超过该阈值则 compact（触发快照边界）；0=不自动压缩。
    size_t compact_threshold{256};
    /// 同一 peer 两次 InstallSnapshot 最小间隔（毫秒）；0=不限流。
    uint32_t snapshot_min_interval_ms{1000};
    /// 超时先 PreVote，多数同意后再抬 term 真选（生产默认开）。
    bool enable_pre_vote{true};
    /// Leader 在约一个 election timeout 内收不到多数派响应则让位。
    bool check_quorum{true};
    /// 每 peer 未确认的 AE/Snap 上限（M6）。
    uint32_t max_inflight_msgs{4};
    /// 单次 AppendEntries 最多条目数（M4）。
    size_t max_ae_entries{64};
    /// 单次 AppendEntries 最多 payload 字节（M4）。
    size_t max_ae_bytes{256 * 1024};
    /// propose 不立即 broadcast，由 tick 合并发送（M4）；单节点不受影响。
    bool coalesce_replication{true};
    /// 异步 apply 队列（M5）；ConfChange 仍同步。
    bool enable_async_apply{true};
    size_t apply_queue_capacity{4096};
    /// C3/P3：Leader lease 时长（ms）；0=禁用 lease，线性读走完整 ReadIndex。
    uint32_t leader_lease_ms{500};
    bool enable_lease_read{true};
    /// P2：持久化同步模式；persist_batch_n>1 时合并多次写再落盘。
    SyncMode persist_sync{SyncMode::kData};
    uint32_t persist_batch_n{1};  // 1=每次立刻落盘
    /// ConfChange / add_peer 时登记对端地址（供 GrpcRaftTransport）。
    std::function<void(const std::string& id, const std::string& addr)> on_peer_addr;
    /// RawNode 模式：不落盘/不发网/不回调 apply，改由 has_ready/take_ready/advance 外提。
    bool external_io{false};
  };

  RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
           SnapshotProvider snap_provider = nullptr, SnapshotApplier snap_applier = nullptr);
  ~RaftNode();

  SoftState soft_state() const;
  bool is_leader() const;

  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  bool propose_conf_change(const ConfChange& cc);
  /// C3：线性读确认。成功则 *read_commit 为可读水位（通常=commit_index）。
  /// Leader：lease 有效则本地确认；否则 ReadIndex（心跳 + 等多数派 ACK）。
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  void tick();

  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);
  /// PreVote：不抬 term、不写 voted_for；term 为候选者未来任期。
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);
  void on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                               uint64_t match_index);
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  /// Follower 安装快照；success 表示接受。
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  void on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                 uint64_t match_index);
  /// Transport RPC 失败时释放 inflight（不改 next_index）；避免发不出心跳触发 check_quorum。
  void on_rpc_transport_fail(const std::string& peer_id);

  void add_peer(const Peer& p);
  void remove_peer(const std::string& id);

  uint32_t shard_id() const { return opts_.shard_id; }
  std::string leader_address() const;
  const std::vector<Peer>& peers() const { return opts_.peers; }
  /// 本节点是否为 learner（learner 不发起选举）。
  bool self_is_learner() const;
  /// Leader 上某 peer 的 match_index；非 Leader / 未知 peer 返回 0。
  uint64_t match_index_of(const std::string& peer_id) const;
  uint64_t last_index() const;
  /// Leader 主动让位（转主前）；随后由目标节点 force_election。
  bool abdicate();
  /// 强制发起一轮选举（供 TRANSFER 目标使用；跳过 PreVote）。
  bool force_election();
  /// 启动后若有未 apply 的已提交日志，补齐状态机。
  void recover_apply();
  /// 本 tick 周期是否有复制/选主活动（供 MultiRaft 冷热调度）。
  bool consume_activity_flag();
  bool in_joint() const;
  /// Joint 中或 LeaveJoint 未完成：禁止再 propose Promote/Remove。
  bool conf_change_busy() const;

  /// raft-rs Ready 管线（external_io 或适配器驱动时使用）。
  bool has_ready() const;
  Ready take_ready();
  void advance(const Ready& rd);
  /// Store persist_fn：external_io 下把 Ready 写入 RaftEngine（LogBatch）。
  void persist_ready(const Ready& rd);
  RaftEngine* raft_engine() { return engine_.get(); }
  uint64_t raft_group_id() const { return group_id_; }
  /// ApplyFsm 完成后推进 last_applied（external_io；ConfChange 已在协议侧推进）。
  void report_applied(uint64_t index);
  /// 注入已落盘状态（RawNode bootstrap）。
  void load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries);

 private:
  struct Outbound {
    std::string peer_id;
    std::string type;
    std::string payload;
  };
  struct ApplyItem {
    uint64_t index{0};
    std::string data;
  };

  void become_follower(uint64_t term, const std::string& leader = {});
  void become_candidate();
  void become_leader();
  void step_down_if_stale(uint64_t term);
  void broadcast_heartbeat();
  void maybe_broadcast();
  void start_pre_vote();
  void start_election();
  void maybe_check_quorum();
  void note_peer_progress(const std::string& peer_id);
  void maybe_commit();
  void apply_committed();
  void maybe_compact();
  void maybe_auto_leave_joint();
  uint32_t majority() const;
  uint32_t majority_of(const std::vector<std::string>& voters) const;
  std::vector<std::string> voter_ids() const;
  bool quorate_at(const std::vector<std::string>& voters, uint64_t index) const;
  bool votes_won(const std::unordered_map<std::string, bool>& votes) const;
  void dec_inflight(const std::string& peer_id);
  bool try_inc_inflight(const std::string& peer_id);
  void apply_conf_entry(const LogEntry& e);
  bool encode_conf(const ConfChange& cc, std::string* out) const;
  static bool decode_conf(std::string_view data, ConfChange* cc);
  void enqueue(std::string peer_id, std::string type, std::string payload);
  void flush_transport();
  void persist_locked();
  void persist_maybe_flush(bool force);
  /// 相对 stable_* 增量 append/cut + put_raft_state（替代整窗 save）。
  void engine_flush_locked(bool sync);
  void load_from_disk();
  void mark_activity();
  void start_apply_worker();
  void stop_apply_worker();
  void apply_worker_loop();
  bool enqueue_apply(uint64_t index, std::string data);
  void wait_apply_caught_up_to(uint64_t index);
  bool lease_valid_locked() const;
  void note_read_index_ack(const std::string& peer_id);

  Options opts_;
  ApplyCallback on_apply_;
  TransportSend send_;
  SnapshotProvider snap_provider_;
  SnapshotApplier snap_applier_;
  std::shared_ptr<RaftEngine> engine_;
  uint64_t group_id_{0};
  /// persist_dir 自动创建的 FileRaftEngine（保持 engine_ 存活）。
  std::shared_ptr<FileRaftEngine> owned_file_engine_;

  mutable std::mutex mu_;
  Role role_{Role::kFollower};
  uint64_t term_{0};
  std::string voted_for_;
  std::string leader_id_;
  RaftLog log_;
  uint64_t commit_index_{0};
  /// 状态机已 apply（含 conf）；Facade 可见。
  uint64_t last_applied_{0};
  /// 已调度进 apply 队列或同步 apply 的最高 index。
  uint64_t apply_scheduled_{0};

  std::unordered_map<std::string, uint64_t> next_index_;
  std::unordered_map<std::string, uint64_t> match_index_;
  std::unordered_map<std::string, uint32_t> inflight_;
  std::unordered_set<std::string> snap_inflight_;
  /// 空心跳 / probe AE：不占 max_inflight，单独限 1 条在途，避免复制管道堵死保活。
  std::unordered_set<std::string> probe_inflight_;
  /// 本轮 broadcast 因 inflight 满未能向该 peer 发 AE（非对端故障）。
  std::unordered_set<std::string> peer_send_blocked_;
  std::unordered_map<std::string, bool> votes_;
  std::unordered_map<std::string, bool> pre_votes_;
  bool in_pre_vote_{false};
  /// Leader：距上次收到该 voter 成功响应的 tick 数。
  std::unordered_map<std::string, int> peer_quiet_ticks_;
  std::vector<Outbound> outbound_;
  bool need_bcast_{false};

  bool in_joint_{false};
  std::vector<std::string> joint_old_voters_;
  std::vector<std::string> joint_new_voters_;
  bool auto_leave_pending_{false};
  /// 已 propose LeaveJoint、等待 apply；避免每 tick 重复 append。
  bool leave_joint_inflight_{false};

  int election_elapsed_{0};
  int heartbeat_elapsed_{0};
  int election_timeout_ticks_{10};
  /// 成为 Leader 后跳过 check_quorum 的剩余 tick（等首轮心跳 ACK）。
  int leader_quorum_grace_ticks_{0};
  bool activity_flag_{false};
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_snap_sent_;

  // C3 ReadIndex / Lease
  std::chrono::steady_clock::time_point lease_expire_{};
  uint64_t read_index_gen_{0};
  uint64_t pending_read_gen_{0};
  uint64_t pending_read_commit_{0};
  std::unordered_set<std::string> read_index_acks_;
  std::condition_variable read_cv_;

  // O1
  uint64_t election_count_{0};
  uint64_t snapshot_send_count_{0};

  // P2 group persist
  uint32_t persist_pending_{0};
  bool persist_dirty_{false};
  /// 已稳定落盘的日志窗（增量 flush 基准；0=尚未落过盘）。
  uint64_t stable_first_index_{0};
  uint64_t stable_last_index_{0};

  // Ready 缓冲（external_io）
  bool ready_hs_dirty_{false};
  HardState ready_hs_{};
  std::vector<LogEntry> ready_unstable_;
  std::vector<LogEntry> ready_committed_;
  std::vector<SnapshotMeta> ready_snaps_;
  uint64_t ready_applied_watermark_{0};

  std::mutex apply_mu_;
  std::condition_variable apply_cv_;
  std::deque<ApplyItem> apply_q_;
  std::atomic<bool> apply_running_{false};
  /// 当前线程正在执行 on_apply；recover_apply 遇此须直接返回，否则自死锁。
  std::atomic<bool> in_on_apply_{false};
  std::thread apply_thread_;
};

}  // namespace opskv::raft
