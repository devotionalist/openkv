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
// 单分片完整 Raft 节点：选举、复制、提交、CheckQuorum、快照、Joint 成员变更、
// 以及相对 stable_* 的增量落盘。
//
// 两种用法：
//   - external_io=true（默认）：核内只攒 Ready，由 Store 负责落盘 / 发网 / apply
//   - external_io=false：嵌入路径（如 Meta）自管盘/网，同步调 on_apply
//
// 协作：日志=RaftLog；落盘=RaftEngine；出站=TransportSend；业务状态机=ApplyCallback。

#pragma once

#include "openkv/raft/core/progress_tracker.hpp"
#include "openkv/raft/core/ready.hpp"
#include "openkv/raft/core/ready_io.hpp"
#include "openkv/raft/engine/file_raft_engine.hpp"
#include "openkv/raft/engine/raft_engine.hpp"
#include "openkv/raft/engine/raft_persist.hpp"
#include "openkv/raft/log/raft_log.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace openkv::raft {

/// 生成状态机快照：写出字节，并填「快照覆盖到哪条日志」(index/term)。
using SnapshotProvider =
    std::function<std::string(uint64_t* last_included_index, uint64_t* last_included_term)>;
/// 装快照进状态机（整份替换本地数据）；返回 false = 拒绝 InstallSnapshot。
using SnapshotApplier =
    std::function<bool(uint64_t last_included_index, uint64_t last_included_term,
                       std::string_view data)>;

class RaftNode {
 public:
  struct Options {
    uint32_t shard_id{0};                 ///< 分片 / Region 编号（展示与默认 group id）
    std::string self_id;                  ///< 本节点 id（和 Peer.id 同一套）
    std::vector<Peer> peers;              ///< 初始成员表（含自己）
    uint32_t election_timeout_ms{1000};   ///< 多久没听到 Leader 就尝试选主
    uint32_t heartbeat_ms{100};           ///< Leader 隔多久发一波心跳 / 复制
    /// 非空则启用 hard state + 日志落盘（宕机靠它恢复）。
    /// 未设 raft_engine 时，在该目录建 FileRaftEngine（group=raft_group_id）。
    std::string persist_dir;
    /// 共享引擎；与 persist_dir 二选一，本字段优先。
    std::shared_ptr<RaftEngine> raft_engine;
    /// Multi-Raft group id；0 表示用 shard_id。
    uint64_t raft_group_id{0};
    /// 日志条数超阈则 compact（触发快照边界）；0=不自动压缩。
    size_t compact_threshold{256};
    /// 同一 peer 两次发 InstallSnapshot 的最小间隔（毫秒）；0=不限流。
    uint32_t snapshot_min_interval_ms{1000};
    /// 超时先 PreVote，多数同意后再抬 term 真选（生产默认开，减扰动）。
    bool enable_pre_vote{true};
    /// Leader 约一个 election timeout 内收不到多数派响应就让位。
    bool check_quorum{true};
    /// 每 peer 未确认的 AE/Snap 条数上限（管道深度）。
    uint32_t max_inflight_msgs{4};
    /// 单次 AppendEntries 最多带几条日志。
    size_t max_ae_entries{64};
    /// 单次 AppendEntries 最多带多少字节 payload。
    size_t max_ae_bytes{256 * 1024};
    /// propose 不立刻广播，等 tick 合并发送；单节点不受影响。
    /// external_io 忽略此开关：出站走 Ready，propose 当拍必须入队 AE。
    bool coalesce_replication{true};
    /// Leader lease 时长（ms）；0=禁用，线性读走完整 ReadIndex。
    uint32_t leader_lease_ms{500};
    bool enable_lease_read{true};         ///< 是否允许 lease 内本地确认线性读
    /// 持久化同步模式；persist_batch_n>1 时合并多次写再落盘。
    SyncMode persist_sync{SyncMode::kData};
    uint32_t persist_batch_n{1};  ///< 1=每次立刻落盘
    /// ConfChange / add_peer 时登记对端地址（给 GrpcRaftTransport 用）。
    std::function<void(const std::string& id, const std::string& addr)> on_peer_addr;
    /// true：不落盘/不发网/不回调 apply，由 Store has_ready/take_ready/advance 驱动。
    /// false：嵌入路径（Meta）自管盘/网、同步 on_apply。
    bool external_io{true};
  };

  /// 构造节点：注入 apply 回调、出站发送，以及可选的快照生成/安装。
  RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
           SnapshotProvider snap_provider = nullptr, SnapshotApplier snap_applier = nullptr);
  ~RaftNode() = default;

  /// 看当前角色 / term / leader（不落盘的「软状态」快照）。
  SoftState soft_state() const;
  /// 本节点此刻是否是 Leader。
  bool is_leader() const;

  /// 提一条普通业务日志。成功则 *assigned_index 为写入的日志下标；非 Leader 返回 false。
  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  /// 提一条成员变更（加/删/晋级/进联合/离联合）。非 Leader 或正在 Joint 忙碌时失败。
  bool propose_conf_change(const ConfChange& cc);
  /// 线性读确认：成功则 *read_commit 为可读水位（通常=commit_index）。
  /// Leader 且 lease 有效 → 本地立刻确认；否则走 ReadIndex（心跳 + 等多数派 ACK）。
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  /// 等到状态机 last_applied >= index（条件变量等，不空转 sleep）。
  /// tick 可在等待切片间调用（例如 Store flush_ready），推动 apply 前进。
  bool wait_applied(uint64_t index, uint32_t timeout_ms = 400,
                    const std::function<void()>& tick = {});
  /// 周期时钟：Leader 发心跳/查多数派；Follower 超时则 PreVote 或竞选。
  /// 外面（Store schedule_tick）按固定间隔敲；不敲则既不心跳也不选举。
  void tick();

  /// 收到 Leader 的 AppendEntries（心跳或带日志）：对齐 prev、追加、抬 commit。
  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  /// 收到正式 RequestVote：可抬 term、写 voted_for，决定是否给票。
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);
  /// 收到 PreVote：只试探「若你现在选会不会赢」，不抬 term、不写 voted_for。
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);
  /// 收到 AE 应答：成功则推 match/next 并尝试提交；失败则回退 next 再探。
  void on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                               uint64_t match_index);
  /// 收到正式选票应答：多数同意 → 当选 Leader。
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  /// 收到 PreVote 应答：多数同意 → 再抬 term 开正式选举。
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  /// Follower 收到 InstallSnapshot：装进状态机并裁日志；*success 表示是否接受。
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  /// Leader 收到快照应答：成功则更新 match，失败则清 inflight 以便重试。
  void on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                 uint64_t match_index);
  /// 出站 RPC 发送失败：释放该 peer 的 inflight，避免「发不出去却占着坑」导致误判分区。
  void on_rpc_transport_fail(const std::string& peer_id);

  /// 本地立刻把 peer 加进成员表（一般由 ConfChange apply 或启动配置调用）。
  void add_peer(const Peer& p);
  /// 本地立刻从成员表摘掉该 id（并清 next/match/inflight 等）。
  void remove_peer(const std::string& id);

  /// 本分片编号。
  uint32_t shard_id() const { return opts_.shard_id; }
  /// 当前已知 Leader 的地址（方便客户端 redirect）；未知则空。
  std::string leader_address() const;
  /// 当前成员列表（只读视图）。
  const std::vector<Peer>& peers() const { return opts_.peers; }
  /// 本节点是否是 learner（learner 不发起选举、不计入多数派）。
  bool self_is_learner() const;
  /// Leader 上该 peer 已复制到的最高 index；非 Leader / 未知 peer 返回 0。
  uint64_t match_index_of(const std::string& peer_id) const;
  /// 本地日志最后一条的 index。
  uint64_t last_index() const;
  /// Leader 主动让位（转主前一步）；随后由目标节点 force_election。
  bool abdicate();
  /// 强制开一轮正式选举（给 TRANSFER 目标用；跳过 PreVote）。
  bool force_election();
  /// 启动后若有「已提交但还没 apply」的日志，补推状态机。
  void recover_apply();
  /// 读并清掉「本周期有没有复制/选主活动」标志（给 MultiRaft 冷热调度用）。
  bool consume_activity_flag();
  /// 是否正处于 Joint 配置（新旧两套投票人同时算多数派）。
  bool in_joint() const;
  /// Joint 中或 LeaveJoint 还没做完：禁止再 propose Promote/Remove，避免叠变更。
  bool conf_change_busy() const;

  /// 是否有待外提的 Ready（HardState / 新日志 / 待 apply / 快照）。
  bool has_ready() const;
  /// 取出一批 Ready 给 Store；核内缓冲清空，等待 advance / persist_ready / report_applied。
  Ready take_ready();
  /// Store 处理完本批 Ready 后回告（清标记、推进稳定窗相关状态）。
  void advance(const Ready& rd);
  /// Store 的落盘钩子：把 Ready 里的 HardState/日志写入 RaftEngine。
  void persist_ready(const Ready& rd);
  /// 底层持久化引擎（可空）。
  RaftEngine* raft_engine() { return engine_.get(); }
  /// Multi-Raft 组 id。
  uint64_t raft_group_id() const { return group_id_; }
  /// ApplyFsm 做完后推进 last_applied（external_io 路径；ConfChange 已在协议侧生效）。
  void report_applied(uint64_t index);
  /// 注入已落盘的 HardState + 日志（RawNode / 启动 bootstrap）。
  void load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries);
  /// external_io：批外产生 Ready 时回调（通常 mark_hot）；不设则只能等 tick。
  void set_ready_notify(std::function<void()> cb);

 private:
  struct Outbound {
    std::string peer_id;
    std::string type;
    std::string payload;
  };

  /// 变成 Follower：更新 term、认 leader（可空）、清选举状态。
  void become_follower(uint64_t term, const std::string& leader = {});
  /// 变成 Candidate：抬 term、给自己投票，准备拉票。
  void become_candidate();
  /// 变成 Leader：初始化 next/match、开租约宽限期，立刻广播一轮。
  void become_leader();
  /// 若对方 term 更大：立刻降为 Follower，避免拿旧任期发号施令。
  void step_down_if_stale(uint64_t term);
  /// Leader：向各 peer 发心跳 / 探测 AE / 必要时发快照。
  void broadcast_heartbeat();
  /// propose 后：合并发送模式下只打 need_bcast_；否则立刻 broadcast。
  void maybe_broadcast();
  /// Ready 非空则调 ready_notify_（放锁后调，避免重入）。
  void maybe_notify_ready();
  /// 发起 PreVote 轮（不抬 term）；多数同意后再 start_election。
  void start_pre_vote();
  /// 发起正式选举（抬 term、RequestVote）。
  void start_election();
  /// Leader 检查：多数派是否还在回话；太久没动静就让位。
  void maybe_check_quorum();
  /// 记下某 peer 刚有进展（清 quiet 计数），供 CheckQuorum / ReadIndex 用。
  void note_peer_progress(const std::string& peer_id);
  /// 按各 peer 的 match_index 算能否抬 commit_index（Joint 时要双多数）。
  void maybe_commit();
  /// 把 commit_index 之前未安排的条目推进 apply：
  /// external_io → 普通条目进 Ready；ConfChange 核内立刻改成员表。
  void apply_committed();
  /// 日志过长则生成本地 FSM 快照并 compact 前缀。
  void maybe_compact();
  /// Joint 已稳定且 auto_leave：自动 propose LeaveJoint，收成单一配置。
  void maybe_auto_leave_joint();
  /// 当前投票人集合的多数派人数（ceil(n/2)+… 按实现）。
  uint32_t majority() const;
  /// 指定投票人列表的多数派人数。
  uint32_t majority_of(const std::vector<std::string>& voters) const;
  /// 有投票权的 peer id 列表（Joint 时返回合适的集合视图）。
  std::vector<std::string> voter_ids() const;
  /// 指定投票人集合里，是否已有多数派的 match >= index。
  bool quorate_at(const std::vector<std::string>& voters, uint64_t index) const;
  /// 选票 map 是否已达多数同意（含自己）。
  bool votes_won(const std::unordered_map<std::string, bool>& votes) const;
  /// 该 peer 一条在途 AE/Snap 已结束：inflight 减一。
  void dec_inflight(const std::string& peer_id);
  /// 尝试占一个 inflight 名额；管道满则返回 false（本轮不发给他）。
  bool try_inc_inflight(const std::string& peer_id);
  /// 应用一条 ConfChange 日志：改 peers / Joint 标志 / learner 等。
  void apply_conf_entry(const LogEntry& e);
  /// 把 ConfChange 编成日志 payload（首字节 0xFF 标记）。
  bool encode_conf(const ConfChange& cc, std::string* out) const;
  /// 从日志 payload 解出 ConfChange；非 conf 返回 false。
  static bool decode_conf(std::string_view data, ConfChange* cc);
  /// 攒一条待发出站消息（先不出网，等 flush_transport）。
  void enqueue(std::string peer_id, std::string type, std::string payload);
  /// 把 outbound_ 交给 TransportSend 真正发出（通常在放锁后调用）。
  void flush_transport();
  /// 标记 hard state / 日志脏了，按 batch 策略决定是否立刻 flush。
  void persist_locked();
  /// 尝试把脏状态刷进引擎；force=true 时无视 batch 合并。
  void persist_maybe_flush(bool force);
  /// 相对 stable_* 做增量 append/cut + put_raft_state（不整窗重写）。
  void engine_flush_locked(bool sync);
  /// 组好要写的 LogBatch；out_first/out_last 是写成功后应记的稳定窗。无引擎则 nullptr。
  std::unique_ptr<RaftLogBatch> build_engine_batch_locked(uint64_t& out_first, uint64_t& out_last);
  /// 写盘成功后推进 stable_*（只前进不回退，兼容并发 flush）。
  void commit_stable_locked(uint64_t first, uint64_t last);
  /// 启动时从引擎拉 HardState + 日志，恢复内存状态。
  void load_from_disk();
  /// 本地 FSM 快照文件路径（compact 前落一份，重启后先恢复再追日志）。
  std::string local_fsm_snap_path() const;
  /// compact 前把当前 FSM 快照落到本地文件（避免 Meta 路由等状态蒸发）。
  void save_local_fsm_snapshot_locked();
  /// 启动时若有本地 FSM 快照则装回状态机。
  void restore_local_fsm_snapshot();
  /// 打上「本周期有活动」标记（复制/选主），供冷热调度。
  void mark_activity();
  /// 当前 Leader lease 是否仍有效（线性读可走本地快路径）。
  bool lease_valid_locked() const;
  /// ReadIndex：记下某 peer 对本次读确认心跳的 ACK，凑够多数则唤醒等待者。
  void note_read_index_ack(const std::string& peer_id);

  Options opts_;
  ApplyCallback on_apply_;
  TransportSend send_;
  SnapshotProvider snap_provider_;
  SnapshotApplier snap_applier_;
  std::shared_ptr<RaftEngine> engine_;
  uint64_t group_id_{0};
  /// persist_dir 自动创建的 FileRaftEngine（拖住 engine_ 生命周期）。
  std::shared_ptr<FileRaftEngine> owned_file_engine_;

  mutable std::mutex mu_;
  Role role_{Role::kFollower};
  uint64_t term_{0};           ///< 当前任期：「第几轮选举 / 谁在当家」
  std::string voted_for_;      ///< 本任期把票投给了谁
  std::string leader_id_;      ///< 当前已知 Leader
  RaftLog log_;
  uint64_t commit_index_{0};   ///< 已提交、可以安全 apply 的最高 index
  /// 状态机已经 apply 完的最高 index（含 conf）；对外可见。
  uint64_t last_applied_{0};
  /// 已经「安排出去」的最高 index（塞进 Ready 或同步调过 on_apply）。
  uint64_t apply_scheduled_{0};

  ProgressTracker progress_;  ///< Leader 复制进度（next/match/inflight）
  std::unordered_map<std::string, bool> votes_;       ///< 正式选举收票
  std::unordered_map<std::string, bool> pre_votes_;   ///< PreVote 收票
  bool in_pre_vote_{false};                            ///< 正在 PreVote 轮
  /// Leader：距上次收到该 voter 成功响应过了多少 tick。
  std::unordered_map<std::string, int> peer_quiet_ticks_;
  std::vector<Outbound> outbound_;                     ///< 待 flush 的出站消息
  bool need_bcast_{false};                             ///< 有人请求尽快广播一轮

  bool in_joint_{false};                               ///< 是否在 Joint 配置
  std::vector<std::string> joint_old_voters_;          ///< Joint 旧投票人
  std::vector<std::string> joint_new_voters_;          ///< Joint 新投票人
  bool auto_leave_pending_{false};                     ///< 稳定后自动离 Joint
  /// 已 propose LeaveJoint、等 apply；避免每 tick 重复 append。
  bool leave_joint_inflight_{false};

  int election_elapsed_{0};           ///< Follower 距上次听到 Leader 的 tick 数
  int heartbeat_elapsed_{0};          ///< Leader 距上次该发心跳的 tick 数
  int election_timeout_ticks_{10};    ///< 折算成「多少次 tick 算选举超时」
  /// 当选后前几拍跳过 CheckQuorum，等首轮心跳 ACK 回来。
  int leader_quorum_grace_ticks_{0};
  bool activity_flag_{false};         ///< 本周期是否有复制/选主活动
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_snap_sent_;

  // 线性读：租约 + ReadIndex
  std::chrono::steady_clock::time_point lease_expire_{};
  uint64_t read_index_gen_{0};
  uint64_t pending_read_gen_{0};
  uint64_t pending_read_commit_{0};
  std::unordered_set<std::string> read_index_acks_;
  std::condition_variable read_cv_;
  /// report_applied 抬 last_applied 时唤醒 wait_applied。
  std::condition_variable applied_cv_;

  // 观测计数
  uint64_t election_count_{0};
  uint64_t snapshot_send_count_{0};

  // 分组落盘
  uint32_t persist_pending_{0};
  bool persist_dirty_{false};
  /// 已稳定落盘的日志窗（增量 flush 基准；0=还没落过）。
  uint64_t stable_first_index_{0};
  uint64_t stable_last_index_{0};
  /// 串行化 engine_->consume（fsync）；不持 mu_，允许 propose 与写盘重叠。
  std::mutex persist_io_mu_;

  ReadyIo ready_io_;  ///< Ready 缓冲（external_io 外提）

  /// 当前线程正在执行 on_apply；recover_apply 遇此须直接返回，否则自死锁。
  std::atomic<bool> in_on_apply_{false};

  /// Store 注册：批外产生 Ready 时唤醒 Peer（见 set_ready_notify）。
  std::function<void()> ready_notify_;
};

}  // namespace openkv::raft
