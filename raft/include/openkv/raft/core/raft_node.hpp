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

/* 单分片 Raft 门面：锁内编排选举/复制/落盘/apply，Ready 可外提给 Store。 */
class RaftNode {
 public:
  friend class Election;
  friend class Membership;
  friend class Replication;
  friend class LogPersist;
  friend class Apply;
  friend class LinearRead;
  friend class ReadyPump;

  /* 构造参数：身份、超时、引擎、复制/读/耐久开关。 */
  struct Options {
    uint32_t shard_id{0};                     // 分片 / Region id
    std::string self_id;                      // 本节点 peer id
    std::vector<Peer> peers;                  // 初始成员表
    uint32_t election_timeout_ms{1000};       // 选举超时（毫秒）
    uint32_t heartbeat_ms{100};               // 心跳间隔（毫秒）
    std::string persist_dir;                  // 无共享引擎时的文件落盘目录
    std::shared_ptr<RaftEngine> raft_engine;  // 共享/注入的持久化引擎；空则自建文件引擎
    uint64_t raft_group_id{0};                // 引擎内 group id（常=shard）
    size_t compact_threshold{256};            // 已 apply 超过该条数才考虑 compact
    uint32_t snapshot_min_interval_ms{1000};  // 对同一 peer 发快照的最小间隔
    bool enable_pre_vote{true};               // 先 PreVote 再正式选举
    bool check_quorum{true};                  // Leader 周期性查多数派是否还活着
    uint32_t max_inflight_msgs{4};            // 每 peer 在途 AE 上限
    size_t max_ae_entries{64};                // 单次 AE 最多条目数
    size_t max_ae_bytes{256 * 1024};          // 单次 AE 最大字节
    bool coalesce_replication{true};          // 合并复制推送
    uint32_t leader_lease_ms{500};            // Leader 读租约窗口（毫秒）
    bool enable_lease_read{true};             // 允许租约内本地线性读
    SyncMode persist_sync{SyncMode::kData};   // 落盘耐久级别
    uint32_t persist_batch_n{1};              // 嵌入路径攒多少次再刷引擎
    std::function<void(const std::string& id, const std::string& addr)>
        on_peer_addr;        // 发现 peer 地址时回调（更新传输表）
    bool external_io{true};  // true=Ready 外提由 Store 落盘发网；false=节点内同步 IO
  };

  /**
   * 构造节点；不自动选主，需上层 tick / 入站 RPC。
   * @param opts 身份与调参
   * @param on_apply 日志 commit 后状态机回调
   * @param send 嵌入路径出站发送；external_io 时主要走 Ready.messages
   * @param snap_provider 生成出站快照；默认空
   * @param snap_applier 安装入站快照到 FSM；默认空
   */
  RaftNode(Options opts, ApplyCallback on_apply, TransportSend send,
           SnapshotProvider snap_provider = nullptr, SnapshotApplier snap_applier = nullptr);
  ~RaftNode() = default;

  /**
   * 观测用软状态快照（角色/Leader/term/commit 等）。
   * @return SoftState 副本
   */
  SoftState soft_state() const;
  /**
   * 本节点当前是否 Leader。
   * @return true 表示当家
   */
  bool is_leader() const;

  /**
   * Leader 提议新业务日志；非 Leader 失败。
   * @param data 状态机载荷
   * @param assigned_index 成功时写出分配的 index；可为 nullptr
   * @return 是否成功入日志
   */
  bool propose(std::string data, uint64_t* assigned_index = nullptr);
  /**
   * 提议成员变更（走日志，commit 后 apply 才改配置）。
   * @param cc 变更内容
   * @return 是否成功入日志
   */
  bool propose_conf_change(const ConfChange& cc);
  /**
   * 线性读：租约内直接返回 commit；否则发 ReadIndex 等多数 ACK。
   * @param read_commit 成功时写出可读水位
   * @param timeout_ms 等待多数 ACK 的超时；默认 500
   * @return 是否拿到可读水位
   */
  bool read_index(uint64_t* read_commit, uint32_t timeout_ms = 500);
  /**
   * 等到 last_applied >= index，或超时。
   * @param index 目标 apply 水位
   * @param timeout_ms 超时毫秒；默认 400
   * @param tick 等待切片间可推进协议的回调；默认空
   * @return 是否在超时前达到
   */
  bool wait_applied(uint64_t index, uint32_t timeout_ms = 400,
                    const std::function<void()>& tick = {});
  /**
   * 推进选举/心跳计时器；由 Store tick 或嵌入循环调用。
   */
  void tick();

  /**
   * 入站 AppendEntries。
   * @param from 对端 peer id
   * @param term Leader 任期
   * @param prev_idx 前一条日志下标
   * @param prev_term 前一条日志 term
   * @param entries 本批条目
   * @param leader_commit Leader 声称的 commit
   * @param success 输出：是否接受
   * @param match_index 输出：本端匹配到的下标
   */
  void on_append_entries(const std::string& from, uint64_t term, uint64_t prev_idx,
                         uint64_t prev_term, const std::vector<LogEntry>& entries,
                         uint64_t leader_commit, bool* success, uint64_t* match_index);
  /**
   * 入站 RequestVote。
   * @param from 候选者 id
   * @param term 候选任期
   * @param last_idx 候选日志末下标
   * @param last_term 候选日志末 term
   * @param granted 输出：是否投票
   */
  void on_request_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                       uint64_t last_term, bool* granted);
  /**
   * 入站 PreVote（不抬 term）。
   * @param from 候选者 id
   * @param term 拟用任期
   * @param last_idx 候选日志末下标
   * @param last_term 候选日志末 term
   * @param granted 输出：是否同意预投
   */
  void on_request_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                           uint64_t last_term, bool* granted);
  /**
   * 入站 AppendEntries 应答。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param success 本轮是否成功
   * @param match_index 对端匹配下标
   */
  void on_append_entries_reply(const std::string& from, uint64_t term, bool success,
                               uint64_t match_index);
  /**
   * 入站 RequestVote 应答。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param granted 是否同意
   */
  void on_request_vote_reply(const std::string& from, uint64_t term, bool granted);
  /**
   * 入站 PreVote 应答。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param granted 是否同意预投
   */
  void on_request_pre_vote_reply(const std::string& from, uint64_t term, bool granted);
  /**
   * 入站 InstallSnapshot。
   * @param from 对端 peer id
   * @param term Leader 任期
   * @param last_index 快照包含的末 index
   * @param last_term 对应 term
   * @param data 快照字节
   * @param success 输出：是否安装成功
   */
  void on_install_snapshot(const std::string& from, uint64_t term, uint64_t last_index,
                           uint64_t last_term, std::string_view data, bool* success);
  /**
   * 入站 InstallSnapshot 应答。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param success 是否成功
   * @param match_index 安装后匹配下标
   */
  void on_install_snapshot_reply(const std::string& from, uint64_t term, bool success,
                                 uint64_t match_index);
  /**
   * 传输层报告对某 peer 发送失败（触发探测/降速）。
   * @param peer_id 不可达 peer
   */
  void on_rpc_transport_fail(const std::string& peer_id);

  /**
   * 热路径加成员（通常仍应走 ConfChange 日志）。
   * @param p 新 peer
   */
  void add_peer(const Peer& p);
  /**
   * 热路径删成员。
   * @param id peer id
   */
  void remove_peer(const std::string& id);

  /**
   * 本节点 shard id。
   * @return Options.shard_id
   */
  uint32_t shard_id() const { return opts_.shard_id; }
  /**
   * 已知 Leader 的地址（redirect 用）；未知可空。
   * @return 地址串
   */
  std::string leader_address() const;
  /**
   * 当前成员表只读视图。
   * @return peers 引用
   */
  const std::vector<Peer>& peers() const { return opts_.peers; }
  /**
   * 本节点是否 learner（无投票权）。
   * @return true 表示学习者
   */
  bool self_is_learner() const;
  /**
   * Leader 视角某 peer 的 match_index；未知为 0。
   * @param peer_id 对端
   * @return 匹配下标
   */
  uint64_t match_index_of(const std::string& peer_id) const;
  /**
   * 本地日志末 index。
   * @return last_index
   */
  uint64_t last_index() const;
  /**
   * Leader 主动退位成 Follower。
   * @return 是否由 Leader 成功退位
   */
  bool abdicate();
  /**
   * 强制立刻发起选举（运维/转让）。
   * @return 是否发起成功
   */
  bool force_election();
  /**
   * 启动或恢复后：把已提交未 apply 的日志推到状态机。
   */
  void recover_apply();
  /**
   * 取出并清零活动标志（Store 热/冷判断）。
   * @return 自上次消费以来是否有活动
   */
  bool consume_activity_flag();
  /**
   * 是否处于 Joint 配置。
   * @return true 表示新旧双多数
   */
  bool in_joint() const;
  /**
   * Joint 中或 LeaveJoint 未完成，勿再 Promote/Remove。
   * @return true 表示成员变更忙
   */
  bool conf_change_busy() const;

  /**
   * external_io：是否有待外提 Ready。
   * @return true 表示可 take_ready
   */
  bool has_ready() const { return ready_pump_.has_ready(); }
  /**
   * 取出一批 Ready（HardState/日志/消息/快照）。
   * @return Ready 批次
   */
  Ready take_ready() { return ready_pump_.take_ready(); }
  /**
   * Store 落盘/发网/apply 完成后推进内部游标。
   * @param rd 刚才处理完的那批 Ready
   */
  void advance(const Ready& rd) { ready_pump_.advance(rd); }
  /**
   * 把 Ready 写入 RaftEngine（Store persist_fn 调用）。
   * @param rd 待落盘批次
   */
  void persist_ready(const Ready& rd);
  /**
   * 底层持久化引擎；可能为空引擎场景。
   * @return 引擎裸指针
   */
  RaftEngine* raft_engine() { return engine_.get(); }
  /**
   * 引擎内 group id。
   * @return raft_group_id
   */
  uint64_t raft_group_id() const { return group_id_; }
  /**
   * Apply worker 回告已应用到 index。
   * @param index 已 apply 水位
   */
  void report_applied(uint64_t index) { apply_.report_applied(index); }
  /**
   * 从磁盘恢复：装入 HardState 与日志窗。
   * @param hs 已持久化硬状态
   * @param entries 日志条目
   */
  void load_hard_state(const PersistedHardState& hs, const std::vector<LogEntry>& entries);
  /**
   * 注册 Ready 有货时的唤醒回调（Store request_ready）。
   * @param cb 无参回调
   */
  void set_ready_notify(std::function<void()> cb) { ready_pump_.set_ready_notify(std::move(cb)); }

 private:
  /**
   * 持锁触发一次持久化（嵌入路径或脏标记）。
   */
  void persist_locked() { log_persist_.persist_locked(); }
  /**
   * 启动时从引擎/目录装载状态。
   */
  void load_from_disk();
  /**
   * 置位 activity_flag_，供 Store 热路径。
   */
  void mark_activity();

  Options opts_;                                       // 构造参数
  ApplyCallback on_apply_;                             // 状态机回调
  TransportSend send_;                                 // 嵌入出站
  SnapshotProvider snap_provider_;                     // 出站快照生成
  SnapshotApplier snap_applier_;                       // 入站快照安装
  std::shared_ptr<RaftEngine> engine_;                 // 持久化引擎
  uint64_t group_id_{0};                               // 引擎 group id
  std::shared_ptr<FileRaftEngine> owned_file_engine_;  // 自建文件引擎（若有）

  mutable std::mutex mu_;        // 协议大锁
  Role role_{Role::kFollower};   // 当前角色
  uint64_t term_{0};             // 当前任期
  std::string voted_for_;        // 本任期已投票对象
  std::string leader_id_;        // 已知 Leader
  RaftLog log_;                  // 内存日志
  uint64_t commit_index_{0};     // 已提交水位
  uint64_t last_applied_{0};     // 状态机已 apply 水位
  uint64_t apply_scheduled_{0};  // 已排进 Ready/同步 apply 的水位

  ProgressTracker progress_;  // 各 peer 复制进度
  Election election_;         // 选举
  Membership membership_;     // 成员 / Joint
  Replication replication_;   // 复制 / 快照
  LogPersist log_persist_;    // 落盘
  Apply apply_;               // apply / compact
  LinearRead linear_read_;    // 租约读 / ReadIndex
  ReadyPump ready_pump_;      // Ready 外提与出站攒批
  ReadyIo ready_io_;          // Ready 缓冲区

  bool activity_flag_{false};             // 自上次消费以来有活动
  std::atomic<bool> in_on_apply_{false};  // 防止 on_apply 重入 recover_apply
};

}  // namespace openkv::raft
