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

// @file storage_node.hpp
// 存储节点：Multi-Raft（多副本）+ 分片引擎；出站传输归 MultiRaft。

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "openkv/config.hpp"
#include "openkv/peer_counter.hpp"
#include "openkv/raft/core/multi_raft.hpp"
#include "openkv/raft/core/raft_rpc.hpp"
#include "openkv/raft/core/types.hpp"
#include "openkv/raft/engine/db_raft_engine.hpp"
#include "openkv/raft/util/snap_stream.hpp"
#include "openkv/raftstore/index_waiter.hpp"
#include "openkv/region.hpp"
#include "openkv/storage/coprocessor.hpp"
#include "openkv/storage/kv_engine.hpp"
#include "openkv/storage/latency_hist.hpp"
#include "openkv/storage/scheduler.hpp"
#include "openkv/storage/watch_hub.hpp"

namespace openkv::storage {

/* 单机存储节点：托管多 Region Raft、引擎、Admin/分裂与 Meta 心跳。 */
class StorageNode {
 public:
  /**
   * 构造未启动节点；真正拉起在 start()。
   * @param cfg 节点配置（listen、data_dir、peers 等）
   */
  explicit StorageNode(NodeConfig cfg);
  /**
   * 析构时若仍 running 则 stop。
   */
  ~StorageNode();

  /**
   * 启动 MultiRaft、调度器、tick/心跳/分裂后台线程。
   */
  void start();
  /**
   * 停服并 join 后台线程；析构前应调用。
   */
  void stop();

  /**
   * 单条 committed 日志 apply 到引擎（含 Watch 推送）。
   * @param shard_id 分片 / Region id
   * @param index 日志下标
   * @param data 日志载荷（Command 编码）
   */
  void on_apply(uint32_t shard_id, uint64_t index, const std::string& data);
  /**
   * 同 Region 一批 committed entries：可批的 ONEPC/Put/Delete 合并为一次 WriteBatch。
   * @param shard_id 分片 / Region id
   * @param entries 日志条目
   */
  void on_apply_entries(uint32_t shard_id, const std::vector<openkv::raft::LogEntry>& entries);

  /**
   * 阻塞至 apply invoke 或超时。tick 用于等待期间推进 Ready。
   * @param index 等待的日志下标
   * @param timeout 超时
   * @param tick 等待期间可选回调（推进 Ready）；默认可空
   * @return 等待结果（完成 / 超时）
   */
  openkv::raftstore::IndexWaiter::WaitOutcome wait_apply(uint64_t index,
                                                         std::chrono::milliseconds timeout,
                                                         const std::function<void()>& tick = {});

  /**
   * 异步等 apply：不占调用线程；回调在 invoke / 超时线程上。
   * @param index 日志下标
   * @param timeout 超时
   * @param cb 完成回调
   */
  void wait_apply_async(uint64_t index, std::chrono::milliseconds timeout,
                        openkv::raftstore::IndexWaiter::CompleteCb cb);

  /**
   * propose→apply 等待延迟 + 超时（Prometheus）。
   * @return Prometheus 文本
   */
  std::string apply_wait_prometheus() const;

  /**
   * 本机 Region 加 learner。
   * @param shard_id 分片 / Region id
   * @param peer 新 learner
   * @return 是否成功
   */
  bool add_learner(uint32_t shard_id, const raft::Peer& peer);
  /**
   * learner 升 voter。
   * @param shard_id 分片 / Region id
   * @param peer_id 目标 peer id
   * @return 是否成功
   */
  bool promote_learner(uint32_t shard_id, const std::string& peer_id);
  /**
   * 从成员中移除 peer。
   * @param shard_id 分片 / Region id
   * @param peer_id 目标 peer id
   * @return 是否成功
   */
  bool remove_peer(uint32_t shard_id, const std::string& peer_id);

  /**
   * 确保本机托管该 Region；peers 非空时用于首次创建（可含 learner）。
   * @param region_id Region id
   * @param peers 初始成员；空则按已有/Meta 解析
   * @return 文本结果（OK / 错误）
   */
  std::string admin_ensure_region(uint32_t region_id, const std::vector<raft::Peer>& peers);
  /**
   * Admin：加 learner。
   * @param region_id Region id
   * @param peer 新 learner
   * @return 文本结果
   */
  std::string admin_add_learner(uint32_t region_id, const raft::Peer& peer);
  /**
   * Admin：learner 升 voter。
   * @param region_id Region id
   * @param peer_id 目标 peer id
   * @return 文本结果
   */
  std::string admin_promote(uint32_t region_id, const std::string& peer_id);
  /**
   * Admin：移除 peer。
   * @param region_id Region id
   * @param peer_id 目标 peer id
   * @return 文本结果
   */
  std::string admin_remove_peer(uint32_t region_id, const std::string& peer_id);
  /**
   * 本机停止托管 Region（place 踢出后防脑裂）。
   * @param region_id Region id
   * @return 文本结果
   */
  std::string admin_destroy_region(uint32_t region_id);
  /**
   * Leader：OK PROGRESS match commit last caught_up(0|1) learner(0|1)
   * @param region_id Region id
   * @param peer_id peer id
   * @return 进度文本
   */
  std::string admin_peer_progress(uint32_t region_id, const std::string& peer_id);
  /**
   * Leader 让位并通知 target 竞选：OK TRANSFER <target>
   * @param region_id Region id
   * @param target_id 目标 Leader peer id
   * @return 文本结果
   */
  std::string admin_transfer_leader(uint32_t region_id, const std::string& target_id);
  /**
   * 本机对该 Region 发起竞选。
   * @param region_id Region id
   * @return 文本结果
   */
  std::string admin_campaign(uint32_t region_id);

  /**
   * MultiRaft 句柄（propose / peer 管理）。
   * @return MultiRaft 引用
   */
  raft::MultiRaft& multi_raft() { return *multi_raft_; }
  /**
   * 分片引擎管理器。
   * @return 管理器引用
   */
  ShardEngineManager& engines() { return *engines_; }
  /**
   * `start()` 后必有；propose 走 multi_raft().store().async_write。
   * @return 调度器引用
   */
  Scheduler& scheduler() { return *scheduler_; }
  /**
   * Apply 路径近期变更环缓。
   * @return WatchHub 引用
   */
  WatchHub& watch_hub() { return watch_hub_; }
  /**
   * Coprocessor：对本机 Region range scan（key/value 前缀、keys_only、count_only）。
   * @param region_id Region id
   * @param req 扫描参数
   * @return 扫描结果
   */
  CoprocessorResponse coprocessor_execute(uint32_t region_id, const CoprocessorRequest& req);
  /**
   * 节点配置。
   * @return 配置常量引用
   */
  const NodeConfig& config() const { return cfg_; }
  /**
   * 出站 gRPC（`start()` 后；归属 MultiRaft）。
   * @return 传输引用
   */
  raft::GrpcRaftTransport& transport() { return multi_raft_->transport(); }
  /**
   * 出站 gRPC（只读）。
   * @return 传输常量引用
   */
  const raft::GrpcRaftTransport& transport() const { return multi_raft_->transport(); }

  /**
   * gRPC 入站 RequestVote。
   * @param shard 分片 / Region id
   * @param from 对端 peer id
   * @param term 任期
   * @param last_idx 对方最后日志下标
   * @param last_term 对应 term
   * @return 结构体回包（填 proto，不拼 OK 行）
   */
  openkv::raft::RaftReply handle_raft_vote(uint32_t shard, const std::string& from, uint64_t term,
                                           uint64_t last_idx, uint64_t last_term);
  /**
   * gRPC 入站 PreVote。
   * @param shard 分片 / Region id
   * @param from 对端 peer id
   * @param term 任期
   * @param last_idx 对方最后日志下标
   * @param last_term 对应 term
   * @return 结构体回包
   */
  openkv::raft::RaftReply handle_raft_pre_vote(uint32_t shard, const std::string& from,
                                               uint64_t term, uint64_t last_idx,
                                               uint64_t last_term);
  /**
   * gRPC 入站 AppendEntries。
   * @param shard 分片 / Region id
   * @param from 对端 peer id
   * @param term 任期
   * @param prev_idx 前一条日志下标
   * @param prev_term 前一条 term
   * @param leader_commit Leader 提交水位
   * @param entries 本轮日志
   * @return 结构体回包
   */
  openkv::raft::RaftReply handle_raft_append(uint32_t shard, const std::string& from, uint64_t term,
                                             uint64_t prev_idx, uint64_t prev_term,
                                             uint64_t leader_commit,
                                             std::vector<openkv::raft::LogEntry> entries);
  /**
   * gRPC 入站整包快照。
   * @param shard 分片 / Region id
   * @param from 对端 peer id
   * @param term 任期
   * @param last_idx 快照覆盖到的日志下标
   * @param last_term 对应 term
   * @param data 快照载荷
   * @return 结构体回包
   */
  openkv::raft::RaftReply handle_raft_snapshot(uint32_t shard, const std::string& from,
                                               uint64_t term, uint64_t last_idx, uint64_t last_term,
                                               std::string_view data);
  /**
   * 流式快照分片：RAFT_SNAPC；未收齐返回 OK SNAPC，收齐后同 handle_raft_snapshot。
   * @param shard 分片 / Region id
   * @param from 对端 peer id
   * @param term 任期
   * @param last_idx 快照覆盖到的日志下标
   * @param last_term 对应 term
   * @param offset 本片起始偏移
   * @param total 总字节数
   * @param done 是否最后一片
   * @param chunk 本片数据
   * @return 应答文本
   */
  std::string handle_raft_snap_chunk(uint32_t shard, const std::string& from, uint64_t term,
                                     uint64_t last_idx, uint64_t last_term, uint64_t offset,
                                     uint64_t total, bool done, std::string_view chunk);

  /**
   * Leader id → 可达地址（供 redirect）。
   * @param node_id peer / 节点 id
   * @return 地址串；未知为空
   */
  std::string peer_address(const std::string& node_id) const;

  /**
   * 分裂流量暂停：拒绝 left 上写路径，直到 admin_split 完成。
   * @param left_region 左 Region id
   * @param split_key 分界键
   * @return 文本结果
   */
  std::string admin_prepare_split(uint32_t left_region, std::string_view split_key);
  /**
   * Range 分裂：先 seed 右 Region 成员，再 Raft propose migrate。
   * @param left_region 左 Region id
   * @param right_region 右 Region id
   * @param split_key 分界键
   * @return 文本结果
   */
  std::string admin_split(uint32_t left_region, uint32_t right_region, std::string_view split_key);
  /**
   * Range 合并：把 right 全部 key 并入 left。
   * @param left_region 左 Region id
   * @param right_region 右 Region id
   * @return 文本结果
   */
  std::string admin_merge(uint32_t left_region, uint32_t right_region);
  /**
   * 解析 Region 成员（本机 Raft → Meta → bootstrap）。
   * @param region_id Region id
   * @return peer 列表
   */
  std::vector<raft::Peer> resolve_region_peers(uint32_t region_id);

  /**
   * 若 Region 处于分裂暂停窗口，返回 true（写应失败 splitting）。
   * @param region_id Region id
   * @return true 表示写应拒绝
   */
  bool region_splitting(uint32_t region_id) const;
  /**
   * Leader tick：清理过期锁 + 按 Meta GC safe point 删 MVCC 旧版本。
   * @param region_id Region id
   */
  void maybe_txn_gc(uint32_t region_id);
  /**
   * 向 Meta 拉 GC safe point；失败返回 0。
   * @return safe_ts；失败为 0
   */
  uint64_t fetch_gc_safe_ts();
  /**
   * 周期性检查阈值，触发 Meta SPLIT + 全副本 ADMIN_SPLIT。
   * @param region_id Region id
   */
  void maybe_auto_split(uint32_t region_id);

  /**
   * 完整 place 编排（与 skv place 同路径）；供 ADMIN_PLACE_REGION / scatter。
   * @param region_id Region id
   * @param desired_peers 期望成员 peer id 列表
   * @return 是否成功
   */
  bool place_region(uint32_t region_id, const std::vector<std::string>& desired_peers);
  /**
   * Admin 入口：place Region 到 desired 成员。
   * @param region_id Region id
   * @param desired 期望 peer id 列表
   * @return 文本结果
   */
  std::string admin_place_region(uint32_t region_id, const std::vector<std::string>& desired);

  /**
   * Region epoch：分裂/合并/place 递增；txn 路径可校验。
   * @param region_id Region id
   * @return 当前 epoch（默认 1）
   */
  uint64_t region_epoch(uint32_t region_id) const;
  /**
   * 设置 Region epoch（内存；可再 persist）。
   * @param region_id Region id
   * @param epoch 新 epoch
   */
  void set_region_epoch(uint32_t region_id, uint64_t epoch);
  /**
   * Region epoch +1。
   * @param region_id Region id
   */
  void bump_region_epoch(uint32_t region_id);
  /**
   * 本机 Region 区间（Split 后同机共享引擎，用区间过滤）。
   * @param region_id Region id
   * @return 半开区间
   */
  openkv::KeyRange region_range(uint32_t region_id) const;
  /**
   * 设置本机 Region 区间。
   * @param region_id Region id
   * @param range 半开区间
   */
  void set_region_range(uint32_t region_id, openkv::KeyRange range);
  /**
   * key 是否落在 Region 区间内。
   * @param region_id Region id
   * @param key 用户键
   * @return true 表示属于该 Region
   */
  bool key_in_region(uint32_t region_id, std::string_view key) const;
  /**
   * Admin：设置 epoch（可入 Raft）。
   * @param region_id Region id
   * @param epoch 新 epoch
   * @return 文本结果
   */
  std::string admin_set_epoch(uint32_t region_id, uint64_t epoch);
  /**
   * E3：对本机 Region 引擎做 Checkpoint；path 空则 data_dir/ckpt_region_<id>_<ts>。
   * @param region_id Region id
   * @param path 目标目录；空=自动生成
   * @return 文本结果
   */
  std::string admin_checkpoint(uint32_t region_id, std::string_view path = {});
  /**
   * 本节点发往 Meta 的出站 RPC 累计次数（STATS meta_out）。
   * @return 累计次数
   */
  uint64_t meta_out_count() const { return meta_out_.load(std::memory_order_relaxed); }
  /**
   * Meta 出站按对端地址编码的计数串。
   * @return 编码文本
   */
  std::string meta_out_peers_encode() const { return meta_out_peers_.encode(); }
  /* AdminService.Stats 快照（避免 header 依赖 storage.v1 pb 与 openkv::storage 歧义）。 */
  struct StatsSnapshot {
    uint64_t requests{0}, gw{0}, raft{0}, sync{0}, admin{0}, meta_out{0}, client{0};  // 各类计数
    uint64_t qdepth{0}, qdrop{0};                                  // 队列深度 / 丢弃
    std::vector<std::pair<std::string, uint64_t>> peer_raft_out;   // peer → Raft 出站
    std::vector<std::pair<std::string, uint64_t>> peer_sync_out;   // peer → sync 出站
    std::vector<std::pair<std::string, uint64_t>> meta_out_peers;  // Meta 地址 → 出站
  };
  /**
   * 采集 Admin Stats 快照。
   * @return 计数快照
   */
  StatsSnapshot stats_snapshot() const;

 private:
  /**
   * Raft tick 后台循环。
   */
  void tick_loop();
  /**
   * 自动分裂工作线程：消费 split_queue_。
   */
  void split_worker_loop();
  /**
   * 向 Meta 注册并周期性心跳。
   */
  void register_and_heartbeat();
  /**
   * REGISTER 后按 Meta 路由 ENSURE 本机应托管的全部 Region（含 split 产物）。
   */
  void recover_local_regions_from_meta();
  /**
   * 确保本机已创建/挂载该 Region。
   * @param region_id Region id
   */
  void ensure_region(uint32_t region_id);
  /**
   * 调 Meta 做 SPLIT；成功写出 right_out。
   * @param left 左 Region
   * @param split_key 分界键
   * @param right_out 输出右 Region id
   * @return 是否成功
   */
  bool meta_split(uint32_t left, const std::string& split_key, uint32_t* right_out);
  /**
   * 向全体副本广播 ADMIN_SPLIT。
   * @param left 左 Region
   * @param right 右 Region
   * @param split_key 分界键
   */
  void broadcast_admin_split(uint32_t left, uint32_t right, const std::string& split_key);
  /**
   * split 成功后可选 Place（SKV_PD_AUTO_BALANCE=1 且 RF>1）。
   * @param left 左 Region
   * @param right 右 Region
   */
  void maybe_place_after_split(uint32_t left, uint32_t right);
  /**
   * S2：在左 Region 全体成员上 ENSURE 右 Region（同成员集），再允许 migrate。
   * @param left_region 左 Region
   * @param right_region 右 Region
   * @return 错误串；空表示成功
   */
  std::string seed_right_region(uint32_t left_region, uint32_t right_region);

  NodeConfig cfg_;                               // 节点配置
  std::unique_ptr<ShardEngineManager> engines_;  // 分片引擎
  // 共享 RaftEngine（data_dir/raft RocksDB）。
  std::shared_ptr<raft::DbRaftEngine> raft_engine_;
  std::unique_ptr<raft::MultiRaft> multi_raft_;  // Multi-Raft
  std::unique_ptr<Scheduler> scheduler_;         // latch 调度
  WatchHub watch_hub_;                           // 内存 Watch 环缓
  raft::SnapStreamBuffer snap_buf_;              // 流式快照收齐缓冲
  std::atomic<bool> running_{false};             // start/stop
  std::atomic<uint64_t> meta_out_{0};            // 本节点 → Meta 出站 RPC
  PeerCounter meta_out_peers_;                   // Meta 地址 → 出站次数
  std::thread tick_thread_;                      // Raft tick 线程
  std::thread split_thread_;                     // 自动分裂线程
  std::thread hb_thread_;                        // Meta 心跳线程
  std::vector<raft::Peer> bootstrap_peers_;      // 启动成员
  size_t auto_split_keys_{256000};               // 默认约 256k keys；SKV_SPLIT_KEYS=0 关
  size_t auto_split_bytes_{96ull << 20};         // 96MiB；SKV_SPLIT_BYTES=0 关
  size_t auto_merge_keys_{0};                    // 0=关；SKV_MERGE_KEYS
  std::mutex split_mu_;                          // 保护分裂队列
  std::condition_variable split_cv_;             // 分裂任务唤醒
  std::deque<uint32_t> split_queue_;             // 待自动分裂 Region
  std::unordered_set<uint32_t> split_queued_;    // 已入队防重
  std::unordered_map<uint32_t, std::chrono::steady_clock::time_point> last_auto_split_at_;  // 限频
  std::mutex create_mu_;           // 串行 ENSURE/创建
  std::mutex place_mu_;            // 串行 place
  mutable std::mutex destroy_mu_;  // 保护 destroyed_regions_
  std::set<uint32_t> placing_;     // 进行中的 place，防重入
  // DESTROY 后拒绝 RAFT get_or_create，直到 ENSURE 清除（防踢出后再被迟到 AE 复活）。
  std::set<uint32_t> destroyed_regions_;
  std::unordered_map<uint32_t, std::vector<raft::Peer>> pending_peers_;  // 待挂成员
  /**
   * Region 是否已被 DESTROY（禁止迟到 AE 复活）。
   * @param region_id Region id
   * @return true 表示已销毁
   */
  bool region_destroyed(uint32_t region_id) const;
  /**
   * 标记 Region 已 DESTROY。
   * @param region_id Region id
   */
  void mark_region_destroyed(uint32_t region_id);
  /**
   * ENSURE 时清除 DESTROY 标记。
   * @param region_id Region id
   */
  void clear_region_destroyed(uint32_t region_id);
  /**
   * REGISTER / 自描述地址：优先 advertise_addr，否则 listen（0.0.0.0→127.0.0.1）。
   * @return 对外地址
   */
  std::string advertise_address() const;
  mutable std::mutex epoch_mu_;                                   // 保护 region_epochs_
  std::unordered_map<uint32_t, uint64_t> region_epochs_;          // Region → epoch；default 1
  mutable std::mutex range_mu_;                                   // 保护 region_ranges_
  std::unordered_map<uint32_t, openkv::KeyRange> region_ranges_;  // Region → 区间

  /* 分裂暂停窗口：active 时拒绝该 Region 写。 */
  struct SplitPause {
    bool active{false};     // 是否暂停写
    std::string split_key;  // 分界键（诊断用）
  };
  mutable std::mutex split_pause_mu_;                      // 保护 split_pauses_
  std::unordered_map<uint32_t, SplitPause> split_pauses_;  // Region → 暂停状态

  /**
   * 设置/清除分裂暂停。
   * @param region_id Region id
   * @param on true=暂停写
   * @param split_key 分界键；清除时可空
   */
  void set_split_pause(uint32_t region_id, bool on, std::string_view split_key = {});
  /**
   * 向副本广播 PREPARE_SPLIT。
   * @param left 左 Region
   * @param split_key 分界键
   */
  void broadcast_prepare_split(uint32_t left, const std::string& split_key);
  /**
   * S1：已编码 Admin/业务 Command 经 Raft propose 并等待 apply。
   * @param shard_id 分片 / Region id
   * @param encoded 已编码日志 data
   * @param error 输出错误串；空表示成功
   * @param redirect 输出建议跳转地址
   * @return true 表示 propose+apply 成功
   */
  bool propose_encoded(uint32_t shard_id, std::string encoded, std::string* error,
                       std::string* redirect);
  /**
   * 把 epoch 持久化到引擎 meta key。
   * @param region_id Region id
   * @param epoch 新 epoch
   */
  void persist_epoch(uint32_t region_id, uint64_t epoch);
  /**
   * 从引擎 meta 读 epoch；无则返回默认。
   * @param region_id Region id
   * @return epoch
   */
  uint64_t load_epoch_from_engine(uint32_t region_id);
  /**
   * 通知 IndexWaiter：某 index 已 apply（可带错误）。
   * @param index 日志下标
   * @param error 错误串；空=成功
   */
  void publish_apply_result(uint64_t index, std::string error);

  openkv::raftstore::IndexWaiter index_waiter_;  // propose 等待 apply
  LatencyHistogram apply_wait_hist_;             // propose→apply 延迟

  /**
   * 周期性检查是否可与右邻合并。
   * @param region_id Region id
   */
  void maybe_auto_merge(uint32_t region_id);
  /**
   * 向 Meta 要推荐放置节点。
   * @param out 输出推荐 peer id
   * @param avoid 避开的 peer id
   * @return 是否成功
   */
  bool meta_suggest_place(std::vector<std::string>* out, const std::vector<std::string>& avoid);
  /**
   * 调 Meta 合并 left+right。
   * @param left 左 Region
   * @param right 右 Region
   * @return 是否成功
   */
  bool meta_merge(uint32_t left, uint32_t right);
  /**
   * 向 Meta 查 left 的可合并右邻。
   * @param left 左 Region
   * @param right_out 输出右 Region id
   * @return 是否找到
   */
  bool meta_find_merge_right(uint32_t left, uint32_t* right_out);
  /**
   * 列出 Meta 上的存储节点 id→地址。
   * @param id_to_addr 输出映射
   * @return 是否成功
   */
  bool meta_list_storage(std::unordered_map<std::string, std::string>* id_to_addr);
  /**
   * 调 Meta 更新 Region 放置。
   * @param region_id Region id
   * @param peers 期望成员
   * @return 是否成功
   */
  bool meta_place(uint32_t region_id, const std::vector<std::string>& peers);
  /**
   * 从 Meta 拉 Region 成员与 Leader。
   * @param region_id Region id
   * @param out 输出 (peer_id, addr) 列表
   * @param leader_id 输出 Leader peer id
   * @return 是否成功
   */
  bool meta_region_peers(uint32_t region_id, std::vector<std::pair<std::string, std::string>>* out,
                         std::string* leader_id);
  /**
   * 发一条 Meta RPC。
   * @param req 请求文本
   * @return 应答文本
   */
  std::string meta_rpc(const std::string& req);
  /**
   * 向种子地址发 Admin RPC（尝试多个）。
   * @param req 请求文本
   * @param seed_addrs 候选地址
   * @return 应答文本
   */
  std::string admin_rpc(const std::string& req, const std::vector<std::string>& seed_addrs);
  /**
   * 等待 peer 追上 Leader（place 用）。
   * @param region_id Region id
   * @param peer_id 目标 peer
   * @param seed_addrs 可查询的 Admin 地址
   * @return 是否在超时内追上
   */
  bool wait_peer_caught_up(uint32_t region_id, const std::string& peer_id,
                           const std::vector<std::string>& seed_addrs);
  /**
   * 限频 CAMPAIGN：仅当无主或距上次 ≥ min_gap；避免 place 重试打出选举风暴。
   * @param region_id Region id
   * @param addr 目标 Admin 地址
   * @param min_gap 最小间隔；默认 800ms
   * @return 是否真正发起了竞选
   */
  bool maybe_campaign(uint32_t region_id, const std::string& addr,
                      std::chrono::milliseconds min_gap = std::chrono::milliseconds(800));

  mutable std::mutex campaign_mu_;  // 保护 last_campaign_at_
  std::unordered_map<uint32_t, std::chrono::steady_clock::time_point> last_campaign_at_;  // 限频
};

}  // namespace openkv::storage
