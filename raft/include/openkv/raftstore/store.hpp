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

// @file store.hpp
// 职责：raftstore **入口**——Router + BatchSystem + Peer 表 + StoreMeta + SnapWorker。
// 架构：所有入站 Raft RPC / Tick / propose 后的 Ready 都汇聚于此；组 commit（ready_coalesce_*）。
// 协作：MultiRaft 持有 Store；persist_fn→RaftNode::persist_ready；apply_*→KV；
//       transport_fn→调用方注入的发送能力（生产为 GrpcRaftTransport::bind；快照改包在 Store
//       内完成）。
// 注意：handle_raft_* 禁止绕过 Store 直调 RaftNode::on_*；入站用 apply_inbound_raft。

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "openkv/raftstore/batch_system.hpp"
#include "openkv/raftstore/peer.hpp"
#include "openkv/raftstore/persist_worker.hpp"
#include "openkv/raftstore/router.hpp"
#include "openkv/raftstore/snap_worker.hpp"
#include "openkv/raftstore/store_meta.hpp"

namespace openkv::raftstore {

/* 本机 Multi-Raft 运行时中枢：调度、Ready 攒批、入站 RPC、Peer 生命周期。 */
class Store {
 public:
  /* 调度、Hibernate、Ready 攒批与 persist 流水线参数。 */
  struct Options {
    BatchSystem::Options batch;         // Raft/Apply worker 池参数
    SnapWorker::Options snap;           // 出站快照通道参数
    uint32_t hibernate_after_ms{5000};  // Follower 空闲超过该毫秒进入 Hibernate（0=禁用）
    uint32_t unload_after_ms{0};        // Hibernate 后再空闲该毫秒可卸协议状态（0=禁用）
    size_t mailbox_capacity{4096};      // 每 Region 邮箱容量
    uint32_t ready_coalesce_n{32};      // Ready group-commit：攒满 N 条再 flush（≤1 关闭）
    uint32_t ready_coalesce_ms{1};      // Ready group-commit：最长等待 ms（0=仅按条数）
    size_t persist_workers{2};          // 异步 persist IO 线程数（0=同步落盘）
  };

  using NodeFactory = std::function<std::shared_ptr<raft::RaftNode>(uint32_t region_id)>;
  using ApplyFn = std::function<void(uint32_t region_id, uint64_t index, const std::string& data)>;
  // 同 Region 一批 committed entries（可合并 WriteBatch）；未设置则退回逐条 ApplyFn。
  using ApplyBatchFn =
      std::function<void(uint32_t region_id, const std::vector<raft::LogEntry>& entries)>;
  using PersistFn = std::function<void(uint32_t region_id, const raft::Ready& rd)>;
  using TransportFn = std::function<void(uint32_t region_id, const std::string& to,
                                         const std::string& type, const std::string& payload)>;
  using SnapProviderFn =
      std::function<std::string(uint32_t region_id, uint64_t* last_idx, uint64_t* last_term)>;
  using ReloadFn = std::function<std::shared_ptr<raft::RaftNode>(uint32_t region_id)>;

  /**
   * 构造未启动的 Store；须再 set_* 回调后 start。
   * @param opts 调度 / 攒批 / persist 参数
   * @param factory 按 region 创建 RaftNode
   */
  Store(Options opts, NodeFactory factory);
  ~Store();

  /**
   * 注入逐条 apply 回调（无 ApplyBatchFn 时使用）。
   * @param f region + index + 日志 data
   */
  void set_apply_fn(ApplyFn f) { apply_fn_ = std::move(f); }
  /**
   * 注入批量 apply；优先于逐条 ApplyFn，便于合并写盘。
   * @param f 同 Region 一批已提交条目
   */
  void set_apply_batch_fn(ApplyBatchFn f) { apply_batch_fn_ = std::move(f); }
  /**
   * 注入 Ready 落盘（通常 RaftNode::persist_ready）。
   * @param f region + Ready
   */
  void set_persist_fn(PersistFn f) { persist_fn_ = std::move(f); }
  /**
   * 注入出站 RPC 发送能力。
   * @param f region + 目标 + 类型 + 载荷
   */
  void set_transport_fn(TransportFn f) { transport_fn_ = std::move(f); }
  /**
   * 注入出站快照数据生成（SnapWorker Provider）。
   * @param f 返回快照字节，并写出 last_idx/last_term
   */
  void set_snap_provider(SnapProviderFn f) { snap_provider_ = std::move(f); }
  /**
   * 冷卸载后再加载：重建 RaftNode。
   * @param f 按 region 重新构造节点
   */
  void set_reload_fn(ReloadFn f) { reload_fn_ = std::move(f); }

  /**
   * 启动 BatchSystem / SnapWorker / ready_coalesce 线程；tick / Ready 前必须调用。
   */
  void start();
  /**
   * 停掉所有后台线程；之后不可再 schedule/drive。
   */
  void stop();

  /**
   * StoreTicker：向所有 Peer 邮箱投递 Tick（须已 start；由 BatchSystem 处理）。
   */
  void schedule_tick();
  /**
   * 仅驱动指定 Region 的 Ready（propose 后；邮箱 kWake，批内合并 persist/sync；须已 start）。
   * @param region_id Region id
   */
  void drive_ready(uint32_t region_id);
  /**
   * propose 热路径：按 ready_coalesce_n/ms 攒批后再 drive_ready（group commit）。
   * @param region_id Region id
   */
  void request_ready(uint32_t region_id);
  /**
   * 写入口：Leader propose + request_ready；不等 apply（由调用方 wait_apply*）。
   * 失败时写 error：no_region / not_leader / propose_failed。
   * @param region_id Region id
   * @param data 写入载荷（通常已编码的 KV 命令）
   * @param assigned_index 成功时写出分配到的日志下标；可为 nullptr
   * @param error 失败时写出原因；空表示成功；默认 nullptr
   * @return 是否 propose 成功
   */
  bool async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                   std::string* error = nullptr);

  /**
   * 查已加载 Peer 的 RaftNode；无则 nullptr。
   * @param region_id Region id
   * @return 裸指针或 nullptr
   */
  raft::RaftNode* get(uint32_t region_id);
  /**
   * 查已加载 Peer 的 shared_ptr；无则空。
   * @param region_id Region id
   * @return shared_ptr 或空
   */
  std::shared_ptr<raft::RaftNode> get_shared(uint32_t region_id);
  /**
   * 取已有或经工厂创建 Peer/RaftNode。
   * @param region_id Region id
   * @return 可写引用
   */
  raft::RaftNode& get_or_create(uint32_t region_id);
  /**
   * 取出 shared_ptr 并从 peers_ 移除。
   * @param region_id Region id
   * @return 取出的节点；不存在则空
   */
  std::shared_ptr<raft::RaftNode> take(uint32_t region_id);
  /**
   * 销毁本机 Peer（邮箱一并注销）。
   * @param region_id Region id
   * @return 原先是否存在
   */
  bool erase(uint32_t region_id);
  /**
   * 标记活跃，推迟 Hibernate / 冷卸载。
   * @param region_id Region id
   */
  void mark_hot(uint32_t region_id);

  /**
   * 向指定 Region 邮箱投递一条 RaftMessage（出站回环或本机注入）。
   * @param region_id Region id
   * @param msg 已填好的 Raft RPC
   * @return 是否入队成功
   */
  bool send_raft_message(uint32_t region_id, RaftMessage msg);
  /**
   * 入站 Raft RPC 唯一步进入口（与 Router 邮箱同路径；同步返回行协议响应）。
   * StorageNode::handle_raft_* 禁止直调 RaftNode::on_*。
   * @param region_id Region id
   * @param msg 入站消息
   * @return 行协议响应串（空表示无需回包或失败路径自理）
   */
  std::string apply_inbound_raft(uint32_t region_id, RaftMessage msg);
  /**
   * 本机自认 Leader 的 Region 列表。
   * @return region_id 集合
   */
  std::vector<uint32_t> leader_shards() const;
  /**
   * 本机已注册的全部 Region。
   * @return region_id 集合
   */
  std::vector<uint32_t> all_shards() const;

  /**
   * 本机 Region 元数据表。
   * @return StoreMeta 引用
   */
  StoreMeta& meta() { return meta_; }
  /**
   * 本机 Region 元数据表只读。
   * @return StoreMeta 常量引用
   */
  const StoreMeta& meta() const { return meta_; }
  /**
   * 邮箱路由器。
   * @return Router 引用
   */
  Router& router() { return *router_; }
  /**
   * 双池调度器。
   * @return BatchSystem 引用
   */
  BatchSystem& batch_system() { return *batch_; }

  /**
   * 同步 range/epoch 到 PeerMeta 与 StoreMeta。
   * @param region_id Region id
   * @param range key 范围
   * @param epoch 路由纪元
   */
  void update_peer_meta(uint32_t region_id, openkv::KeyRange range, uint64_t epoch);
  /**
   * Apply 邮箱是否过深（QoS 背压）。
   * @param region_id Region id
   * @return true 表示应拒绝/降速新写入
   */
  bool apply_queue_overloaded(uint32_t region_id) const;

 private:
  /**
   * Raft worker：一批 PeerMsg 的入口（Tick / RPC / Wake / PersistDone）。
   * @param region_id Region id
   * @param msgs 本批消息
   */
  void on_raft_batch(uint32_t region_id, std::vector<PeerMsg> msgs);
  /**
   * Apply worker：一批 ApplyControl（ApplyMsg / SnapTask）。
   * @param region_id Region id
   * @param msgs 本批控制消息
   */
  void on_apply_batch(uint32_t region_id, std::vector<ApplyControl> msgs);
  /**
   * 处理 Tick；run_ready=false 时只 tick，由调用方在批末统一 process_ready。
   * @param peer 目标 Peer
   * @param run_ready 是否立刻驱动 Ready
   */
  void handle_tick(Peer& peer, bool run_ready);
  /**
   * 只 step / 唤醒；Ready 一律由 on_raft_batch 批末合并。
   * @param peer 目标 Peer
   * @param msg 入站 Raft RPC
   */
  void handle_raft_message(Peer& peer, const RaftMessage& msg);
  /**
   * 真正 step 入站消息；run_ready 控制是否立刻 Ready（入站同步路径常 true）。
   * @param peer 目标 Peer
   * @param msg 入站消息
   * @param run_ready 是否立刻 process_ready
   * @return 行协议响应
   */
  std::string step_raft_message(Peer& peer, const RaftMessage& msg, bool run_ready);
  /**
   * 取 Ready 并落盘/发网/投递 Apply。
   * pipeline=true：persist 丢 PersistWorker，完成后 kPersistDone 再发网/apply。
   * pipeline=false：入站 RPC 同步落盘后再回包（耐久与响应一致）。
   * @param peer 目标 Peer
   * @param pipeline 是否走持久化流水线
   */
  void process_ready(Peer& peer, bool pipeline);
  /**
   * 落盘完成后的发网 + Apply 投递 + advance。
   * @param peer 目标 Peer
   * @param rd 已持久化的 Ready
   */
  void dispatch_ready_after_persist(Peer& peer, raft::Ready& rd);
  /**
   * kPersistDone：取出 pending Ready 走 dispatch。
   * @param peer 目标 Peer
   */
  void finish_persist_pipeline(Peer& peer);
  /**
   * 按空闲时长决定 Hibernate 或冷卸载。
   * @param peer 目标 Peer
   */
  void maybe_hibernate_or_unload(Peer& peer);
  /**
   * 从 Hibernate/卸载态拉回可服务。
   * @param peer 目标 Peer
   */
  void wake_peer(Peer& peer);
  /**
   * 持 mu_ 查 Peer；不存在返回空。
   * @param region_id Region id
   * @return shared_ptr 或空
   */
  std::shared_ptr<Peer> peer_locked(uint32_t region_id);
  /**
   * 取 Peer 上的 RaftNode；未加载返回 nullptr。
   * @param peer 目标 Peer
   * @return 裸指针或 nullptr
   */
  raft::RaftNode* peer_node(Peer& peer);
  /**
   * 是否启用异步 persist 流水线。
   * @return persist_ 存在且 persist_workers>0
   */
  bool persist_pipeline_enabled() const { return persist_ && opts_.persist_workers > 0; }

  Options opts_;                  // 调度参数
  NodeFactory factory_;           // 创建 RaftNode
  ApplyFn apply_fn_;              // 逐条 apply
  ApplyBatchFn apply_batch_fn_;   // 批量 apply
  PersistFn persist_fn_;          // Ready 落盘
  TransportFn transport_fn_;      // 出站 RPC
  SnapProviderFn snap_provider_;  // 出站快照数据
  ReloadFn reload_fn_;            // 冷卸载后再加载

  std::shared_ptr<Router> router_;          // 邮箱路由
  std::unique_ptr<BatchSystem> batch_;      // 双池调度
  std::unique_ptr<SnapWorker> snap_;        // 出站快照通道
  std::unique_ptr<PersistWorker> persist_;  // Ready 落盘线程池
  StoreMeta meta_;                          // 本机 Region 元数据

  mutable std::mutex mu_;                                      // 保护 peers_
  std::unordered_map<uint32_t, std::shared_ptr<Peer>> peers_;  // region → Peer

  /* propose 后 Ready 攒批状态（按 Region）。 */
  struct CoalesceState {
    uint32_t pending{0};                               // 已积压的 request_ready 次数
    bool armed{false};                                 // 是否已排入 coalesce 等待
    std::chrono::steady_clock::time_point deadline{};  // 最晚 flush 时刻
  };
  /**
   * ready_coalesce 后台循环：到期或满 N 则 drive_ready。
   */
  void coalesce_loop();
  std::mutex coalesce_mu_;                                // 保护 coalesce_
  std::condition_variable coalesce_cv_;                   // 有 pending / 停机
  std::unordered_map<uint32_t, CoalesceState> coalesce_;  // region → 攒批状态
  std::atomic<bool> coalesce_running_{false};             // coalesce 线程生命周期
  std::thread coalesce_thr_;                              // 攒批线程
};

}  // namespace openkv::raftstore
