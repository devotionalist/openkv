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

// @file meta_node.hpp
// Meta 节点：Raft 多副本 + 强一致状态机；对外由 MetaGrpcServer 暴露 gRPC。
//
// 职责：
// - 持有 ClusterState，所有写经 replicate → Raft → on_apply → apply_meta_command
// - 暴露 gRPC：路由/TSO/心跳/运维（SPLIT/PLACE/DRAIN/…）与 Meta 组内 Raft RPC 同端口
// - 编排 Scaler（扩容）与 RegionScheduler（迁移）；向 Storage 发 ADMIN_*（storage_rpc）
// - O1：连接级 AUTH（admin_token_）；读路径与 Raft/心跳不强制鉴权

#pragma once

#include "openkv/admin_acl.hpp"
#include "openkv/config.hpp"
#include "openkv/meta/cluster_state.hpp"
#include "openkv/meta/meta_fsm.hpp"
#include "openkv/meta/region_scheduler.hpp"
#include "openkv/meta/scaler.hpp"
#include "openkv/peer_counter.hpp"
#include "openkv/raft/grpc_transport.hpp"
#include "openkv/raft/raft_node.hpp"
#include "openkv/raft/snap_stream.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace openkv::meta {

/// Meta 节点主对象：生命周期 start/stop；gRPC 由 MetaGrpcServer 挂载。
class MetaNode {
 public:
  /// 用 bootstrap 配置构造（节点 ID、监听地址、Storage 初值、Meta peers 等）。
  /// 构造时本地装载 bootstrap 快照到 ClusterState；之后变更一律走 Raft。
  explicit MetaNode(ClusterBootstrap bootstrap);
  ~MetaNode();

  /// 启动 Raft、transport、Scaler/Scheduler、gRPC、tick 线程。
  void start();
  /// 停止线程与监听；幂等。
  void stop();

  /// 本节点是否为 Meta Raft Leader（写路径门槛）。
  bool is_leader() const;
  /// 当前 Leader 的可拨号地址（Follower 填 redirect= 用）。
  std::string leader_address() const;
  /// 本节点 Meta ID（bootstrap.node_id）。
  std::string self_id() const { return self_id_; }

  /// 可变 ClusterState（单测 / 内部编排只读查询；生产写须 replicate）。
  ClusterState& state() { return *state_; }
  /// 扩容编排器；未 start 前可能为空。
  Scaler* scaler() { return scaler_.get(); }
  /// Region 迁移调度器；未 start 前可能为空。
  RegionScheduler* scheduler() { return scheduler_.get(); }

  /// 写路径：非 Leader → false 并填 *redirect_to；成功则多数派提交且本地已 apply。
  /// @param error 失败原因（非 redirect）
  bool replicate(const MetaCommand& cmd, std::string* error, std::string* redirect_to);

  /// 写路径（异步）：仅 propose，不等待 apply。用于高扇出 HEARTBEAT / 幂等 re-REGISTER，
  /// 避免 accept 线程在 Raft 提交上阻塞导致连接风暴拖垮 Leader。
  bool replicate_async(const MetaCommand& cmd, std::string* error, std::string* redirect_to);

  /// 写路径（回调）：propose 后不占调用线程；apply / 超时回调 `make_ok()` 或 ERR。
  void replicate_cb(const MetaCommand& cmd, std::function<std::string()> make_ok,
                    std::function<void(std::string)> cb);

  /// 便捷：注册节点（内部 MetaOp::kRegister）。
  std::string register_node(NodeInfo node, std::string* error, std::string* redirect);
  /// 异步注册（gRPC）：本地续租立即回调；需 Raft 时走 replicate_cb。
  void register_node_async(NodeInfo node, std::function<void(std::string)> cb);
  /// 便捷：心跳（内部 MetaOp::kHeartbeat）。
  bool heartbeat(const std::string& node_id, const std::vector<uint32_t>& leaders,
                 std::string* error, std::string* redirect);
  /// 异步 ReportMinStartTs（gRPC）。
  void report_min_start_ts_async(uint64_t min_start_ts, std::string reporter,
                                 std::function<void(std::string)> cb);
  void advance_safe_point_async(uint32_t retain, std::function<void(std::string)> cb);
  void bump_route_async(std::function<void(std::string)> cb);
  void split_region_async(uint32_t left_region_id, std::string split_key,
                          std::function<void(std::string)> cb);
  void merge_regions_async(uint32_t left_region_id, uint32_t right_region_id,
                           std::function<void(std::string)> cb);
  void place_region_async(uint32_t region_id, std::vector<std::string> peer_node_ids,
                          std::function<void(std::string)> cb);
  /// Drain / Offline / StoreDelete 等纯 SetNodeState（不含 Activate/Join 出站 Ping）。
  void set_node_state_async(std::string node_id, NodeState state, std::string ok_op,
                            std::function<void(std::string)> cb);
  void kick_node_async(std::string node_id, std::function<void(std::string)> cb);
  /// 读路由表（Follower 可读，不强制 Leader）。
  RoutingTable routing_table(std::string* error, std::string* redirect) const;

  /// ---- Meta 组内 Raft RPC（由 handle_line 解析后转发）----
  std::string handle_raft_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                               uint64_t last_term);
  std::string handle_raft_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term);
  std::string handle_raft_append(const std::string& from, uint64_t term, uint64_t prev_idx,
                                 uint64_t prev_term, uint64_t leader_commit,
                                 const std::vector<raft::LogEntry>& entries);
  std::string handle_raft_snapshot(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term, std::string_view data);
  /// 分片快照流：拼装 snap_buf_ 后转 handle_raft_snapshot。
  std::string handle_raft_snap_chunk(const std::string& from, uint64_t term, uint64_t last_idx,
                                     uint64_t last_term, uint64_t offset, uint64_t total, bool done,
                                     std::string_view chunk);

  /// TCP 行协议入口：PING/ROUTE/TSO/运维/Raft 文本 RPC 等。
  /// @param sess 连接级 AdminSession；nullptr 时用临时会话（单测 / 无 AUTH）
  std::string handle_line(const std::string& line, AdminSession* sess = nullptr);

  /// 异步 AllocTs（GET_TS）：propose 后不占调用线程；回调在 apply / 超时路径。
  void alloc_ts_async(uint32_t count, std::function<void(std::string)> cb);

  /// MetaService.Stats 快照（避免 header 依赖 meta.v1 pb 与 openkv::meta 歧义）。
  struct StatsSnapshot {
    uint64_t requests{0}, gw{0}, store{0}, raft{0}, sync{0}, admin{0}, client{0};
    uint64_t meta_out{0}, to_store{0}, to_meta{0};
    uint64_t route_ver{0}, next_ts{0}, tso_alloc{0}, watch_waits{0}, migrates{0};
    std::vector<std::pair<std::string, uint64_t>> peer_raft, peer_sync, peer_raft_out, peer_sync_out,
        peer_store;
  };
  StatsSnapshot stats_snapshot() const;

  /// 只读 bootstrap 配置。
  const ClusterBootstrap& bootstrap() const { return bootstrap_; }

 private:
  /// Meta 自身只用单个 Raft 组，shard 固定为 0（与 Multi-Raft Storage 区分）。
  static constexpr uint32_t kMetaShard = 0;

  /// Raft 已提交日志回调：decode → apply_meta_command；唤醒 replicate 等待。
  void on_apply(uint64_t index, const std::string& data);
  /// 周期 tick：raft_->tick()、GC advance、H2 sweep_stale。
  void tick_loop();
  /// 向 Storage 发一行协议；跟随 not_leader redirect（最多数跳）。
  std::string storage_rpc(std::string addr, const std::string& req) const;
  /// 路由表中 Region 可拨号地址：优先 is_leader → 非 learner → 首个副本。
  std::optional<std::string> region_leader_addr(uint32_t region_id) const;
  /// A2/A3：编码 `OK ROUTE …`（不要求本节点是 Leader）。
  std::string format_route(const RoutingTable& rt) const;

  ClusterBootstrap bootstrap_;              ///< 启动配置副本
  std::string self_id_;                     ///< 本 Meta 节点 ID
  std::string listen_addr_;                 ///< gRPC 监听 host:port
  std::unique_ptr<ClusterState> state_;     ///< 权威集群状态
  std::unique_ptr<Scaler> scaler_;          ///< 在线扩容编排（start 后创建）
  std::unique_ptr<RegionScheduler> scheduler_; ///< Region 迁移调度（start 后创建）
  std::unique_ptr<raft::RaftNode> raft_;    ///< Meta 单组 Raft
  raft::GrpcRaftTransport transport_;      ///< 向其他 Meta 发 Raft（gRPC）
  raft::SnapStreamBuffer snap_buf_;         ///< 入站分片快照拼装缓冲
  std::vector<raft::Peer> peers_;           ///< Meta Raft 同伴列表

  std::atomic<bool> running_{false};        ///< start/stop 门闩
  std::thread tick_thread_;                 ///< tick_loop
  std::unique_ptr<class MetaGrpcServer> grpc_server_;  ///< 默认 gRPC 监听
  mutable std::mutex apply_mu_;             ///< propose 等待 apply 的条件同步
  std::condition_variable apply_cv_;        ///< apply / AllocTs 唤醒（替 sleep 轮询）
  uint64_t last_applied_wait_{0};           ///< 已观察到的 apply index（配合等待）
  /// AllocTs apply 结果：raft index → 分配起点（避免 GET_TS 跨 Raft 等待持锁）。
  std::unordered_map<uint64_t, uint64_t> ts_alloc_by_index_;
  /// 异步 GET_TS waiters：raft index → 回调列表
  std::unordered_map<uint64_t, std::vector<std::function<void(std::string)>>> ts_async_waiters_;
  /// 通用写 apply waiters（REGISTER / ReportMinStart / …）
  struct ApplyWaiter {
    std::function<void(std::string)> cb;
    std::function<std::string()> make_ok;
  };
  std::unordered_map<uint64_t, std::vector<ApplyWaiter>> apply_waiters_;
  mutable std::mutex route_mu_;             ///< WATCH_ROUTE
  std::condition_variable route_cv_;
  uint64_t route_version_notify_{0};        ///< 最近一次路由版本（on_apply 更新）
  uint32_t gc_advance_ticks_{0};            ///< tick 计数：满则 Leader ADVANCE_SAFE_POINT
  uint32_t stale_sweep_ticks_{0};           ///< tick 计数：满则 Leader SWEEP_STALE
  bool was_leader_{false};                  ///< 用于检测新晋 Leader，宽限租约
  uint32_t leader_lease_grace_ticks_{0};    ///< 新 Leader 前若干 tick 不 sweep Offline
  uint32_t store_ttl_ms_{10000};            ///< Storage 心跳超时（SKV_META_STORE_TTL_MS）
  std::string admin_token_;                 ///< SKV_ADMIN_TOKEN；空 = ACL 关闭

  // ---- 入站流量计数（STATS / METRICS）----
  std::atomic<uint64_t> req_total_{0};      ///< 入站请求总数（含分类累计）
  std::atomic<uint64_t> req_gw_{0};         ///< Gateway：ROUTE / TSO / LOCATE …
  std::atomic<uint64_t> req_store_{0};      ///< Storage：HEARTBEAT / REGISTER …
  std::atomic<uint64_t> req_raft_{0};       ///< Meta Raft 心跳 / 投票
  std::atomic<uint64_t> req_sync_{0};       ///< Meta Raft 带日志复制 / 快照
  std::atomic<uint64_t> req_admin_{0};      ///< 运维：SPLIT / PLACE / LIST …
  PeerCounter peer_raft_;                   ///< 按 from 统计 Raft 入站
  PeerCounter peer_sync_;                   ///< 按 from 统计同步入站
  PeerCounter peer_store_;                  ///< HEARTBEAT/REGISTER 来源 node_id
  std::atomic<uint64_t> tso_alloc_total_{0};///< 累计分配的时间戳个数
  std::atomic<uint64_t> route_watch_waits_{0}; ///< WATCH_ROUTE 阻塞等待次数
};

}  // namespace openkv::meta
