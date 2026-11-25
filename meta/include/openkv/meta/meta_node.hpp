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

#include <atomic>
#include <chrono>
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

#include "openkv/admin_acl.hpp"
#include "openkv/config.hpp"
#include "openkv/meta/cluster_state.hpp"
#include "openkv/meta/meta_fsm.hpp"
#include "openkv/meta/region_scheduler.hpp"
#include "openkv/meta/scaler.hpp"
#include "openkv/peer_counter.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/net/grpc_transport.hpp"
#include "openkv/raft/util/snap_stream.hpp"

namespace openkv::meta {

/* Meta 节点主对象：生命周期 start/stop；gRPC 由 MetaGrpcServer 挂载。 */
class MetaNode {
 public:
  /**
   * 用 bootstrap 配置构造（节点 ID、监听地址、Storage 初值、Meta peers 等）。
   * 构造时本地装载 bootstrap 快照到 ClusterState；之后变更一律走 Raft。
   * @param bootstrap 集群 bootstrap 配置
   */
  explicit MetaNode(ClusterBootstrap bootstrap);
  ~MetaNode();

  /**
   * 启动 Raft、transport、Scaler/Scheduler、gRPC、tick 线程。
   */
  void start();
  /**
   * 停止线程与监听；幂等。
   */
  void stop();

  /**
   * 本节点是否为 Meta Raft Leader（写路径门槛）。
   */
  bool is_leader() const;
  /**
   * 当前 Leader 的可拨号地址（Follower 填 redirect= 用）。
   */
  std::string leader_address() const;
  /**
   * 本节点 Meta ID（bootstrap.node_id）。
   */
  std::string self_id() const { return self_id_; }

  /**
   * 可变 ClusterState（单测 / 内部编排只读查询；生产写须 replicate）。
   */
  ClusterState& state() { return *state_; }
  /**
   * 扩容编排器；未 start 前可能为空。
   */
  Scaler* scaler() { return scaler_.get(); }
  /**
   * Region 迁移调度器；未 start 前可能为空。
   */
  RegionScheduler* scheduler() { return scheduler_.get(); }

  /**
   * 写路径：非 Leader → false 并填 *redirect_to；成功则多数派提交且本地已 apply。
   * @param cmd 待复制/应用的 Meta 命令
   * @param error 错误串；空表示成功
   * @param redirect_to 非 Leader 时写出 Leader 地址
   */
  bool replicate(const MetaCommand& cmd, std::string* error, std::string* redirect_to);

  /**
   * 写路径（异步）：仅 propose，不等待 apply。用于高扇出 HEARTBEAT / 幂等 re-REGISTER，
   * 避免 accept 线程在 Raft 提交上阻塞导致连接风暴拖垮 Leader。
   * @param cmd 待复制/应用的 Meta 命令
   * @param error 错误串；空表示成功
   * @param redirect_to 非 Leader 时写出 Leader 地址
   */
  bool replicate_async(const MetaCommand& cmd, std::string* error, std::string* redirect_to);

  /**
   * 写路径（回调）：propose 后不占调用线程；apply / 超时回调 `make_ok()` 或 ERR。
   * @param cmd 待复制/应用的 Meta 命令
   * @param make_ok apply 成功时生成 OK 正文的工厂
   * @param cb 完成回调
   */
  void replicate_cb(const MetaCommand& cmd, std::function<std::string()> make_ok,
                    std::function<void(std::string)> cb);

  /**
   * 便捷：注册节点（内部 MetaOp::kRegister）。
   * @param node 节点描述
   * @param error 错误串；空表示成功
   * @param redirect 非 Leader 时写出 Leader 地址
   */
  std::string register_node(NodeInfo node, std::string* error, std::string* redirect);
  /**
   * 异步注册（gRPC）：本地续租立即回调；需 Raft 时走 replicate_cb。
   * @param node 节点描述
   * @param cb 完成回调
   */
  void register_node_async(NodeInfo node, std::function<void(std::string)> cb);
  /**
   * 便捷：心跳（内部 MetaOp::kHeartbeat）。
   * @param node_id 节点 id
   * @param leaders 本节点担任 Leader 的 Region 列表
   * @param error 错误串；空表示成功
   * @param redirect 非 Leader 时写出 Leader 地址
   */
  bool heartbeat(const std::string& node_id, const std::vector<uint32_t>& leaders,
                 std::string* error, std::string* redirect);
  /**
   * 异步 ReportMinStartTs（gRPC）。
   * @param min_start_ts 活跃最小 start_ts
   * @param reporter 上报方标识（如 gateway / node_id）
   * @param cb 完成回调
   */
  void report_min_start_ts_async(uint64_t min_start_ts, std::string reporter,
                                 std::function<void(std::string)> cb);
  /**
   * 异步推进 GC SafePoint（retain=0 用集群默认）。
   * @param retain 相对 next_ts 保留跨度；0=默认
   * @param cb 完成回调（OK/ERR 行）
   */
  void advance_safe_point_async(uint32_t retain, std::function<void(std::string)> cb);
  /**
   * 异步仅 bump 路由版本。
   * @param cb 完成回调
   */
  void bump_route_async(std::function<void(std::string)> cb);
  /**
   * 异步分裂 Region。
   * @param left_region_id 左 Region id
   * @param split_key 分裂点（原始字节，非 hex）
   * @param cb 完成回调
   */
  void split_region_async(uint32_t left_region_id, std::string split_key,
                          std::function<void(std::string)> cb);
  /**
   * 异步合并相邻 Region。
   * @param left_region_id 左 Region id
   * @param right_region_id 右 Region id
   * @param cb 完成回调
   */
  void merge_regions_async(uint32_t left_region_id, uint32_t right_region_id,
                           std::function<void(std::string)> cb);
  /**
   * 异步原子替换 Region 副本集（PLACE）。
   * @param region_id Region id
   * @param peer_node_ids 期望副本 node_id 列表
   * @param cb 完成回调
   */
  void place_region_async(uint32_t region_id, std::vector<std::string> peer_node_ids,
                          std::function<void(std::string)> cb);
  /**
   * Drain / Offline / StoreDelete 等纯 SetNodeState（不含 Activate/Join 出站 Ping）。
   * @param node_id 节点 id
   * @param state 目标节点状态 / ClusterState 引用
   * @param ok_op 成功时响应里的操作名标签
   * @param cb 完成回调
   */
  void set_node_state_async(std::string node_id, NodeState state, std::string ok_op,
                            std::function<void(std::string)> cb);
  /**
   * 异步踢节点（Offline + 编排迁出）；详见实现。
   * @param node_id 目标节点
   * @param cb 完成回调
   */
  void kick_node_async(std::string node_id, std::function<void(std::string)> cb);
  /**
   * Activate/Join：异步 Ping + 心跳检查后 SetNodeState（不经 WorkPool）。
   * @param join true=Join 流程；false=Activate
   * @param node_id 节点 id
   * @param cb 完成回调
   */
  void activate_or_join_async(bool join, std::string node_id, std::function<void(std::string)> cb);
  /**
   * WATCH_ROUTE 异步：version 已新则立即回调；否则挂起至 bump / 超时（tick 扫）。
   * @param since 已知路由版本；大于此值才返回新表
   * @param timeout_ms 超时毫秒
   * @param cb 完成回调
   */
  void watch_route_async(uint64_t since, int timeout_ms, std::function<void(RoutingTable)> cb);
  /**
   * GET_SCALE 异步：CatchingUp 时异步 PeerProgress，其余同步拼行。
   * @param task_id 任务 id
   * @param cb 完成回调
   */
  void get_scale_task_async(std::string task_id, std::function<void(std::string)> cb);
  /**
   * 读路由表（Follower 可读，不强制 Leader）。
   * @param error 错误串；空表示成功
   * @param redirect 非 Leader 时写出 Leader 地址
   */
  RoutingTable routing_table(std::string* error, std::string* redirect) const;

  /**
   * 入站 RequestVote；返回行协议应答正文。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param last_idx 对方最后日志下标
   * @param last_term 对应 term
   * @return 应答正文
   */
  std::string handle_raft_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                               uint64_t last_term);
  /**
   * 入站 PreVote；返回行协议应答正文。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param last_idx 对方最后日志下标
   * @param last_term 对应 term
   * @return 应答正文
   */
  std::string handle_raft_pre_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term);
  /**
   * 入站 AppendEntries；返回行协议应答正文。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param prev_idx 前一条日志下标
   * @param prev_term 前一条日志 term
   * @param leader_commit Leader 已提交下标
   * @param entries 本轮追加的日志
   * @return 应答正文
   */
  std::string handle_raft_append(const std::string& from, uint64_t term, uint64_t prev_idx,
                                 uint64_t prev_term, uint64_t leader_commit,
                                 const std::vector<raft::LogEntry>& entries);
  /**
   * 入站整包 InstallSnapshot。
   * @param from 对端 peer id
   * @param term 对端任期
   * @param last_idx 快照覆盖到的日志下标
   * @param last_term 对应 term
   * @param data 快照载荷
   * @return 应答正文
   */
  std::string handle_raft_snapshot(const std::string& from, uint64_t term, uint64_t last_idx,
                                   uint64_t last_term, std::string_view data);
  /**
   * 分片快照流：拼装 snap_buf_ 后转 handle_raft_snapshot。
   * @param from 对端 peer id
   * @param term 任期
   * @param last_idx 对方最后日志下标
   * @param last_term 对应 term
   * @param offset 本分片在快照中的字节偏移
   * @param total 快照总字节数
   * @param done 是否最后一片 / 完成回调
   * @param chunk 快照分片载荷
   */
  std::string handle_raft_snap_chunk(const std::string& from, uint64_t term, uint64_t last_idx,
                                     uint64_t last_term, uint64_t offset, uint64_t total, bool done,
                                     std::string_view chunk);

  /**
   * TCP 行协议入口：PING/ROUTE/TSO/运维/Raft 文本 RPC 等。
   * @param line 一行协议正文（无尾换行亦可）
   * @param sess 连接级 Admin 会话；可空
   */
  std::string handle_line(const std::string& line, AdminSession* sess = nullptr);

  /**
   * 异步 AllocTs（GET_TS）：propose 后不占调用线程；回调在 apply / 超时路径。
   * @param count 分配/请求个数
   * @param cb 完成回调
   */
  void alloc_ts_async(uint32_t count, std::function<void(std::string)> cb);

  /* MetaService.Stats 快照（避免 header 依赖 meta.v1 pb 与 openkv::meta 歧义）。 */
  struct StatsSnapshot {
    uint64_t requests{0}, gw{0}, store{0}, raft{0}, sync{0}, admin{0}, client{0};  // 入站分类计数
    uint64_t meta_out{0}, to_store{0}, to_meta{0};                                 // 出站粗分
    uint64_t route_ver{0}, next_ts{0}, tso_alloc{0}, watch_waits{0}, migrates{0};  // 状态水位
    std::vector<std::pair<std::string, uint64_t>> peer_raft, peer_sync, peer_raft_out,
        peer_sync_out, peer_store;  // 按对端拆开
  };
  /**
   * 采集 STATS 用计数快照（线程安全读原子/计数器）。
   * @return 当前计数视图
   */
  StatsSnapshot stats_snapshot() const;

  /**
   * 只读 bootstrap 配置。
   */
  const ClusterBootstrap& bootstrap() const { return bootstrap_; }

 private:
  // Meta 自身只用单个 Raft 组，shard 固定为 0（与 Multi-Raft Storage 区分）。
  static constexpr uint32_t kMetaShard = 0;

  /**
   * Raft 已提交日志回调：decode → apply_meta_command；唤醒 replicate 等待。
   * @param index 日志下标
   * @param data 载荷
   */
  void on_apply(uint64_t index, const std::string& data);
  /**
   * 周期 tick：raft_->tick()、GC advance、H2 sweep_stale。
   */
  void tick_loop();
  /**
   * 向 Storage 发一行协议；跟随 not_leader redirect（最多数跳）。
   * @param addr 对端地址 host:port
   * @param req 请求正文（行协议或类型化映射前）
   */
  std::string storage_rpc(std::string addr, const std::string& req) const;
  /**
   * 路由表中 Region 可拨号地址：优先 is_leader → 非 learner → 首个副本。
   * @param region_id Region id
   */
  std::optional<std::string> region_leader_addr(uint32_t region_id) const;
  /**
   * A2/A3：编码 `OK ROUTE …`（不要求本节点是 Leader）。
   * @param rt 路由表
   */
  std::string format_route(const RoutingTable& rt) const;
  /**
   * 唤醒到期 / 版本已新的 watch_route_async 回调；force_all 用于 stop。
   * @param force_all true 则唤醒全部挂起 watch（stop 用）
   */
  void fire_route_watches(bool force_all = false);

  ClusterBootstrap bootstrap_;                  // 启动配置副本
  std::string self_id_;                         // 本 Meta 节点 ID
  std::string listen_addr_;                     // gRPC 监听 host:port
  std::unique_ptr<ClusterState> state_;         // 权威集群状态
  std::unique_ptr<Scaler> scaler_;              // 在线扩容编排（start 后创建）
  std::unique_ptr<RegionScheduler> scheduler_;  // Region 迁移调度（start 后创建）
  std::unique_ptr<raft::RaftNode> raft_;        // Meta 单组 Raft
  raft::GrpcRaftTransport transport_;           // 向其他 Meta 发 Raft（gRPC）
  raft::SnapStreamBuffer snap_buf_;             // 入站分片快照拼装缓冲
  std::vector<raft::Peer> peers_;               // Meta Raft 同伴列表

  std::atomic<bool> running_{false};                   // start/stop 门闩
  std::thread tick_thread_;                            // tick_loop
  std::unique_ptr<class MetaGrpcServer> grpc_server_;  // 默认 gRPC 监听
  mutable std::mutex apply_mu_;                        // propose 等待 apply 的条件同步
  std::condition_variable apply_cv_;                   // apply / AllocTs 唤醒（替 sleep 轮询）
  uint64_t last_applied_wait_{0};                      // 已观察到的 apply index（配合等待）
  // AllocTs apply 结果：raft index → 分配起点（避免 GET_TS 跨 Raft 等待持锁）。
  std::unordered_map<uint64_t, uint64_t> ts_alloc_by_index_;
  // 异步 GET_TS waiters：raft index → 回调列表
  std::unordered_map<uint64_t, std::vector<std::function<void(std::string)>>> ts_async_waiters_;
  /* 通用写 apply waiters（REGISTER / ReportMinStart / …） */
  struct ApplyWaiter {
    std::function<void(std::string)> cb;   // 完成回调
    std::function<std::string()> make_ok;  // apply 成功时拼 OK 正文
  };
  std::unordered_map<uint64_t, std::vector<ApplyWaiter>> apply_waiters_;
  mutable std::mutex route_mu_;  // WATCH_ROUTE
  std::condition_variable route_cv_;
  uint64_t route_version_notify_{0};  // 最近一次路由版本（on_apply 更新）
  /* gRPC watch_route_async 挂起表（行协议仍用 route_cv_）。 */
  struct RouteWatch {
    uint64_t id{0};                                    // 本地序号
    uint64_t since{0};                                 // 客户端已知 version
    std::chrono::steady_clock::time_point deadline{};  // 超时点
    std::function<void(RoutingTable)> cb;              // 完成回调
  };
  std::vector<RouteWatch> route_watches_;
  std::atomic<uint64_t> route_watch_seq_{0};
  uint32_t gc_advance_ticks_{0};          // tick 计数：满则 Leader ADVANCE_SAFE_POINT
  uint32_t stale_sweep_ticks_{0};         // tick 计数：满则 Leader SWEEP_STALE
  bool was_leader_{false};                // 用于检测新晋 Leader，宽限租约
  uint32_t leader_lease_grace_ticks_{0};  // 新 Leader 前若干 tick 不 sweep Offline
  uint32_t store_ttl_ms_{10000};          // Storage 心跳超时（SKV_META_STORE_TTL_MS）
  std::string admin_token_;               // SKV_ADMIN_TOKEN；空 = ACL 关闭

  // ---- 入站流量计数（STATS / METRICS）----
  std::atomic<uint64_t> req_total_{0};          // 入站请求总数（含分类累计）
  std::atomic<uint64_t> req_gw_{0};             // Gateway：ROUTE / TSO / LOCATE …
  std::atomic<uint64_t> req_store_{0};          // Storage：HEARTBEAT / REGISTER …
  std::atomic<uint64_t> req_raft_{0};           // Meta Raft 心跳 / 投票
  std::atomic<uint64_t> req_sync_{0};           // Meta Raft 带日志复制 / 快照
  std::atomic<uint64_t> req_admin_{0};          // 运维：SPLIT / PLACE / LIST …
  PeerCounter peer_raft_;                       // 按 from 统计 Raft 入站
  PeerCounter peer_sync_;                       // 按 from 统计同步入站
  PeerCounter peer_store_;                      // HEARTBEAT/REGISTER 来源 node_id
  std::atomic<uint64_t> tso_alloc_total_{0};    // 累计分配的时间戳个数
  std::atomic<uint64_t> route_watch_waits_{0};  // WATCH_ROUTE 阻塞等待次数
};

}  // namespace openkv::meta
