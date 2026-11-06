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

// @file meta_node.cpp
// MetaNode 实现：
// - 构造：本地 bootstrap ClusterState；读 SKV_ADMIN_TOKEN / SKV_META_STORE_TTL_MS
// - start：建 Raft + Scaler/Scheduler 钩子（storage_rpc）+ accept/tick
// - replicate：Leader propose → 等 on_apply；Follower 填 redirect
// - handle_line：行协议分发（读可走 Follower；写走 replicate；运维需 AUTH）
// - tick_loop：raft tick + 周期 ADVANCE_SAFE_POINT / SWEEP_STALE

#include "opskv/meta/meta_node.hpp"

#include "opskv/admin_acl.hpp"
#include "opskv/protocol.hpp"
#include "opskv/codec.hpp"
#include "opskv/grpc_auth.hpp"
#include "opskv/logging.hpp"
#include "opskv/meta/grpc_server.hpp"
#include "opskv/net/tcp_line_client.hpp"

#include <grpcpp/grpcpp.h>
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace opskv::meta {
namespace proto = opskv::proto;
namespace {

using opskv::from_hex;
using opskv::to_hex;

std::vector<std::string> split_ws(const std::string& line) {
  std::vector<std::string> parts;
  std::istringstream iss(line);
  std::string p;
  while (iss >> p) parts.push_back(p);
  return parts;
}



}  // namespace

// 装载 bootstrap 拓扑到 state_；尚未 start Raft/监听。
MetaNode::MetaNode(ClusterBootstrap bootstrap) : bootstrap_(std::move(bootstrap)) {
  self_id_ = bootstrap_.node_id.empty() ? "meta-0" : bootstrap_.node_id;
  listen_addr_ = bootstrap_.listen_addr.empty() ? bootstrap_.meta_addr : bootstrap_.listen_addr;
  if (listen_addr_.empty()) {
    listen_addr_ = "0.0.0.0:8500";
  }

  state_ = std::make_unique<ClusterState>();
  std::vector<NodeInfo> storages;
  for (const auto& p : bootstrap_.storage_peers) {
    NodeInfo n;
    n.node_id = p.node_id;
    n.address = p.address;
    n.role = NodeRole::kStorage;
    // 实际 Active/Joining 由 bootstrap_range 按 RF 切分
    n.state = NodeState::kJoining;
    storages.push_back(n);
  }
  if (storages.empty()) {
    storages.push_back({"storage-0", NodeRole::kStorage, "127.0.0.1:9100", NodeState::kJoining, 0});
  }
  // 各副本本地装载相同 bootstrap 快照；前 RF 个 Active，其余 Joining；之后变更一律走 Raft
  state_->bootstrap(bootstrap_.shard_count == 0 ? 8 : bootstrap_.shard_count,
                    bootstrap_.replica_factor == 0 ? 3 : bootstrap_.replica_factor, storages);
  admin_token_ = admin_token_from_env();
  if (const char* e = std::getenv(proto::env::kMetaStoreTtlMs)) {
    try {
      unsigned long v = std::stoul(e);
      if (v >= 1000 && v < 3600000) store_ttl_ms_ = static_cast<uint32_t>(v);
    } catch (...) {
    }
  }
}

MetaNode::~MetaNode() { stop(); }

bool MetaNode::is_leader() const { return raft_ && raft_->is_leader(); }

std::string MetaNode::leader_address() const {
  return raft_ ? raft_->leader_address() : std::string{};
}

// Raft 提交回调：解码并 apply；更新 last_applied_wait_ 唤醒 replicate。
void MetaNode::on_apply(uint64_t index, const std::string& data) {
  MetaCommand cmd;
  if (!decode_meta_command(data, &cmd)) {
    SKV_WARN << "meta bad raft cmd index=" << index;
    return;
  }
  apply_meta_command(*state_, cmd);
  std::lock_guard<std::mutex> lock(apply_mu_);
  if (cmd.op == MetaOp::kAllocTs) {
    const uint32_t n = cmd.ts_count == 0 ? 1 : cmd.ts_count;
    ts_alloc_by_index_[index] = state_->next_ts() - n;
  }
  last_applied_wait_ = index;
}

// 写入口：非 Leader 立即 redirect；Leader propose 后自旋等到 apply index。
bool MetaNode::replicate(const MetaCommand& cmd, std::string* error, std::string* redirect_to) {
  if (!raft_) {
    if (error) *error = "raft_not_ready";
    return false;
  }
  if (!raft_->is_leader()) {
    if (redirect_to) *redirect_to = raft_->leader_address();
    if (error) *error = "not_leader";
    return false;
  }
  uint64_t idx = 0;
  if (!raft_->propose(encode_meta_command(cmd), &idx)) {
    if (error) *error = "propose_failed";
    return false;
  }
  // 最长约 5s；给多数派 AE 留余量，避免与 Store HB 抢队列时误报 apply_timeout。
  for (int i = 0; i < 1000; ++i) {
    {
      std::lock_guard<std::mutex> lock(apply_mu_);
      if (last_applied_wait_ >= idx) {
        return true;
      }
    }
    if (i % 10 == 9) {
      raft_->tick();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  if (error) *error = "apply_timeout";
  return false;
}

bool MetaNode::replicate_async(const MetaCommand& cmd, std::string* error,
                                 std::string* redirect_to) {
  if (!raft_) {
    if (error) *error = "raft_not_ready";
    return false;
  }
  if (!raft_->is_leader()) {
    if (redirect_to) *redirect_to = raft_->leader_address();
    if (error) *error = "not_leader";
    return false;
  }
  uint64_t idx = 0;
  if (!raft_->propose(encode_meta_command(cmd), &idx)) {
    if (error) *error = "propose_failed";
    return false;
  }
  return true;
}

std::string MetaNode::register_node(NodeInfo node, std::string* error, std::string* redirect) {
  MetaCommand cmd;
  cmd.op = MetaOp::kRegister;
  cmd.node = std::move(node);
  if (!replicate(cmd, error, redirect)) {
    return {};
  }
  return cmd.node.node_id.empty() ? state_->get_node("")->node_id : cmd.node.node_id;
}

bool MetaNode::heartbeat(const std::string& node_id, const std::vector<uint32_t>& leaders,
                           std::string* error, std::string* redirect) {
  if (!raft_ || !raft_->is_leader()) {
    if (redirect) *redirect = raft_ ? raft_->leader_address() : "";
    if (error) *error = "not_leader";
    return false;
  }
  if (!state_->get_node(node_id)) {
    if (error) *error = "unknown_node";
    return false;
  }
  // 无 *L 变更：仅 Leader 本地续租（H2 list_stale 只看 Leader 租约）。
  // 有路由 Leader 变更才进 Raft，避免 6 Store × 2s 心跳把 TSO/AE 打死。
  if (!state_->heartbeat_changes_leaders(node_id, leaders)) {
    if (!state_->touch_lease(node_id)) {
      if (error) *error = "unknown_node";
      return false;
    }
    return true;
  }
  MetaCommand cmd;
  cmd.op = MetaOp::kHeartbeat;
  cmd.node.node_id = node_id;
  cmd.leader_shards = leaders;
  return replicate_async(cmd, error, redirect);
}

RoutingTable MetaNode::routing_table(std::string* error, std::string* redirect) const {
  // A3：路由读允许 Follower（已 apply 的 ClusterState；相对 Leader 有界陈旧）
  (void)error;
  (void)redirect;
  return state_->routing_table();
}

// 编码 `OK ROUTE ver=… regions=…`（hex range + 副本 node@addr[*L]）。
std::string MetaNode::format_route(const RoutingTable& rt) const {
  std::ostringstream oss;
  oss << "OK ROUTE " << rt.version << ' ' << rt.regions.size() << ' ' << rt.regions.size();
  for (const auto& s : rt.regions) {
    oss << ' ' << s.region_id << ':' << to_hex(s.range.start_key) << ':'
        << to_hex(s.range.end_key) << ':' << s.epoch << ':' << s.replicas.size();
    for (const auto& r : s.replicas) {
      oss << ',' << r.node_id << '@' << r.address << (r.is_leader ? "*L" : "")
          << (r.is_learner ? "*N" : "");
    }
  }
  return oss.str();
}

std::string MetaNode::handle_raft_vote(const std::string& from, uint64_t term, uint64_t last_idx,
                                         uint64_t last_term) {
  bool granted = false;
  raft_->on_request_vote(from, term, last_idx, last_term, &granted);
  auto st = raft_->soft_state();
  return "OK VOTE " + std::to_string(st.term) + " " + (granted ? "1" : "0");
}

std::string MetaNode::handle_raft_pre_vote(const std::string& from, uint64_t term,
                                             uint64_t last_idx, uint64_t last_term) {
  bool granted = false;
  raft_->on_request_pre_vote(from, term, last_idx, last_term, &granted);
  auto st = raft_->soft_state();
  return "OK PREVOTE " + std::to_string(st.term) + " " + (granted ? "1" : "0");
}

std::string MetaNode::handle_raft_append(const std::string& from, uint64_t term, uint64_t prev_idx,
                                           uint64_t prev_term, uint64_t leader_commit,
                                           const std::vector<raft::LogEntry>& entries) {
  bool success = false;
  uint64_t match = 0;
  raft_->on_append_entries(from, term, prev_idx, prev_term, entries, leader_commit, &success,
                           &match);
  auto st = raft_->soft_state();
  return "OK AE " + std::to_string(st.term) + " " + (success ? "1" : "0") + " " +
         std::to_string(match);
}

std::string MetaNode::handle_raft_snapshot(const std::string& from, uint64_t term,
                                             uint64_t last_idx, uint64_t last_term,
                                             std::string_view data) {
  bool success = false;
  raft_->on_install_snapshot(from, term, last_idx, last_term, data, &success);
  auto st = raft_->soft_state();
  return "OK SNAP " + std::to_string(st.term) + " " + (success ? "1" : "0") + " " +
         std::to_string(last_idx);
}

std::string MetaNode::handle_raft_snap_chunk(const std::string& from, uint64_t term,
                                               uint64_t last_idx, uint64_t last_term,
                                               uint64_t offset, uint64_t total, bool done,
                                               std::string_view chunk) {
  raft::SnapStreamBuffer::Key key{from, kMetaShard, last_idx};
  bool complete = false;
  std::string data;
  uint64_t out_term = 0, out_last_term = 0;
  if (!snap_buf_.append(key, term, last_term, offset, total, done, chunk, &complete, &data,
                        &out_term, &out_last_term)) {
    snap_buf_.drop(key);
    auto st = raft_->soft_state();
    return "ERR snapc " + std::to_string(st.term);
  }
  if (!complete) {
    auto st = raft_->soft_state();
    return "OK SNAPC " + std::to_string(st.term) + " 1 " + std::to_string(offset + chunk.size());
  }
  return handle_raft_snapshot(from, out_term, last_idx, out_last_term, data);
}

// TCP/单测统一入口：AUTH → ACL 门闩 → 按 op 分发读写/Raft/运维。
std::string MetaNode::handle_line(const std::string& line, AdminSession* sess) {
  AdminSession local;
  AdminSession& session = sess ? *sess : local;
  auto parts = split_ws(line);
  if (parts.empty()) return "ERR empty";
  const auto& op = parts[0];
  if (op == proto::kOpAuth) {
    if (parts.size() < 2) return "ERR usage: AUTH <token>";
    if (!admin_try_auth(&session, admin_token_, parts[1])) return "ERR unauthorized";
    return "OK AUTH";
  }
  if (meta_needs_admin(op) && !admin_allowed(session, admin_token_)) {
    return "ERR unauthorized";
  }
  if (op == proto::kOpStats) {
    const auto pr = peer_raft_.encode();
    const auto ps = peer_sync_.encode();
    const auto pst = peer_store_.encode();
    const auto pro = transport_.encode_out_raft();
    const auto pso = transport_.encode_out_sync();
    const uint64_t raft_all = req_raft_.load() + transport_.out_raft_total();
    const uint64_t sync_all = req_sync_.load() + transport_.out_sync_total();
    auto rt = state_->routing_table();
    return "OK STATS requests=" + std::to_string(req_total_.load() + transport_.out_raft_total() +
                                                 transport_.out_sync_total()) +
           " gw=" + std::to_string(req_gw_.load()) +
           " store=" + std::to_string(req_store_.load()) +
           " raft=" + std::to_string(raft_all) +
           " sync=" + std::to_string(sync_all) +
           " admin=" + std::to_string(req_admin_.load()) +
           " client=" + std::to_string(req_gw_.load() + req_store_.load()) +
           " route_ver=" + std::to_string(rt.version) +
           " next_ts=" + std::to_string(state_->next_ts()) +
           " tso_alloc=" + std::to_string(tso_alloc_total_.load()) +
           " watch_waits=" + std::to_string(route_watch_waits_.load()) +
           " migrates=" + std::to_string(state_->list_migrates().size()) +
           (pr.empty() ? "" : (" peer_raft=" + pr)) +
           (ps.empty() ? "" : (" peer_sync=" + ps)) +
           (pro.empty() ? "" : (" peer_raft_out=" + pro)) +
           (pso.empty() ? "" : (" peer_sync_out=" + pso)) +
           (pst.empty() ? "" : (" peer_store=" + pst));
  }
  // O2：Prometheus 文本（多行；末行 END）
  if (op == proto::kOpMetrics) {
    auto rt = state_->routing_table();
    auto soft = raft_ ? raft_->soft_state() : raft::SoftState{};
    std::ostringstream oss;
    oss << "# HELP skv_meta_route_version Meta routing table version\n"
        << "# TYPE skv_meta_route_version gauge\n"
        << "skv_meta_route_version " << rt.version << '\n'
        << "# TYPE skv_meta_next_ts gauge\n"
        << "skv_meta_next_ts " << state_->next_ts() << '\n'
        << "# TYPE skv_meta_gc_safe_ts gauge\n"
        << "skv_meta_gc_safe_ts " << state_->gc_safe_ts() << '\n'
        << "# TYPE skv_meta_tso_alloc_total counter\n"
        << "skv_meta_tso_alloc_total " << tso_alloc_total_.load() << '\n'
        << "# TYPE skv_meta_route_watch_waits_total counter\n"
        << "skv_meta_route_watch_waits_total " << route_watch_waits_.load() << '\n'
        << "# TYPE skv_meta_migrate_tasks gauge\n"
        << "skv_meta_migrate_tasks " << state_->list_migrates().size() << '\n'
        << "# TYPE skv_meta_raft_term gauge\n"
        << "skv_meta_raft_term " << soft.term << '\n'
        << "# TYPE skv_meta_raft_commit gauge\n"
        << "skv_meta_raft_commit " << soft.commit_index << '\n'
        << "# TYPE skv_meta_is_leader gauge\n"
        << "skv_meta_is_leader " << (is_leader() ? 1 : 0) << '\n'
        << "END\n";
    return oss.str();
  }
  if (op != "PING" && op != "WHO_LEADER") {
    req_total_.fetch_add(1, std::memory_order_relaxed);
    if (op == proto::kOpRaftVote || op == proto::kOpRaftPreVote) {
      req_raft_.fetch_add(1, std::memory_order_relaxed);
      if (parts.size() >= 3) peer_raft_.add(parts[2]);
    } else if (op == proto::kOpRaftAe) {
      uint64_t n = 0;
      if (parts.size() >= 8) {
        try {
          n = std::stoull(parts[7]);
        } catch (...) {
          n = 0;
        }
      }
      const std::string from = parts.size() >= 3 ? parts[2] : "";
      if (n > 0) {
        req_sync_.fetch_add(1, std::memory_order_relaxed);
        peer_sync_.add(from);
      } else {
        req_raft_.fetch_add(1, std::memory_order_relaxed);
        peer_raft_.add(from);
      }
    } else if (op.rfind(proto::kRaftSnapPrefix, 0) == 0 || op == proto::kOpRaftSnapshot) {
      req_sync_.fetch_add(1, std::memory_order_relaxed);
      if (parts.size() >= 3) peer_sync_.add(parts[2]);
    } else if (op == "HEARTBEAT" || op == "REGISTER" || op == "REPORT_MIN_START_TS" ||
               op == "REPORT_MIN_START") {
      if (op == "HEARTBEAT" || op == "REGISTER") {
        req_store_.fetch_add(1, std::memory_order_relaxed);
        if (parts.size() >= 2) peer_store_.add(parts[1]);
      } else {
        req_gw_.fetch_add(1, std::memory_order_relaxed);
      }
    } else if (op == "GET_TS" || op == "GET_ROUTE" || op == "WATCH_ROUTE" || op == "LOCATE" ||
               op == "GET_SAFE_POINT") {
      if (op == "GET_SAFE_POINT") {
        req_store_.fetch_add(1, std::memory_order_relaxed);
      } else {
        req_gw_.fetch_add(1, std::memory_order_relaxed);
      }
    } else {
      req_admin_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  if (op == proto::kOpPing) return "OK PONG";
  if (op == "WHO_LEADER") {
    auto a = leader_address();
    return a.empty() ? "OK LEADER none" : ("OK LEADER " + a);
  }
  try {
    if (op == proto::kOpRaftVote && parts.size() == 6) {
      // RAFT_VOTE shard from term last_idx last_term  (shard ignored, meta=0)
      return handle_raft_vote(parts[2], std::stoull(parts[3]), std::stoull(parts[4]),
                              std::stoull(parts[5]));
    }
    if (op == proto::kOpRaftPreVote && parts.size() == 6) {
      return handle_raft_pre_vote(parts[2], std::stoull(parts[3]), std::stoull(parts[4]),
                                  std::stoull(parts[5]));
    }
    if (op == proto::kOpRaftAe && parts.size() >= 8) {
      const std::string& from = parts[2];
      uint64_t term = std::stoull(parts[3]);
      uint64_t prev = std::stoull(parts[4]);
      uint64_t prev_term = std::stoull(parts[5]);
      uint64_t commit = std::stoull(parts[6]);
      size_t n = static_cast<size_t>(std::stoull(parts[7]));
      std::vector<raft::LogEntry> ents;
      size_t off = 8;
      for (size_t i = 0; i < n; ++i) {
        if (off + 2 >= parts.size()) return "ERR ae_truncated";
        raft::LogEntry e;
        e.term = std::stoull(parts[off]);
        e.index = std::stoull(parts[off + 1]);
        if (!from_hex(parts[off + 2], &e.data)) return "ERR ae_bad_hex";
        ents.push_back(std::move(e));
        off += 3;
      }
      return handle_raft_append(from, term, prev, prev_term, commit, ents);
    }
    // RAFT_SNAP shard from term last_idx last_term hexdata  (shard ignored, meta=0)
    if (op == proto::kOpRaftSnap && parts.size() == 7) {
      std::string data;
      if (!from_hex(parts[6], &data)) return "ERR snap_bad_hex";
      return handle_raft_snapshot(parts[2], std::stoull(parts[3]), std::stoull(parts[4]),
                                  std::stoull(parts[5]), data);
    }
    // RAFT_SNAPC shard from term last_idx last_term offset total done hexchunk
    if (op == proto::kOpRaftSnapc && parts.size() == 10) {
      std::string chunk;
      if (!from_hex(parts[9], &chunk)) return "ERR snap_bad_hex";
      return handle_raft_snap_chunk(parts[2], std::stoull(parts[3]), std::stoull(parts[4]),
                                    std::stoull(parts[5]), std::stoull(parts[6]),
                                    std::stoull(parts[7]), parts[8] != "0", chunk);
    }
    if (op == "REGISTER" && parts.size() == 4) {
      NodeInfo n;
      n.role = (parts[1] == "compute") ? NodeRole::kCompute : NodeRole::kStorage;
      n.node_id = parts[2];
      n.address = parts[3];
      std::string err, redir;
      MetaCommand cmd{MetaOp::kRegister, n, {}, 0, {}};
      // 已注册且地址未变：仅 Leader 本地续租（Follower 必须 redirect，否则 Leader 租约饿死→误 Offline）。
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      const auto existing = state_->get_node(n.node_id);
      const bool known_same =
          existing && existing->address == n.address && existing->role == n.role;
      if (known_same) {
        // Offline 须走 Raft register_node（→ Joining），禁止本地 touch_lease 升成 Active，
        // 否则 JOIN_NODE 会 ERR not_pending，且 Follower 仍见 Offline。
        if (existing->state == NodeState::kOffline) {
          if (!replicate(cmd, &err, &redir)) {
            return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
          }
          return "OK REGISTER " + n.node_id;
        }
        state_->touch_lease(n.node_id);
        return "OK REGISTER " + n.node_id;
      }
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK REGISTER " + n.node_id;
    }
    if (op == "HEARTBEAT" && parts.size() >= 3) {
      std::string nid = parts[1];
      size_t n = static_cast<size_t>(std::stoul(parts[2]));
      if (parts.size() < 3 + n) return "ERR bad_heartbeat";
      std::vector<uint32_t> leaders;
      for (size_t i = 0; i < n; ++i) {
        leaders.push_back(static_cast<uint32_t>(std::stoul(parts[3 + i])));
      }
      std::string err, redir;
      if (!heartbeat(nid, leaders, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK HEARTBEAT";
    }
    if (op == "GET_ROUTE") {
      // A3：Follower 可读
      return format_route(state_->routing_table());
    }
    // WATCH_ROUTE since_ver [timeout_ms] — A2 长轮询；version>since 或超时返回当前表
    if (op == "WATCH_ROUTE" && parts.size() >= 2) {
      const uint64_t since = std::stoull(parts[1]);
      int timeout_ms = 5000;
      if (parts.size() >= 3) {
        timeout_ms = std::stoi(parts[2]);
        if (timeout_ms < 0) timeout_ms = 0;
        if (timeout_ms > 60000) timeout_ms = 60000;
      }
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
      route_watch_waits_.fetch_add(1, std::memory_order_relaxed);
      for (;;) {
        auto rt = state_->routing_table();
        if (rt.version > since || std::chrono::steady_clock::now() >= deadline) {
          return format_route(rt);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    }
    // GET_TS [count] — Percolator TSO；返回 OK TS <start> <count>
    if (op == "GET_TS") {
      uint32_t count = 1;
      if (parts.size() >= 2) count = static_cast<uint32_t>(std::stoul(parts[1]));
      if (count == 0) count = 1;
      if (count > 10000) count = 10000;
      MetaCommand cmd;
      cmd.op = MetaOp::kAllocTs;
      cmd.ts_count = count;
      std::string err, redir;
      // 不跨 Raft 等待持全局锁：并发 GET_TS 可并行 propose，起点按 apply index 回读。
      if (!raft_ || !raft_->is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      uint64_t idx = 0;
      if (!raft_->propose(encode_meta_command(cmd), &idx)) {
        return "ERR propose_failed";
      }
      uint64_t start = 0;
      bool got = false;
      for (int i = 0; i < 1000; ++i) {
        {
          std::lock_guard<std::mutex> lock(apply_mu_);
          auto it = ts_alloc_by_index_.find(idx);
          if (it != ts_alloc_by_index_.end()) {
            start = it->second;
            ts_alloc_by_index_.erase(it);
            got = true;
            break;
          }
        }
        if (i % 10 == 9) raft_->tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      if (!got) return "ERR apply_timeout";
      tso_alloc_total_.fetch_add(count, std::memory_order_relaxed);
      return "OK TS " + std::to_string(start) + " " + std::to_string(count);
    }
    // GET_SAFE_POINT — 读 GC 水位；OK SAFE <gc_safe_ts> <next_ts> <min_start_hint>
    if (op == "GET_SAFE_POINT") {
      return "OK SAFE " + std::to_string(state_->gc_safe_ts()) + " " +
             std::to_string(state_->next_ts()) + " " +
             std::to_string(state_->min_start_ts_hint());
    }
    // ADVANCE_SAFE_POINT [retain] — 按 TSO/min_start 推进水位
    if (op == proto::kOpAdvanceSafePoint) {
      uint32_t retain = 0;
      if (parts.size() >= 2) retain = static_cast<uint32_t>(std::stoul(parts[1]));
      MetaCommand cmd;
      cmd.op = MetaOp::kAdvanceSafePoint;
      cmd.ts_count = retain;
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK SAFE " + std::to_string(state_->gc_safe_ts()) + " " +
             std::to_string(state_->next_ts()) + " " +
             std::to_string(state_->min_start_ts_hint());
    }
    // REPORT_MIN_START_TS <ts> [reporter] — T2：GW/Storage 分源上报
    if (op == "REPORT_MIN_START_TS" && (parts.size() == 2 || parts.size() == 3)) {
      MetaCommand cmd;
      cmd.op = MetaOp::kReportMinStartTs;
      cmd.ts_value = std::stoull(parts[1]);
      if (parts.size() == 3) cmd.node.node_id = parts[2];
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK MIN_START " + std::to_string(state_->min_start_ts_hint());
    }
    // SPLIT_REGION <region_id> <hex_split_key>
    if (op == proto::kOpSplitRegion && parts.size() == 3) {
      std::string split_key;
      if (!from_hex(parts[2], &split_key)) return "ERR bad_hex";
      const uint32_t left_id = static_cast<uint32_t>(std::stoul(parts[1]));
      MetaCommand cmd;
      cmd.op = MetaOp::kSplitRegion;
      cmd.shard_id = left_id;
      cmd.split_key = split_key;
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      auto rt = state_->routing_table();
      uint32_t right = 0;
      bool found = false;
      for (const auto& r : rt.regions) {
        if (r.range.start_key == split_key) {
          right = r.region_id;
          found = true;
          break;
        }
      }
      if (!found) return "ERR split_apply_missing";
      return "OK SPLIT " + std::to_string(left_id) + " " + std::to_string(right);
    }
    // MERGE_REGIONS <left_id> <right_id>
    if (op == proto::kOpMergeRegions && parts.size() == 3) {
      MetaCommand cmd;
      cmd.op = MetaOp::kMergeRegions;
      cmd.shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
      cmd.merge_right_id = static_cast<uint32_t>(std::stoul(parts[2]));
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      if (!state_->find_region(cmd.shard_id)) return "ERR merge_lost_left";
      if (state_->find_region(cmd.merge_right_id)) return "ERR merge_right_still_present";
      return "OK MERGE " + parts[1] + " " + parts[2];
    }
    // LOCATE <hex_key>
    if (op == "LOCATE" && parts.size() == 2) {
      std::string key;
      if (!from_hex(parts[1], &key)) return "ERR bad_hex";
      auto rid = state_->locate(key);
      if (!rid) return "ERR not_found";
      return "OK LOCATE " + std::to_string(*rid);
    }
    // DRAIN_NODE / STORE_DELETE / ACTIVATE_NODE|JOIN_NODE / OFFLINE_NODE id
    // STORE_DELETE → Draining，调度迁光后变 Tombstone
    // JOIN_NODE / ACTIVATE_NODE：Joining（待加入）→ Active，方可调度；须 Store 可达 + 心跳新鲜
    if ((op == proto::kOpDrainNode || op == proto::kOpStoreDelete || op == proto::kOpActivateNode ||
         op == proto::kOpJoinNode || op == proto::kOpOfflineNode) &&
        parts.size() == 2) {
      // 租约/心跳只在 Leader 内存续；Follower 上读 last_heartbeat 会误判 stale。
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      auto cur = state_->get_node(parts[1]);
      if (!cur) return "ERR unknown_node";
      NodeState st = NodeState::kDraining;
      if (op == proto::kOpActivateNode || op == proto::kOpJoinNode) {
        if (cur->state != NodeState::kJoining && cur->state != NodeState::kOffline) {
          return "ERR not_pending";  // 仅待加入 / Offline 可加入
        }
        if (cur->address.empty()) return "ERR no_address";
        // 可用性：gRPC Admin ExecLine PING
        try {
          auto pong = storage_rpc(cur->address, "PING");
          if (!proto::starts_with_ok(pong)) {
            return "ERR unreachable ping=" + pong;
          }
        } catch (const std::exception& e) {
          return std::string("ERR unreachable ") + e.what();
        }
        // 心跳新鲜：超过 store TTL 视为不可用（进程假活 / 未接入 Meta）
        const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        if (cur->last_heartbeat_ms <= 0) {
          return "ERR no_heartbeat";
        }
        const int64_t age = now - cur->last_heartbeat_ms;
        if (age > static_cast<int64_t>(store_ttl_ms_)) {
          return "ERR heartbeat_stale age_ms=" + std::to_string(age) +
                 " ttl_ms=" + std::to_string(store_ttl_ms_);
        }
        st = NodeState::kActive;
      } else if (op == proto::kOpOfflineNode) {
        st = NodeState::kOffline;
      }
      MetaCommand cmd;
      cmd.op = MetaOp::kSetNodeState;
      cmd.node.node_id = parts[1];
      cmd.ts_value = static_cast<uint64_t>(st);
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK " + op + " " + parts[1];
    }
    // REMOVE_TOMBSTONE / KICK_NODE id — 踢出：须先下线（非 Active）且路由无副本
    if ((op == proto::kOpRemoveTombstone || op == proto::kOpKickNode) && parts.size() == 2) {
      auto n = state_->get_node(parts[1]);
      if (!n) return "ERR unknown_node";
      if (n->state == NodeState::kActive) return "ERR still_active";  // 须先下线
      if (state_->region_count_on(parts[1]) > 0) return "ERR still_has_regions";
      MetaCommand cmd;
      cmd.op = MetaOp::kEraseTombstone;
      cmd.node.node_id = parts[1];
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK KICK_NODE " + parts[1];
    }
    // STORE_STATUS id — state / region_count / leader_count
    if (op == "STORE_STATUS" && parts.size() == 2) {
      auto n = state_->get_node(parts[1]);
      if (!n) return "ERR unknown_node";
      return "OK STORE " + parts[1] + " state=" + std::to_string(static_cast<int>(n->state)) +
             " regions=" + std::to_string(state_->region_count_on(parts[1])) +
             " leaders=" + std::to_string(state_->leader_count_on(parts[1]));
    }
    // BALANCE [n] — 显式入队最多 n 个 RF 不变的 balance 迁移（须 Leader：任务只在本进程推进）
    if (op == "BALANCE") {
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      if (!scheduler_) return "ERR no_scheduler";
      size_t max_n = 1;
      if (parts.size() >= 2) max_n = static_cast<size_t>(std::stoul(parts[1]));
      if (max_n == 0) max_n = 1;
      size_t n = scheduler_->enqueue_balance(max_n);
      return "OK BALANCE " + std::to_string(n);
    }
    // LIST_NODES [storage|compute]
    if (op == "LIST_NODES") {
      std::optional<NodeRole> role;
      if (parts.size() >= 2) {
        if (parts[1] == "storage") role = NodeRole::kStorage;
        else if (parts[1] == "compute") role = NodeRole::kCompute;
        else return "ERR bad_role";
      }
      auto nodes = state_->list_nodes(role);
      std::ostringstream oss;
      oss << "OK NODES " << nodes.size();
      for (const auto& n : nodes) {
        oss << ' ' << n.node_id << '@' << n.address << ':'
            << (n.role == NodeRole::kStorage ? "S" : "C") << ':'
            << static_cast<int>(n.state);
      }
      return oss.str();
    }
    // SUGGEST_PLACE [avoid=id,id,...]
    if (op == "SUGGEST_PLACE") {
      std::vector<std::string> avoid;
      if (parts.size() >= 2 && parts[1].rfind("avoid=", 0) == 0) {
        std::stringstream ss(parts[1].substr(6));
        std::string item;
        while (std::getline(ss, item, ',')) {
          if (!item.empty()) avoid.push_back(item);
        }
      }
      auto ids = state_->suggest_placement(avoid);
      std::ostringstream oss;
      oss << "OK SUGGEST " << ids.size();
      for (const auto& id : ids) oss << ' ' << id;
      return oss.str();
    }
    // FIND_MERGE_RIGHT <left_region_id>
    if (op == "FIND_MERGE_RIGHT" && parts.size() == 2) {
      auto right = state_->adjacent_right(static_cast<uint32_t>(std::stoul(parts[1])));
      if (!right) return "ERR no_adjacent";
      return "OK MERGE_RIGHT " + parts[1] + " " + std::to_string(*right);
    }
    // SCALE_OUT / GET_SCALE / LIST_SCALE：任务仅内存；必须在 Leader 上创建与推进。
    auto scale_need_leader = [this]() -> std::string {
      if (is_leader()) return {};
      auto a = leader_address();
      return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
    };
    // SCALE_OUT node_id [rid,rid,...] — S1 在线扩容
    if (op == proto::kOpScaleOut && parts.size() >= 2) {
      if (!scaler_) return "ERR no_scaler";
      if (auto e = scale_need_leader(); !e.empty()) return e;
      std::vector<uint32_t> shards;
      if (parts.size() >= 3) {
        std::stringstream ss(parts[2]);
        std::string item;
        while (std::getline(ss, item, ',')) {
          if (!item.empty()) shards.push_back(static_cast<uint32_t>(std::stoul(item)));
        }
      }
      auto t = scaler_->start_scale_out(parts[1], std::move(shards));
      return "OK SCALE " + t.task_id + " " + std::to_string(static_cast<int>(t.state)) +
             (t.error.empty() ? "" : (" " + t.error));
    }
    // CANCEL_SCALE <node_id> [region_id] — 取消该节点（可选指定 Region）上未完成的 SCALE_OUT
    if (op == "CANCEL_SCALE" && parts.size() >= 2) {
      if (!scaler_) return "ERR no_scaler";
      if (auto e = scale_need_leader(); !e.empty()) return e;
      std::optional<uint32_t> rid;
      if (parts.size() >= 3) rid = static_cast<uint32_t>(std::stoul(parts[2]));
      int n = scaler_->cancel_inflight(parts[1], rid);
      return "OK CANCEL_SCALE " + std::to_string(n);
    }
    if (op == "GET_SCALE" && parts.size() == 2) {
      if (!scaler_) return "ERR no_scaler";
      if (auto e = scale_need_leader(); !e.empty()) return e;
      auto t = scaler_->get_task(parts[1]);
      if (!t) return "ERR no_task";
      // OK SCALE id state progress node=… shards=… [match= commit= caught=] [error=…]
      std::ostringstream oss;
      oss << "OK SCALE " << t->task_id << ' ' << static_cast<int>(t->state) << ' ' << t->progress
          << " node=" << t->new_node_id << " shards=";
      for (size_t i = 0; i < t->shard_ids.size(); ++i) {
        if (i) oss << ',';
        oss << t->shard_ids[i];
      }
      if (t->state == ScaleTaskState::kCatchingUp && !t->shard_ids.empty()) {
        const uint32_t sid = t->shard_ids.front();
        auto addr = region_leader_addr(sid);
        if (addr) {
          try {
            auto resp = storage_rpc(
                *addr, std::string(proto::kOpAdminPeerProgress) + " " + std::to_string(sid) + " " + t->new_node_id);
            std::istringstream iss(resp);
            std::string ok, tag;
            uint64_t match = 0, commit = 0, last = 0;
            int caught = 0;
            if ((iss >> ok >> tag >> match >> commit >> last >> caught) && ok == "OK") {
              oss << " match=" << match << " commit=" << commit << " caught=" << caught;
            }
          } catch (...) {
          }
        }
      }
      if (!t->error.empty()) oss << " error=" << t->error;
      return oss.str();
    }
    if (op == "LIST_SCALE") {
      if (!scaler_) return "ERR no_scaler";
      if (auto e = scale_need_leader(); !e.empty()) return e;
      auto tasks = scaler_->list_tasks();
      std::ostringstream oss;
      oss << "OK SCALE_LIST " << tasks.size();
      for (const auto& t : tasks) {
        oss << ' ' << t.task_id << ':' << t.new_node_id << ':' << static_cast<int>(t.state) << ':'
            << t.progress;
        if (!t.shard_ids.empty()) {
          oss << ':';
          for (size_t i = 0; i < t.shard_ids.size(); ++i) {
            if (i) oss << ',';
            oss << t.shard_ids[i];
          }
        }
      }
      return oss.str();
    }
    // SCHEDULE_MIGRATE / LIST_MIGRATE / CANCEL_MIGRATE：调度状态机仅 Leader 有意义
    if (op == proto::kOpScheduleMigrate && parts.size() >= 2) {
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      if (!scheduler_) return "ERR no_scheduler";
      std::vector<std::string> desired;
      if (parts.size() >= 3) {
        std::stringstream ss(parts[2]);
        std::string item;
        while (std::getline(ss, item, ',')) {
          if (!item.empty()) desired.push_back(item);
        }
      }
      auto t = scheduler_->submit(static_cast<uint32_t>(std::stoul(parts[1])), std::move(desired));
      return "OK MIGRATE " + t.task_id + " " + std::to_string(static_cast<int>(t.state));
    }
    if (op == "LIST_MIGRATE") {
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      if (!scheduler_) return "ERR no_scheduler";
      auto tasks = scheduler_->list_tasks();
      std::ostringstream oss;
      oss << "OK MIGRATE_LIST " << tasks.size();
      for (const auto& t : tasks) {
        oss << ' ' << t.task_id << ':' << t.region_id << ':' << static_cast<int>(t.state);
      }
      return oss.str();
    }
    if (op == proto::kOpCancelMigrate && parts.size() == 2) {
      if (!is_leader()) {
        auto a = leader_address();
        return std::string("ERR not_leader") + (a.empty() ? "" : (" redirect=" + a));
      }
      if (!scheduler_) return "ERR no_scheduler";
      return scheduler_->cancel(parts[1]) ? "OK CANCELLED" : "ERR cancel";
    }
    // PLACE_REGION <rid> <node_id>[,node_id...]
    // 仅更新 Meta 路由；数据面 ConfChange 由 skv place 编排后再调本命令。
    if (op == proto::kOpPlaceRegion && parts.size() == 3) {
      const uint32_t rid = static_cast<uint32_t>(std::stoul(parts[1]));
      MetaCommand cmd;
      cmd.op = MetaOp::kPlaceRegion;
      cmd.shard_id = rid;
      std::stringstream ss(parts[2]);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        auto node = state_->get_node(item);
        if (!node) return "ERR unknown_node " + item;
        Replica r;
        r.node_id = node->node_id;
        r.address = node->address;
        cmd.place_replicas.push_back(std::move(r));
      }
      if (cmd.place_replicas.empty()) return "ERR empty_peers";
      std::string err, redir;
      if (!replicate(cmd, &err, &redir)) {
        return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
      }
      return "OK PLACE " + parts[1] + " " + std::to_string(cmd.place_replicas.size());
    }
  } catch (const std::exception& e) {
    return std::string("ERR ") + e.what();
  }
  return "ERR bad_request";
}

// 创建 Raft/transport、绑定 Scaler/Scheduler 钩子、restore 迁移、起线程。
void MetaNode::start() {
  if (running_.exchange(true)) return;

  if (!bootstrap_.meta_peers.empty()) {
    for (const auto& p : bootstrap_.meta_peers) {
      peers_.push_back({p.node_id, p.address, false});
      transport_.set_peer(p.node_id, p.address);
    }
  } else {
    peers_.push_back({self_id_, listen_addr_, false});
    // 出站地址：若 listen 为 0.0.0.0 则用 127.0.0.1
    std::string host;
    uint16_t port = 8500;
    net::parse_host_port(listen_addr_, &host, &port);
    if (host.empty()) host = "0.0.0.0";
    std::string adv = (host == "0.0.0.0" ? "127.0.0.1" : host) + ":" + std::to_string(port);
    peers_.back().address = adv;
    transport_.set_peer(self_id_, adv);
  }

  transport_.start();
  raft::RaftNode::Options opts;
  opts.shard_id = kMetaShard;
  opts.self_id = self_id_;
  opts.peers = peers_;
  opts.election_timeout_ms = bootstrap_.raft_election_timeout_ms;
  opts.heartbeat_ms = bootstrap_.raft_heartbeat_ms;
  opts.compact_threshold = bootstrap_.raft_compact_threshold;
  opts.snapshot_min_interval_ms = 1000;
  // Meta 与 Storage 共用端口做业务+Raft；注册洪峰时 AE 排队会误触发 check_quorum 让位风暴。
  // Storage Multi-Raft 仍默认开启；Meta 单组管控面关闭该检查更稳。
  opts.check_quorum = false;
  // propose 仍立即 AE（低延迟 TSO）；与 maybe_broadcast 修复配合，不再靠关 coalesce 硬扛。
  opts.coalesce_replication = false;
  // macOS F_FULLFSYNC 每次落盘可达数百 ms 且持有 Raft mu_，会堵死 AE/TSO。
  // Meta 管控面优先可用；耐久由定期 snapshot/生产改 kData。
  opts.persist_sync = raft::SyncMode::kNone;
  opts.persist_batch_n = 32;
  {
    std::string data_root = bootstrap_.data_dir;
    if (data_root.empty()) {
      data_root = "./data/" + self_id_;
    }
    auto raft_dir = std::filesystem::path(data_root) / "raft";
    std::filesystem::create_directories(raft_dir);
    opts.persist_dir = raft_dir.string();
    SKV_INFO << "meta raft persist_dir=" << opts.persist_dir
             << " compact_threshold=" << opts.compact_threshold;
  }
  auto snap_provider = [this](uint64_t* /*last_idx*/, uint64_t* /*last_term*/) {
    return state_->serialize();
  };
  auto snap_applier = [this](uint64_t last_idx, uint64_t /*last_term*/, std::string_view data) {
    if (!state_->load_snapshot(data)) {
      SKV_WARN << "meta snap_applier failed last_idx=" << last_idx;
      return false;
    }
    {
      std::lock_guard<std::mutex> lock(apply_mu_);
      last_applied_wait_ = last_idx;
    }
    return true;
  };
  raft_ = std::make_unique<raft::RaftNode>(
      std::move(opts),
      [this](uint64_t index, const std::string& data) { on_apply(index, data); },
      transport_.bind(kMetaShard), std::move(snap_provider), std::move(snap_applier));
  // Meta 单组不 DESTROY：用 aliasing shared_ptr，生命周期仍由 unique_ptr 管。
  transport_.register_node(kMetaShard, self_id_,
                           std::shared_ptr<raft::RaftNode>(raft_.get(), [](raft::RaftNode*) {}));
  raft_->recover_apply();

  // S1：Scaler — Meta 路由 + Storage ConfChange + catch-up 门闩
  auto add_learner = [this](uint32_t shard_id, const Replica& learner) {
    if (!is_leader()) return false;
    SKV_INFO << "scale add_learner region=" << shard_id << " peer=" << learner.node_id;
    MetaCommand cmd;
    cmd.op = MetaOp::kSetLearner;
    cmd.shard_id = shard_id;
    cmd.learner = learner;
    std::string err, redir;
    if (!replicate(cmd, &err, &redir)) return false;
    auto addr = region_leader_addr(shard_id);
    if (!addr) return true;  // 尚无 Storage 可达：仅 Meta 表
    // 必须先在新节点 ENSURE（learner），否则 RAFT 消息会 get_or_create 成单副本 Leader（voters=1）。
    {
      std::ostringstream peers_csv;
      bool first = true;
      auto rt = state_->routing_table();
      for (const auto& region : rt.regions) {
        if (region.region_id != shard_id) continue;
        for (const auto& r : region.replicas) {
          if (r.node_id == learner.node_id) continue;
          if (!first) peers_csv << ',';
          first = false;
          peers_csv << r.node_id << '@' << r.address;
          if (r.is_learner) peers_csv << "*N";
        }
        break;
      }
      if (!first) peers_csv << ',';
      peers_csv << learner.node_id << '@' << learner.address << "*N";
      try {
        auto er = storage_rpc(learner.address,
                              std::string(proto::kOpAdminEnsureRegion) + " " + std::to_string(shard_id) + " " +
                                  peers_csv.str());
        if (!proto::starts_with_ok(er)) {
          SKV_WARN << "ADMIN_ENSURE_REGION (scale): " << er;
          return false;
        }
        SKV_INFO << "scale ensure region=" << shard_id << " peer=" << learner.node_id
                 << " -> " << er;
      } catch (const std::exception& e) {
        SKV_WARN << "ADMIN_ENSURE_REGION (scale): " << e.what();
        return false;
      }
    }
    // ConfChange 可能遇 not_leader / busy / 瞬时超时；与 PROMOTE 一样短重试。
    for (int attempt = 0; attempt < 16; ++attempt) {
      auto cur = region_leader_addr(shard_id);
      if (!cur) cur = addr;
      try {
        auto resp = storage_rpc(
            *cur, std::string(proto::kOpAdminAddLearner) + " " + std::to_string(shard_id) + " " + learner.node_id + "@" +
                       learner.address);
        if (proto::starts_with_ok(resp)) return true;
        SKV_WARN << "ADMIN_ADD_LEARNER: " << resp;
        if (resp.find("busy") == std::string::npos && resp.find("joint") == std::string::npos &&
            resp.find("not_leader") == std::string::npos &&
            resp.find("already") == std::string::npos) {
          return false;
        }
      } catch (const std::exception& e) {
        SKV_WARN << "ADMIN_ADD_LEARNER: " << e.what();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200 + attempt * 50));
    }
    return false;
  };
  auto catch_up = [this](uint32_t shard_id, const std::string& node_id) {
    auto addr = region_leader_addr(shard_id);
    if (!addr) return true;
    try {
      auto resp =
          storage_rpc(*addr, std::string(proto::kOpAdminPeerProgress) + " " + std::to_string(shard_id) + " " + node_id);
      std::istringstream iss(resp);
      std::string ok, tag;
      uint64_t match = 0, commit = 0, last = 0;
      int caught = 0;
      if (!(iss >> ok >> tag >> match >> commit >> last >> caught) || ok != proto::kOk) return false;
      return caught == 1;
    } catch (...) {
      return false;
    }
  };
  auto promote = [this, &add_learner](uint32_t shard_id, const std::string& node_id) {
    if (!is_leader()) return false;
    SKV_INFO << "scale promote region=" << shard_id << " peer=" << node_id;
    auto addr = region_leader_addr(shard_id);
    if (addr) {
      bool ok = false;
      bool readded = false;
      for (int attempt = 0; attempt < 16; ++attempt) {
        try {
          auto resp =
              storage_rpc(*addr, std::string(proto::kOpAdminPromote) + " " + std::to_string(shard_id) + " " + node_id);
          if (proto::starts_with_ok(resp)) {
            ok = true;
            break;
          }
          SKV_WARN << "ADMIN_PROMOTE: " << resp;
          // 快照追平后新 Leader 丢了 Learner：补一次 ADD_LEARNER 再 Promote
          if (!readded && resp.find("unknown_peer") != std::string::npos) {
            readded = true;
            auto node = state_->get_node(node_id);
            if (node) {
              Replica learner;
              learner.node_id = node_id;
              learner.address = node->address;
              learner.is_learner = true;
              if (add_learner(shard_id, learner)) {
                SKV_INFO << "scale re-add learner after unknown_peer region=" << shard_id
                         << " peer=" << node_id;
              }
            }
            if (auto fresh = region_leader_addr(shard_id)) addr = fresh;
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            continue;
          }
          if (resp.find("busy") == std::string::npos && resp.find("joint") == std::string::npos &&
              resp.find("not_leader") == std::string::npos) {
            return false;
          }
        } catch (const std::exception& e) {
          SKV_WARN << "ADMIN_PROMOTE: " << e.what();
          const std::string msg = e.what();
          if (msg.find("busy") == std::string::npos && msg.find("joint") == std::string::npos &&
              msg.find("not_leader") == std::string::npos) {
            return false;
          }
        }
        // Leader 可能刚变；刷新地址再试
        if (auto fresh = region_leader_addr(shard_id)) addr = fresh;
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
      }
      if (!ok) return false;
    }
    MetaCommand cmd;
    cmd.op = MetaOp::kPromote;
    cmd.shard_id = shard_id;
    cmd.learner.node_id = node_id;
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  };
  auto bump = [this]() {
    MetaCommand cmd;
    cmd.op = MetaOp::kBumpRoute;
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  };
  scaler_ = std::make_unique<Scaler>(*state_, std::move(add_learner), std::move(promote),
                                     std::move(bump), std::move(catch_up),
                                     [this] { return is_leader(); });

  auto place_fn = [this](uint32_t region_id, const std::vector<std::string>& desired) {
    // 通知 Storage 执行 ADMIN_PLACE_REGION（完整 ConfChange，可能数十秒）
    // 优先打 Region Leader / 当前成员，避免对全量 Store 串行 180s 超时。
    if (!is_leader()) return false;
    if (desired.empty()) return false;
    std::ostringstream csv;
    for (size_t i = 0; i < desired.size(); ++i) {
      if (i) csv << ',';
      csv << desired[i];
    }
    const std::string req =
        std::string(proto::kOpAdminPlaceRegion) + " " + std::to_string(region_id) + " " + csv.str();
    std::vector<std::string> seeds;
    if (auto la = region_leader_addr(region_id)) seeds.push_back(*la);
    auto rt = state_->routing_table();
    for (const auto& reg : rt.regions) {
      if (reg.region_id != region_id) continue;
      for (const auto& r : reg.replicas) {
        if (r.address.empty()) continue;
        bool dup = false;
        for (const auto& s : seeds)
          if (s == r.address) {
            dup = true;
            break;
          }
        if (!dup) seeds.push_back(r.address);
      }
      break;
    }
    for (const auto& id : desired) {
      auto n = state_->get_node(id);
      if (!n || n->address.empty()) continue;
      if (n->state == NodeState::kTombstone || n->state == NodeState::kOffline) continue;
      bool dup = false;
      for (const auto& s : seeds)
        if (s == n->address) {
          dup = true;
          break;
        }
      if (!dup) seeds.push_back(n->address);
    }
    for (const auto& addr : seeds) {
      try {
        auto resp = storage_rpc(addr, req);
        if (proto::starts_with_ok(resp)) return true;
      } catch (...) {
      }
    }
    return false;
  };
  auto commit_place = [this](uint32_t region_id, const std::vector<std::string>& desired) {
    if (!is_leader()) return false;
    MetaCommand cmd;
    cmd.op = MetaOp::kPlaceRegion;
    cmd.shard_id = region_id;
    for (const auto& id : desired) {
      auto node = state_->get_node(id);
      if (!node) return false;
      Replica r;
      r.node_id = node->node_id;
      r.address = node->address;
      cmd.place_replicas.push_back(std::move(r));
    }
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  };
  auto persist_mig = [this](const PersistMigrate& pm) {
    MetaCommand cmd;
    cmd.op = MetaOp::kUpsertMigrate;
    cmd.migrate = pm;
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  };
  auto erase_mig = [this](const std::string& tid) {
    MetaCommand cmd;
    cmd.op = MetaOp::kEraseMigrate;
    cmd.migrate.task_id = tid;
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  };
  scheduler_ = std::make_unique<RegionScheduler>(
      *state_, std::move(place_fn), std::move(commit_place), [this] { return is_leader(); },
      /*max_inflight=*/2, std::move(persist_mig), std::move(erase_mig));
  scheduler_->set_tombstone_hook([this](const std::string& id) {
    MetaCommand cmd;
    cmd.op = MetaOp::kSetNodeState;
    cmd.node.node_id = id;
    cmd.ts_value = static_cast<uint64_t>(NodeState::kTombstone);
    std::string err, redir;
    return replicate(cmd, &err, &redir);
  });
  // S5 PD：SKV_PD_AUTO_BALANCE=1 开启主动 balance；默认关（避免 Join 与 SCALE_OUT 抢 Region）。
  {
    bool auto_bal = false;
    if (const char* e = std::getenv(proto::env::kPdAutoBalance)) {
      auto_bal = (e[0] == '1' || e[0] == 't' || e[0] == 'T' || e[0] == 'y' || e[0] == 'Y');
    }
    scheduler_->set_auto_balance(auto_bal);
  }
  scheduler_->set_pause_balance_hook([this] {
    return scaler_ && scaler_->has_inflight();
  });
  scheduler_->restore_persisted(state_->list_migrates());

  grpc_server_ = std::make_unique<MetaGrpcServer>(*this, listen_addr_);
  grpc_server_->start();

  tick_thread_ = std::thread([this] { tick_loop(); });
  SKV_INFO << "meta server started id=" << self_id_ << " listen=" << listen_addr_
           << " grpc=1 raft_peers=" << peers_.size()
           << " route_ver=" << state_->routing_table().version;
}

// 供 storage_rpc：优先 is_leader → 非 learner → 首个副本地址。
std::optional<std::string> MetaNode::region_leader_addr(uint32_t region_id) const {
  auto rt = state_->routing_table();
  for (const auto& region : rt.regions) {
    if (region.region_id != region_id) continue;
    for (const auto& r : region.replicas) {
      if (r.is_leader && !r.address.empty()) return r.address;
    }
    for (const auto& r : region.replicas) {
      if (!r.is_learner && !r.address.empty()) return r.address;
    }
    if (!region.replicas.empty() && !region.replicas[0].address.empty()) {
      return region.replicas[0].address;
    }
  }
  return std::nullopt;
}

// gRPC AdminService.ExecLine。
// ConfChange / 快照相关 ADMIN 可能 >3s，超时会导致管控台「增加副本」在 add_learner 阶段失败。
std::string MetaNode::storage_rpc(std::string addr, const std::string& req) const {
  int timeout_ms = proto::timeout_ms::kStorageRpcDefault;
  if (req.rfind(proto::kOpAdminAddLearner, 0) == 0 || req.rfind(proto::kOpAdminPromote, 0) == 0 ||
      req.rfind(proto::kOpAdminRemovePeer, 0) == 0 ||
      req.rfind(proto::kAdminEnsurePrefix, 0) == 0 ||
      req.rfind(proto::kAdminTransferPrefix, 0) == 0 ||
      req.rfind(proto::kOpAdminPeerProgress, 0) == 0) {
    timeout_ms = proto::timeout_ms::kStorageRpcAdmin;
  }
  for (int hop = 0; hop < 6; ++hop) {
    if (addr.rfind("0.0.0.0", 0) == 0) addr.replace(0, 7, "127.0.0.1");
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = ::storage::v1::AdminService::NewStub(ch);
    ::storage::v1::ExecLineRequest er;
    er.set_line(req);
    ::storage::v1::ExecLineResponse eres;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
    grpc_attach_admin_token(&ctx, admin_token_);
    auto st = stub->ExecLine(&ctx, er, &eres);
    if (!st.ok()) throw std::runtime_error("storage_rpc grpc: " + st.error_message());
    std::string buf = eres.body();
    if (buf.find("not_leader") != std::string::npos) {
      auto p = buf.find("redirect=");
      if (p == std::string::npos) throw std::runtime_error(buf);
      addr = buf.substr(p + 9);
      auto sp = addr.find_first_of(" \t\r\n");
      if (sp != std::string::npos) addr = addr.substr(0, sp);
      continue;
    }
    return buf;
  }
  throw std::runtime_error("storage_rpc redirect loop");
}

void MetaNode::stop() {
  if (!running_.exchange(false)) return;
  if (grpc_server_) {
    grpc_server_->stop();
    grpc_server_.reset();
  }
  if (tick_thread_.joinable()) tick_thread_.join();
  transport_.stop();
  scheduler_.reset();
  scaler_.reset();
}

// ~50ms 一轮：raft tick；~5s GC；~2s sweep（仅 Leader propose，不阻塞 tick）。
void MetaNode::tick_loop() {
  while (running_) {
    if (raft_) raft_->tick();
    // ~5s：Leader 推进 GC safe point（50ms * 100）
    if (is_leader() && ++gc_advance_ticks_ >= 100) {
      gc_advance_ticks_ = 0;
      MetaCommand cmd;
      cmd.op = MetaOp::kAdvanceSafePoint;
      cmd.ts_count = 0;  // 用 ClusterState 默认 retain
      std::string err, redir;
      // 异步：避免 tick 线程卡在 apply 等待导致 AE/心跳停摆。
      if (!replicate_async(cmd, &err, &redir)) {
        SKV_DEBUG << "advance_safe_point skipped: " << err;
      }
    }
    // H2：~2s 在 Leader 上按本地租约判失联，再 Raft 发布 Offline（Followers 无租约副本）。
    if (is_leader() && ++stale_sweep_ticks_ >= 40) {
      stale_sweep_ticks_ = 0;
      for (const auto& id : state_->list_stale_storage(store_ttl_ms_)) {
        MetaCommand cmd;
        cmd.op = MetaOp::kSetNodeState;
        cmd.node.node_id = id;
        cmd.ts_value = static_cast<uint64_t>(NodeState::kOffline);
        std::string err, redir;
        if (!replicate_async(cmd, &err, &redir)) {
          SKV_DEBUG << "offline stale skipped id=" << id << " err=" << err;
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

}  // namespace opskv::meta
