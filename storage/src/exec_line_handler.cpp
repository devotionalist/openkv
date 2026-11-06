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

// @file exec_line_handler.cpp
// 行协议命令解析（供 gRPC AdminService.ExecLine）。

#include "opskv/storage/exec_line_handler.hpp"

#include "opskv/admin_acl.hpp"
#include "opskv/protocol.hpp"
#include "opskv/codec.hpp"
#include "opskv/raft/types.hpp"
#include "opskv/storage/storage_node.hpp"
#include "opskv/storage/watch_log.hpp"

#include <cctype>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace opskv::storage {
namespace {

using opskv::from_hex;
using opskv::to_hex;

std::vector<std::string> split_ws(const std::string& line) {
  std::vector<std::string> parts;
  std::istringstream iss(line);
  std::string p;
  while (iss >> p) {
    parts.push_back(p);
  }
  return parts;
}

}  // namespace

namespace proto = opskv::proto;

ExecLineHandler::ExecLineHandler(StoreFacade& facade)
    : facade_(facade), admin_token_(admin_token_from_env()) {}

std::string ExecLineHandler::process_line(const std::string& line, AdminSession* sess) {
  return handle_line(line, sess);
}

std::string ExecLineHandler::handle_line(const std::string& line, AdminSession* sess) {
  AdminSession local;
  AdminSession& session = sess ? *sess : local;
  auto parts = split_ws(line);
  if (parts.empty()) {
    return "ERR empty";
  }
  const auto& op = parts[0];
  if (op == proto::kOpAuth) {
    if (parts.size() < 2) return "ERR usage: AUTH <token>";
    if (!admin_try_auth(&session, admin_token_, parts[1])) return "ERR unauthorized";
    return "OK AUTH";
  }
  if (storage_needs_admin(op) && !admin_allowed(session, admin_token_)) {
    return "ERR unauthorized";
  }
  const ReqClass cls = classify_op(op);
  InflightGuard guard(gate_, cls);
  if (!guard) {
    return std::string("ERR busy class=") + req_class_name(cls);
  }
  if (op == proto::kOpStats) {
    auto& tr = facade_.node().transport();
    const auto pr = peer_raft_.encode();
    const auto ps = peer_sync_.encode();
    const auto pm = facade_.node().meta_out_peers_encode();
    const auto pro = tr.encode_out_raft();
    const auto pso = tr.encode_out_sync();
    const uint64_t raft_all = req_raft_.load() + tr.out_raft_total();
    const uint64_t sync_all = req_sync_.load() + tr.out_sync_total();
    return "OK STATS requests=" + std::to_string(req_total_.load() + tr.out_raft_total() + tr.out_sync_total()) +
           " gw=" + std::to_string(req_gw_.load()) +
           " raft=" + std::to_string(raft_all) +
           " sync=" + std::to_string(sync_all) +
           " admin=" + std::to_string(req_admin_.load()) +
           " meta_out=" + std::to_string(facade_.node().meta_out_count()) +
           " client=" + std::to_string(req_gw_.load()) +
           (pr.empty() ? "" : (" peer_raft=" + pr)) +
           (ps.empty() ? "" : (" peer_sync=" + ps)) +
           (pro.empty() ? "" : (" peer_raft_out=" + pro)) +
           (pso.empty() ? "" : (" peer_sync_out=" + pso)) +
           (pm.empty() ? "" : (" meta_out_peers=" + pm)) +
           " qdepth=" + std::to_string(tr.queue_depth()) +
           " qdrop=" + std::to_string(tr.dropped_jobs()) + " " + gate_.encode_stats();
  }
  // O2：Prometheus 文本（多行；末行 END）
  if (op == proto::kOpMetrics) {
    return facade_.metrics().prometheus_text() + "END\n";
  }
  if (op != proto::kOpPing) {
    req_total_.fetch_add(1, std::memory_order_relaxed);
    if (op == proto::kOpRaftVote || op == proto::kOpRaftPreVote) {
      req_raft_.fetch_add(1, std::memory_order_relaxed);
      if (parts.size() >= 3) peer_raft_.add(parts[2]);
    } else if (op == proto::kOpRaftAe) {
      // parts[7]=n：有日志条目算数据同步，否则算 Raft 心跳
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
    } else if (op.rfind(proto::kAdminPrefix, 0) == 0) {
      req_admin_.fetch_add(1, std::memory_order_relaxed);
    } else {
      req_gw_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  if (op == proto::kOpPing) {
    return "OK PONG";
  }
  try {
    // Raft 内部：RAFT_VOTE / RAFT_PREVOTE shard from term last_idx last_term
    if (op == proto::kOpRaftVote && parts.size() == 6) {
      return facade_.node().handle_raft_vote(
          static_cast<uint32_t>(std::stoul(parts[1])), parts[2], std::stoull(parts[3]),
          std::stoull(parts[4]), std::stoull(parts[5]));
    }
    if (op == proto::kOpRaftPreVote && parts.size() == 6) {
      return facade_.node().handle_raft_pre_vote(
          static_cast<uint32_t>(std::stoul(parts[1])), parts[2], std::stoull(parts[3]),
          std::stoull(parts[4]), std::stoull(parts[5]));
    }
    // RAFT_AE shard from term prev prev_term commit n [term idx hex]*
    if (op == proto::kOpRaftAe && parts.size() >= 8) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      const std::string& from = parts[2];
      uint64_t term = std::stoull(parts[3]);
      uint64_t prev = std::stoull(parts[4]);
      uint64_t prev_term = std::stoull(parts[5]);
      uint64_t commit = std::stoull(parts[6]);
      size_t n = static_cast<size_t>(std::stoull(parts[7]));
      std::vector<raft::LogEntry> ents;
      size_t off = 8;
      for (size_t i = 0; i < n; ++i) {
        if (off + 2 >= parts.size()) {
          return "ERR ae_truncated";
        }
        raft::LogEntry e;
        e.term = std::stoull(parts[off]);
        e.index = std::stoull(parts[off + 1]);
        if (!from_hex(parts[off + 2], &e.data)) {
          return "ERR ae_bad_hex";
        }
        ents.push_back(std::move(e));
        off += 3;
      }
      return facade_.node().handle_raft_append(shard, from, term, prev, prev_term, commit, ents);
    }
    // ADMIN_PREPARE_SPLIT left_region hex_split_key — 打开写暂停窗口
    if (op == proto::kOpAdminPrepareSplit && parts.size() == 3) {
      std::string split_key;
      if (!from_hex(parts[2], &split_key)) return "ERR bad_hex";
      return facade_.node().admin_prepare_split(static_cast<uint32_t>(std::stoul(parts[1])),
                                                split_key);
    }
    // ADMIN_SPLIT left_region right_region hex_split_key
    if (op == proto::kOpAdminSplit && parts.size() == 4) {
      std::string split_key;
      if (!from_hex(parts[3], &split_key)) return "ERR bad_hex";
      return facade_.node().admin_split(static_cast<uint32_t>(std::stoul(parts[1])),
                                        static_cast<uint32_t>(std::stoul(parts[2])), split_key);
    }
    // ADMIN_MERGE left_region right_region
    if (op == proto::kOpAdminMerge && parts.size() == 3) {
      return facade_.node().admin_merge(static_cast<uint32_t>(std::stoul(parts[1])),
                                        static_cast<uint32_t>(std::stoul(parts[2])));
    }
    // ADMIN_ENSURE_REGION rid [id@addr[*N],...]
    if (op == proto::kOpAdminEnsureRegion && parts.size() >= 2) {
      uint32_t rid = static_cast<uint32_t>(std::stoul(parts[1]));
      std::vector<raft::Peer> peers;
      if (parts.size() >= 3) {
        std::stringstream ss(parts[2]);
        std::string item;
        while (std::getline(ss, item, ',')) {
          if (item.empty()) continue;
          raft::Peer p;
          bool learner = false;
          if (!item.empty() && item.back() == 'N' && item.size() >= 2 &&
              item[item.size() - 2] == '*') {
            learner = true;
            item = item.substr(0, item.size() - 2);
          }
          auto at = item.find('@');
          if (at == std::string::npos) continue;
          p.id = item.substr(0, at);
          p.address = item.substr(at + 1);
          p.learner = learner;
          peers.push_back(std::move(p));
        }
      }
      return facade_.node().admin_ensure_region(rid, peers);
    }
    // ADMIN_ADD_LEARNER rid id@addr
    if (op == proto::kOpAdminAddLearner && parts.size() == 3) {
      uint32_t rid = static_cast<uint32_t>(std::stoul(parts[1]));
      auto at = parts[2].find('@');
      if (at == std::string::npos) return "ERR bad_peer";
      raft::Peer p;
      p.id = parts[2].substr(0, at);
      p.address = parts[2].substr(at + 1);
      p.learner = true;
      return facade_.node().admin_add_learner(rid, p);
    }
    // ADMIN_PROMOTE rid peer_id
    if (op == proto::kOpAdminPromote && parts.size() == 3) {
      return facade_.node().admin_promote(static_cast<uint32_t>(std::stoul(parts[1])), parts[2]);
    }
    // ADMIN_REMOVE_PEER rid peer_id
    if (op == proto::kOpAdminRemovePeer && parts.size() == 3) {
      return facade_.node().admin_remove_peer(static_cast<uint32_t>(std::stoul(parts[1])),
                                              parts[2]);
    }
    // ADMIN_DESTROY_REGION rid
    if (op == proto::kOpAdminDestroyRegion && parts.size() == 2) {
      return facade_.node().admin_destroy_region(static_cast<uint32_t>(std::stoul(parts[1])));
    }
    // ADMIN_PEER_PROGRESS rid peer_id
    if (op == proto::kOpAdminPeerProgress && parts.size() == 3) {
      return facade_.node().admin_peer_progress(static_cast<uint32_t>(std::stoul(parts[1])),
                                                parts[2]);
    }
    // ADMIN_TRANSFER_LEADER rid target_id
    if (op == proto::kOpAdminTransferLeader && parts.size() == 3) {
      return facade_.node().admin_transfer_leader(static_cast<uint32_t>(std::stoul(parts[1])),
                                                  parts[2]);
    }
    // ADMIN_CAMPAIGN rid
    if (op == proto::kOpAdminCampaign && parts.size() == 2) {
      return facade_.node().admin_campaign(static_cast<uint32_t>(std::stoul(parts[1])));
    }
    // ADMIN_PLACE_REGION rid id1,id2,...
    if (op == proto::kOpAdminPlaceRegion && parts.size() == 3) {
      std::vector<std::string> desired;
      std::stringstream ss(parts[2]);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) desired.push_back(item);
      }
      return facade_.node().admin_place_region(static_cast<uint32_t>(std::stoul(parts[1])),
                                               desired);
    }
    // ADMIN_SET_EPOCH rid epoch
    if (op == proto::kOpAdminSetEpoch && parts.size() == 3) {
      return facade_.node().admin_set_epoch(static_cast<uint32_t>(std::stoul(parts[1])),
                                            std::stoull(parts[2]));
    }
    // ADMIN_CHECKPOINT rid [path] — E3 RocksDB Checkpoint
    if (op == proto::kOpAdminCheckpoint && (parts.size() == 2 || parts.size() == 3)) {
      return facade_.node().admin_checkpoint(static_cast<uint32_t>(std::stoul(parts[1])),
                                             parts.size() == 3 ? parts[2] : "");
    }
    // ADMIN_COP rid start_hex|- end_hex|- limit [kp=hex] [vp=hex] [keys_only] [count_only]
    if (op == proto::kOpAdminCop && parts.size() >= 5) {
      const uint32_t rid = static_cast<uint32_t>(std::stoul(parts[1]));
      CoprocessorRequest req;
      if (parts[2] != "-" && !from_hex(parts[2], &req.start_key)) return "ERR bad_hex";
      if (parts[3] != "-" && !from_hex(parts[3], &req.end_key)) return "ERR bad_hex";
      req.limit = static_cast<uint32_t>(std::stoul(parts[4]));
      for (size_t i = 5; i < parts.size(); ++i) {
        if (parts[i].rfind("kp=", 0) == 0) {
          if (!from_hex(parts[i].substr(3), &req.key_prefix)) return "ERR bad_hex";
        } else if (parts[i].rfind("vp=", 0) == 0) {
          if (!from_hex(parts[i].substr(3), &req.value_prefix)) return "ERR bad_hex";
        } else if (parts[i] == "keys_only") {
          req.keys_only = true;
        } else if (parts[i] == "count_only") {
          req.count_only = true;
        }
      }
      auto resp = facade_.node().coprocessor_execute(rid, req);
      std::ostringstream oss;
      oss << "OK COP " << resp.matched << " " << (resp.truncated ? 1 : 0) << " "
          << resp.pairs.size();
      for (const auto& p : resp.pairs) {
        oss << ' ' << to_hex(p.key) << ':' << to_hex(p.value) << ':' << p.revision;
      }
      return oss.str();
    }
    // ADMIN_REGION_STATS rid — Raft 软状态 + O2 延迟直方图摘要
    if (op == proto::kOpAdminRegionStats && parts.size() == 2) {
      uint32_t rid = static_cast<uint32_t>(std::stoul(parts[1]));
      auto* g = facade_.node().multi_raft().get(rid);
      if (!g) return "ERR no_region";
      auto st = g->soft_state();
      return "OK REGION " + parts[1] + " role=" + std::to_string(static_cast<int>(st.role)) +
             " term=" + std::to_string(st.term) + " commit=" + std::to_string(st.commit_index) +
             " applied=" + std::to_string(st.last_applied) +
             " elections=" + std::to_string(st.election_count) +
             " snaps=" + std::to_string(st.snapshot_send_count) +
             " lag=" + std::to_string(st.replication_lag) +
             " joint=" + (st.in_joint ? "1" : "0") + " " +
             facade_.metrics().region_compact(rid);
    }
    // RAFT_SNAP shard from term last_idx last_term hexdata
    if (op == proto::kOpRaftSnap && parts.size() == 7) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string data;
      if (!from_hex(parts[6], &data)) {
        return "ERR snap_bad_hex";
      }
      return facade_.node().handle_raft_snapshot(shard, parts[2], std::stoull(parts[3]),
                                                 std::stoull(parts[4]), std::stoull(parts[5]), data);
    }
    // RAFT_SNAPC shard from term last_idx last_term offset total done hexchunk
    if (op == proto::kOpRaftSnapc && parts.size() == 10) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string chunk;
      if (!from_hex(parts[9], &chunk)) return "ERR snap_bad_hex";
      return facade_.node().handle_raft_snap_chunk(
          shard, parts[2], std::stoull(parts[3]), std::stoull(parts[4]), std::stoull(parts[5]),
          std::stoull(parts[6]), std::stoull(parts[7]), parts[8] != "0", chunk);
    }
    if (op == proto::kOpPut && parts.size() == 4) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key, value;
      if (!from_hex(parts[2], &key) || !from_hex(parts[3], &value)) {
        return "ERR bad_hex";
      }
      Command cmd{CmdOp::kPut, key, value, 0};
      auto r = facade_.propose(shard, cmd);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "propose_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK PUT " + std::to_string(r.revision);
    }
    // GET rid hex_key [linearizable=1]
    if (op == proto::kOpGet && (parts.size() == 3 || parts.size() == 4)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) {
        return "ERR bad_hex";
      }
      const bool lin = parts.size() < 4 || parts[3] != "0";
      auto r = facade_.get(shard, key, lin);
      if (r.redirect_to == "no_region") return "ERR no_region";
      // 非 Leader 必须报 not_leader（即使尚无 redirect），禁止当成「空值」成功返回
      if (!r.is_leader && lin) {
        return r.redirect_to.empty() ? "ERR not_leader"
                                     : ("ERR not_leader redirect=" + r.redirect_to);
      }
      if (!r.found) {
        return "OK GET " + std::to_string(r.revision) + " -";
      }
      return "OK GET " + std::to_string(r.revision) + " " + to_hex(r.value);
    }
    if (op == proto::kOpDel && parts.size() == 3) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) {
        return "ERR bad_hex";
      }
      Command cmd{CmdOp::kDelete, key, "", 0};
      auto r = facade_.propose(shard, cmd);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "propose_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK DEL " + std::to_string(r.revision) + " 1";
    }
    // PREWRITE rid hex_key start_ts hex_primary op hex_value [epoch]
    // hex_value 可为 "-" 表示空（Delete）；避免空白字段被 split 吃掉。
    if (op == proto::kOpPrewrite && (parts.size() == 7 || parts.size() == 8)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key, primary, value;
      if (!from_hex(parts[2], &key) || !from_hex(parts[4], &primary)) {
        return "ERR bad_hex";
      }
      if (parts[6] == "-") {
        value.clear();
      } else if (!from_hex(parts[6], &value)) {
        return "ERR bad_hex";
      }
      const uint64_t start_ts = std::stoull(parts[3]);
      const uint8_t mop = static_cast<uint8_t>(std::stoul(parts[5]));
      const uint64_t epoch = parts.size() == 8 ? std::stoull(parts[7]) : 0;
      auto r = facade_.prewrite(shard, key, start_ts, primary, mop, value, epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "prewrite_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK PREWRITE " + std::to_string(r.revision);
    }
    // COMMIT rid hex_key start_ts commit_ts [epoch]
    if (op == proto::kOpCommit && (parts.size() == 5 || parts.size() == 6)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      const uint64_t epoch = parts.size() == 6 ? std::stoull(parts[5]) : 0;
      auto r = facade_.commit(shard, key, std::stoull(parts[3]), std::stoull(parts[4]), epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "commit_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK COMMIT " + std::to_string(r.revision);
    }
    // ROLLBACK rid hex_key start_ts [epoch]
    if (op == proto::kOpRollback && (parts.size() == 4 || parts.size() == 5)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      const uint64_t epoch = parts.size() == 5 ? std::stoull(parts[4]) : 0;
      auto r = facade_.rollback(shard, key, std::stoull(parts[3]), epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "rollback_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK ROLLBACK " + std::to_string(r.revision);
    }
    // PESSIMISTIC_LOCK rid hex_key start_ts for_update_ts hex_primary [ttl_ms] [epoch]
    if (op == proto::kOpPessimisticLock && parts.size() >= 6 && parts.size() <= 8) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key, primary;
      if (!from_hex(parts[2], &key) || !from_hex(parts[5], &primary)) return "ERR bad_hex";
      uint64_t ttl = 5000;
      uint64_t epoch = 0;
      if (parts.size() >= 7) ttl = std::stoull(parts[6]);
      if (parts.size() >= 8) epoch = std::stoull(parts[7]);
      auto r = facade_.pessimistic_lock(shard, key, std::stoull(parts[3]), std::stoull(parts[4]),
                                        primary, ttl, epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "pessimistic_lock_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK PESSIMISTIC_LOCK " + std::to_string(r.revision);
    }
    // PESSIMISTIC_ROLLBACK rid hex_key start_ts [epoch]
    if (op == proto::kOpPessimisticRollback && (parts.size() == 4 || parts.size() == 5)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      const uint64_t epoch = parts.size() == 5 ? std::stoull(parts[4]) : 0;
      auto r = facade_.pessimistic_rollback(shard, key, std::stoull(parts[3]), epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "pessimistic_rollback_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK PESSIMISTIC_ROLLBACK " + std::to_string(r.revision);
    }
    // RESOLVE_LOCK rid hex_key start_ts commit_ts [epoch]  (commit_ts=0 → rollback)
    if (op == proto::kOpResolveLock && (parts.size() == 5 || parts.size() == 6)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      const uint64_t epoch = parts.size() == 6 ? std::stoull(parts[5]) : 0;
      auto r =
          facade_.resolve_lock(shard, key, std::stoull(parts[3]), std::stoull(parts[4]), epoch);
      if (!r.ok) {
        return "ERR " + (r.error.empty() ? "resolve_failed" : r.error) +
               (r.redirect_to.empty() ? "" : (" redirect=" + r.redirect_to));
      }
      return "OK RESOLVE_LOCK " + std::to_string(r.revision);
    }
    // GET_LOCK rid hex_key
    if (op == proto::kOpGetLock && parts.size() == 3) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      auto r = facade_.get_lock(shard, key);
      if (r.redirect_to == "no_region") return "ERR no_region";
      if (!r.is_leader) {
        return r.redirect_to.empty() ? "ERR not_leader"
                                     : ("ERR not_leader redirect=" + r.redirect_to);
      }
      if (!r.found) return "OK GET_LOCK -";
      return "OK GET_LOCK " + std::to_string(r.lock.start_ts) + " " +
             std::to_string(static_cast<int>(r.lock.op)) + " " + to_hex(r.lock.primary) + " " +
             std::to_string(r.lock.for_update_ts) + " " + std::to_string(r.lock.ttl_ms);
    }
    // CHECK_TXN_STATUS rid hex_primary lock_ts
    if (op == proto::kOpCheckTxnStatus && parts.size() == 4) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string primary;
      if (!from_hex(parts[2], &primary)) return "ERR bad_hex";
      auto r = facade_.check_txn_status(shard, primary, std::stoull(parts[3]));
      if (r.redirect_to == "no_region") return "ERR no_region";
      if (!r.is_leader) {
        return r.redirect_to.empty() ? "ERR not_leader"
                                     : ("ERR not_leader redirect=" + r.redirect_to);
      }
      const char* st = "rolledback";
      if (r.status == TxnStatus::kLocked) st = r.lock_expired ? "expired" : "locked";
      else if (r.status == TxnStatus::kCommitted) st = "committed";
      return std::string("OK CHECK_TXN_STATUS ") + st + " " + std::to_string(r.commit_ts);
    }
    // TXN_GET rid hex_key start_ts [linearizable=1]
    if (op == proto::kOpTxnGet && (parts.size() == 4 || parts.size() == 5)) {
      uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      std::string key;
      if (!from_hex(parts[2], &key)) return "ERR bad_hex";
      const bool lin = parts.size() < 5 || parts[4] != "0";
      auto r = facade_.txn_get(shard, key, std::stoull(parts[3]), lin);
      if (r.redirect_to == "no_region") return "ERR no_region";
      if (!r.is_leader && lin) {
        return r.redirect_to.empty() ? "ERR not_leader"
                                     : ("ERR not_leader redirect=" + r.redirect_to);
      }
      if (r.locked) {
        return "ERR locked primary=" + to_hex(r.blocking_lock.primary) +
               " lock_ts=" + std::to_string(r.blocking_lock.start_ts);
      }
      if (!r.found) return "OK TXN_GET -";
      return "OK TXN_GET " + to_hex(r.value);
    }
    // WATCH_HEAD rid — 当前 Region last_applied（Watch 起点）
    if (op == proto::kOpWatchHead && parts.size() == 2) {
      const uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      auto* g = facade_.node().multi_raft().get(shard);
      if (!g) return "ERR no_region";
      return "OK WATCH_HEAD " + std::to_string(g->soft_state().last_applied);
    }
    // WATCH_POLL rid since_rev [limit] — 持久化 Watch 日志（O3）；环缓作热路径补充
    if (op == proto::kOpWatchPoll && (parts.size() == 3 || parts.size() == 4)) {
      const uint32_t shard = static_cast<uint32_t>(std::stoul(parts[1]));
      const uint64_t since = std::stoull(parts[2]);
      size_t limit = 64;
      if (parts.size() == 4) limit = static_cast<size_t>(std::stoul(parts[3]));
      auto& eng = facade_.node().engines().get_or_create(shard);
      auto evs = WatchLog::poll(eng, shard, since, limit);
      if (evs.empty()) {
        // 日志已裁剪或尚无落盘：回退环缓（进程内近期事件）
        evs = facade_.node().watch_hub().poll(shard, since, limit);
      }
      std::ostringstream oss;
      oss << "OK WATCH " << evs.size();
      uint64_t max_rev = since;
      for (const auto& e : evs) {
        if (e.revision > max_rev) max_rev = e.revision;
        oss << ' ' << to_hex(e.key) << ':' << e.revision << ':' << (e.deleted ? 'D' : 'P') << ':'
            << (e.deleted ? "-" : to_hex(e.value));
      }
      oss << ' ' << max_rev;
      return oss.str();
    }
  } catch (const std::exception& e) {
    return std::string("ERR ") + e.what();
  }
  return "ERR bad_request";
}

}  // namespace opskv::storage
