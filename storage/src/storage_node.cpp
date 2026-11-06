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

// @file storage_node.cpp
// StorageNode：Multi-Raft + Range 分片 + 持久化 / 快照 / 自动分裂。

#include "opskv/storage/storage_node.hpp"

#include "opskv/admin_acl.hpp"
#include "opskv/codec.hpp"
#include "opskv/grpc_auth.hpp"
#include "opskv/logging.hpp"
#include "opskv/protocol.hpp"
#include <grpcpp/grpcpp.h>
#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"
#include "opskv/raft/file_raft_engine.hpp"
#include "opskv/raft/raft_engine.hpp"
#include "opskv/raft/raft_persist.hpp"
#include "opskv/raftstore/msg.hpp"
#include "opskv/storage/mvcc.hpp"
#include "opskv/storage/store_service.hpp"
#include "opskv/storage/watch_log.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace opskv::storage {
namespace proto = opskv::proto;
namespace {

using opskv::from_hex;
using opskv::to_hex;

// 快照 KV blob 前缀：SKVR1\nhex(start)|hex(end)\n<data>
// 接收端用发送方区间 apply，避免本地 region_ranges_ 丢失后空 range 清空整库。
constexpr std::string_view kSnapRangeMagic = "SKVR1\n";

std::string wrap_snap_range(const opskv::KeyRange& range, std::string kv) {
  return std::string(kSnapRangeMagic) + to_hex(range.start_key) + "|" + to_hex(range.end_key) +
         "\n" + std::move(kv);
}

bool peel_snap_range(std::string_view data, opskv::KeyRange* range, std::string_view* kv) {
  if (!data.starts_with(kSnapRangeMagic)) {
    if (kv) *kv = data;
    return false;
  }
  const auto rest = data.substr(kSnapRangeMagic.size());
  const auto nl = rest.find('\n');
  if (nl == std::string_view::npos) {
    if (kv) *kv = data;
    return false;
  }
  const auto line = rest.substr(0, nl);
  const auto bar = line.find('|');
  if (bar == std::string_view::npos) {
    if (kv) *kv = data;
    return false;
  }
  if (range) {
    range->start_key.clear();
    range->end_key.clear();
    from_hex(line.substr(0, bar), &range->start_key);
    from_hex(line.substr(bar + 1), &range->end_key);
  }
  if (kv) *kv = rest.substr(nl + 1);
  return true;
}

std::string tcp_line(const std::string& addr, const std::string& req, int timeout_ms = 8000) {
  auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  auto stub = ::storage::v1::AdminService::NewStub(ch);
  ::storage::v1::ExecLineRequest er;
  er.set_line(req);
  ::storage::v1::ExecLineResponse eres;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&ctx, admin_token_from_env());
  auto st = stub->ExecLine(&ctx, er, &eres);
  if (!st.ok()) throw std::runtime_error(std::string("storage ExecLine: ") + st.error_message());
  return eres.body();
}

std::string meta_exec_line(const std::string& addr, const std::string& req, int timeout_ms = 8000) {
  auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  auto stub = ::meta::v1::MetaService::NewStub(ch);
  ::meta::v1::ExecLineRequest er;
  er.set_line(req);
  ::meta::v1::ExecLineResponse eres;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&ctx, admin_token_from_env());
  auto st = stub->ExecLine(&ctx, er, &eres);
  if (!st.ok()) throw std::runtime_error(std::string("meta ExecLine: ") + st.error_message());
  return eres.body();
}

}  // namespace

StorageNode::StorageNode(NodeConfig cfg) : cfg_(std::move(cfg)) {
  if (const char* e = std::getenv(proto::env::kPersistSync)) {
    cfg_.persist_sync = e;
  }
  const auto sync = raft::parse_sync_mode(cfg_.persist_sync);
  engines_ = std::make_unique<ShardEngineManager>(cfg_.data_dir, sync);
  engines_->migrate_legacy_shards_if_needed();
  {
    const auto raft_path = (std::filesystem::path(cfg_.data_dir) / "raft").string();
    raft_engine_ = std::make_shared<RocksRaftEngine>(raft_path, sync);
  }
  if (const char* e = std::getenv(proto::env::kSplitKeys)) {
    auto_split_keys_ = static_cast<size_t>(std::stoul(e));
  }
  if (const char* e = std::getenv(proto::env::kSplitBytes)) {
    auto_split_bytes_ = static_cast<size_t>(std::stoul(e));
  }
  if (const char* e = std::getenv(proto::env::kMergeKeys)) {
    auto_merge_keys_ = static_cast<size_t>(std::stoul(e));
  }
}

StorageNode::~StorageNode() { stop(); }

void StorageNode::publish_apply_result(uint64_t index, std::string error) {
  std::lock_guard<std::mutex> lock(apply_res_mu_);
  apply_results_[index] = std::move(error);
  // 防止无客户端等待时无限膨胀
  if (apply_results_.size() > 4096) {
    auto it = apply_results_.begin();
    while (apply_results_.size() > 2048 && it != apply_results_.end()) {
      if (it->first + 1024 < index) {
        it = apply_results_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

std::optional<std::string> StorageNode::take_apply_result(uint64_t index) {
  std::lock_guard<std::mutex> lock(apply_res_mu_);
  auto it = apply_results_.find(index);
  if (it == apply_results_.end()) return std::nullopt;
  std::string err = std::move(it->second);
  apply_results_.erase(it);
  return err;
}

std::optional<StorageNode::IdempotentRecord> StorageNode::lookup_request(
    uint32_t shard_id, uint64_t request_id) const {
  if (request_id == 0) return std::nullopt;
  std::lock_guard<std::mutex> lock(req_mu_);
  auto sit = req_cache_.find(shard_id);
  if (sit == req_cache_.end()) return std::nullopt;
  auto it = sit->second.find(request_id);
  if (it == sit->second.end()) return std::nullopt;
  return it->second;
}

void StorageNode::remember_request(uint32_t shard_id, uint64_t request_id, IdempotentRecord rec) {
  if (request_id == 0) return;
  std::lock_guard<std::mutex> lock(req_mu_);
  auto& map = req_cache_[shard_id];
  auto& order = req_order_[shard_id];
  if (map.count(request_id)) {
    map[request_id] = std::move(rec);
    return;
  }
  map[request_id] = std::move(rec);
  order.push_back(request_id);
  constexpr size_t kMax = 4096;
  while (order.size() > kMax) {
    map.erase(order.front());
    order.pop_front();
  }
}

void StorageNode::on_apply(uint32_t shard_id, uint64_t index, const std::string& data) {
  Command cmd;
  if (!decode_command(data, &cmd)) {
    SKV_WARN << "bad command at index=" << index;
    publish_apply_result(index, "bad_command");
    return;
  }
  if (cmd.request_id != 0) {
    if (auto hit = lookup_request(shard_id, cmd.request_id)) {
      // 重复 apply：跳过状态机副作用，回传首次结果
      publish_apply_result(index, hit->error);
      return;
    }
  }
  const uint64_t req_id = cmd.request_id;
  auto finish = [this, shard_id, index, req_id](std::string err = {}) {
    publish_apply_result(index, err);
    if (req_id != 0) {
      IdempotentRecord rec;
      rec.ok = err.empty();
      rec.revision = index;
      rec.error = err;
      remember_request(shard_id, req_id, std::move(rec));
    }
  };
  auto& eng = engines_->get_or_create(shard_id);
  switch (cmd.op) {
    case CmdOp::kPut:
      eng.put(cmd.key, cmd.value, index);
      {
        WatchRecord wr{shard_id, index, false, cmd.key, cmd.value};
        WatchLog::append(eng, wr);
        watch_hub_.push(std::move(wr));
      }
      finish();
      break;
    case CmdOp::kDelete:
      eng.del(cmd.key, index);
      {
        WatchRecord wr{shard_id, index, true, cmd.key, {}};
        WatchLog::append(eng, wr);
        watch_hub_.push(std::move(wr));
      }
      finish();
      break;
    case CmdOp::kPrewrite: {
      uint64_t start_ts = 0;
      uint8_t mop = 0;
      std::string primary, value;
      if (!decode_prewrite_value(cmd.value, &start_ts, &mop, &primary, &value)) {
        SKV_WARN << "bad prewrite at index=" << index;
        finish("bad_prewrite");
        break;
      }
      MvccStore mvcc(eng);
      if (auto lk = mvcc.get_lock(cmd.key)) {
        if (lk->start_ts != start_ts) {
          finish("locked");
          break;
        }
        // 同事务：允许从悲观锁升级，或幂等覆盖同 op
        if (lk->op != MvccOp::kPessimistic && lk->op == static_cast<MvccOp>(mop)) {
          finish();
          break;
        }
      } else if (mvcc.has_write_conflict(cmd.key, start_ts)) {
        finish("write_conflict");
        break;
      }
      LockInfo li;
      li.start_ts = start_ts;
      li.op = static_cast<MvccOp>(mop);
      li.primary = std::move(primary);
      li.value = value;
      li.lock_wall_ms = wall_ms_now();
      mvcc.put_lock(cmd.key, li, index);
      if (li.op == MvccOp::kPut) {
        mvcc.put_data(cmd.key, start_ts, value, index);
      }
      finish();
      break;
    }
    case CmdOp::kCommit: {
      uint64_t start_ts = 0, commit_ts = 0;
      if (!decode_commit_value(cmd.value, &start_ts, &commit_ts)) {
        SKV_WARN << "bad commit at index=" << index;
        finish("bad_commit");
        break;
      }
      MvccStore mvcc(eng);
      if (mvcc.find_commit_ts(cmd.key, start_ts)) {
        finish();  // idempotent
        break;
      }
      auto lk = mvcc.get_lock(cmd.key);
      if (!lk || lk->start_ts != start_ts) {
        finish("lock_not_found");
        break;
      }
      if (lk->op == MvccOp::kPessimistic) {
        finish("pessimistic_not_prewritten");
        break;
      }
      WriteInfo wi;
      wi.start_ts = start_ts;
      wi.op = lk->op;
      const bool is_del = lk->op == MvccOp::kDelete;
      std::string watch_val = is_del ? std::string{} : lk->value;
      mvcc.put_write(cmd.key, commit_ts, wi, index);
      mvcc.clear_lock(cmd.key, index);
      // O3：Watch revision 统一为 Raft apply index（不再用 commit_ts）
      {
        WatchRecord wr{shard_id, index, is_del, cmd.key, std::move(watch_val)};
        WatchLog::append(eng, wr);
        watch_hub_.push(std::move(wr));
      }
      finish();
      break;
    }
    case CmdOp::kRollback: {
      uint64_t start_ts = 0;
      if (!decode_rollback_value(cmd.value, &start_ts)) {
        SKV_WARN << "bad rollback at index=" << index;
        finish("bad_rollback");
        break;
      }
      MvccStore mvcc(eng);
      auto lk = mvcc.get_lock(cmd.key);
      if (!lk || lk->start_ts != start_ts) {
        finish();  // idempotent
        break;
      }
      if (lk->op == MvccOp::kPut) {
        mvcc.clear_data(cmd.key, start_ts, index);
      }
      mvcc.clear_lock(cmd.key, index);
      finish();
      break;
    }
    case CmdOp::kPessimisticLock: {
      uint64_t start_ts = 0, for_update_ts = 0, ttl_ms = 5000;
      std::string primary;
      if (!decode_pessimistic_lock_value(cmd.value, &start_ts, &for_update_ts, &ttl_ms, &primary)) {
        SKV_WARN << "bad pessimistic_lock at index=" << index;
        finish("bad_pessimistic_lock");
        break;
      }
      MvccStore mvcc(eng);
      if (auto lk = mvcc.get_lock(cmd.key)) {
        if (lk->start_ts == start_ts) {
          finish();  // idempotent
          break;
        }
        finish("locked");
        break;
      }
      if (mvcc.has_write_conflict(cmd.key, for_update_ts != 0 ? for_update_ts : start_ts)) {
        finish("write_conflict");
        break;
      }
      LockInfo li;
      li.start_ts = start_ts;
      li.op = MvccOp::kPessimistic;
      li.primary = std::move(primary);
      li.for_update_ts = for_update_ts;
      li.ttl_ms = ttl_ms;
      li.lock_wall_ms = wall_ms_now();
      mvcc.put_lock(cmd.key, li, index);
      finish();
      break;
    }
    case CmdOp::kPessimisticRollback: {
      uint64_t start_ts = 0;
      if (!decode_rollback_value(cmd.value, &start_ts)) {
        SKV_WARN << "bad pessimistic_rollback at index=" << index;
        finish("bad_pessimistic_rollback");
        break;
      }
      MvccStore mvcc(eng);
      auto lk = mvcc.get_lock(cmd.key);
      if (!lk || lk->start_ts != start_ts || lk->op != MvccOp::kPessimistic) {
        finish();  // idempotent / no-op
        break;
      }
      mvcc.clear_lock(cmd.key, index);
      finish();
      break;
    }
    case CmdOp::kGcBelow: {
      uint64_t safe_ts = 0;
      if (cmd.value.size() >= sizeof(uint64_t)) {
        std::memcpy(&safe_ts, cmd.value.data(), sizeof(safe_ts));
      }
      MvccStore mvcc(eng);
      mvcc.gc_below(safe_ts);
      finish();
      break;
    }
    case CmdOp::kAdminPrepareSplit:
      set_split_pause(shard_id, true, cmd.key);
      finish();
      break;
    case CmdOp::kAdminSplit: {
      // 同机不搬 KV，只改区间元数据 + 共享引擎；跨机靠 Place。
      if (cmd.value.size() < sizeof(uint32_t)) {
        SKV_WARN << "bad admin_split at index=" << index;
        finish("bad_admin_split");
        break;
      }
      uint32_t right = 0;
      std::memcpy(&right, cmd.value.data(), sizeof(right));
      set_split_pause(shard_id, true, cmd.key);
      if (!multi_raft_->get(right)) {
        std::vector<raft::Peer> peers;
        if (auto* lg = multi_raft_->get(shard_id)) {
          peers = lg->peers();
          for (auto& p : peers) {
            if (p.address.empty()) p.address = peer_address(p.id);
          }
        }
        if (peers.empty()) peers = bootstrap_peers_;
        std::lock_guard<std::mutex> lock(create_mu_);
        pending_peers_[right] = std::move(peers);
      }
      ensure_region(right);
      engines_->get_or_create(shard_id);
      if (!engines_->share_with(right, shard_id)) {
        engines_->get_or_create(right);
        engines_->share_with(right, shard_id);
      }
      auto left_range = region_range(shard_id);
      opskv::KeyRange right_range;
      right_range.start_key = cmd.key;
      right_range.end_key = left_range.end_key;
      left_range.end_key = cmd.key;
      set_region_range(shard_id, left_range);
      set_region_range(right, right_range);
      // version 抬高：此处左右各 bump / 置位以抬高 epoch
      bump_region_epoch(shard_id);
      set_region_epoch(right, region_epoch(shard_id));
      set_split_pause(shard_id, false);
      SKV_INFO << "apply ADMIN_SPLIT (meta-only) left=" << shard_id << " right=" << right
               << " split_key_len=" << cmd.key.size() << " idx=" << index;
      finish();
      break;
    }
    case CmdOp::kAdminMerge: {
      if (cmd.value.size() < sizeof(uint32_t)) {
        SKV_WARN << "bad admin_merge at index=" << index;
        finish("bad_admin_merge");
        break;
      }
      uint32_t right = 0;
      std::memcpy(&right, cmd.value.data(), sizeof(right));
      ensure_region(shard_id);
      auto left_range = region_range(shard_id);
      auto right_range = region_range(right);
      if (engines_->shares_engine(shard_id, right)) {
        // 同机共享引擎：只扩左区间，不搬数据
        left_range.end_key = right_range.end_key;
        set_region_range(shard_id, left_range);
        SKV_INFO << "apply ADMIN_MERGE (shared-engine) left=" << shard_id << " right=" << right
                 << " idx=" << index;
      } else if (auto* right_eng = engines_->get(right)) {
        auto& left = engines_->get_or_create(shard_id);
        size_t n = right_eng->migrate_all(left);
        left_range.end_key = right_range.end_key.empty() ? left_range.end_key : right_range.end_key;
        set_region_range(shard_id, left_range);
        SKV_INFO << "apply ADMIN_MERGE left=" << shard_id << " right=" << right << " moved=" << n
                 << " idx=" << index;
      }
      bump_region_epoch(shard_id);
      finish();
      break;
    }
    case CmdOp::kAdminSetEpoch: {
      if (cmd.value.size() < sizeof(uint64_t)) {
        finish("bad_admin_set_epoch");
        break;
      }
      uint64_t epoch = 0;
      std::memcpy(&epoch, cmd.value.data(), sizeof(epoch));
      // 只升不降：避免 Gateway 用过期 Meta 路由把 Split 后的 epoch 打回去。
      if (epoch > region_epoch(shard_id)) {
        set_region_epoch(shard_id, epoch);
      }
      finish();
      break;
    }
    default:
      finish("unknown_op");
      break;
  }
  // 注意：不可在 apply 路径里同步打 Meta/对端 TCP（会死锁/拖垮 Raft）
}

bool StorageNode::add_learner(uint32_t shard_id, const raft::Peer& peer) {
  if (!peer.address.empty()) transport_.set_peer(peer.id, peer.address);
  // 禁止 get_or_create：Joining 节点 raft_peers 常为单机，误建会成孤儿 Leader 脑裂。
  auto* g = multi_raft_->get(shard_id);
  if (!g || !g->is_leader()) return false;
  raft::ConfChange cc;
  cc.type = raft::ConfChange::kAddLearner;
  cc.peer = peer;
  return g->propose_conf_change(cc);
}

bool StorageNode::promote_learner(uint32_t shard_id, const std::string& peer_id) {
  auto* g = multi_raft_->get(shard_id);
  if (!g || !g->is_leader()) return false;
  raft::ConfChange cc;
  cc.type = raft::ConfChange::kPromoteLearner;
  cc.peer.id = peer_id;
  return g->propose_conf_change(cc);
}

bool StorageNode::remove_peer(uint32_t shard_id, const std::string& peer_id) {
  auto* g = multi_raft_->get(shard_id);
  if (!g || !g->is_leader()) return false;
  raft::ConfChange cc;
  cc.type = raft::ConfChange::kRemoveNode;
  cc.peer.id = peer_id;
  return g->propose_conf_change(cc);
}

bool StorageNode::region_destroyed(uint32_t region_id) const {
  std::lock_guard<std::mutex> lock(destroy_mu_);
  return destroyed_regions_.count(region_id) > 0;
}

void StorageNode::mark_region_destroyed(uint32_t region_id) {
  std::lock_guard<std::mutex> lock(destroy_mu_);
  destroyed_regions_.insert(region_id);
}

void StorageNode::clear_region_destroyed(uint32_t region_id) {
  std::lock_guard<std::mutex> lock(destroy_mu_);
  destroyed_regions_.erase(region_id);
}

std::string StorageNode::admin_ensure_region(uint32_t region_id,
                                            const std::vector<raft::Peer>& peers) {
  try {
    for (const auto& p : peers) {
      if (!p.address.empty()) transport_.set_peer(p.id, p.address);
    }
    clear_region_destroyed(region_id);
    bool want_learner = false;
    for (const auto& p : peers) {
      if (p.id == cfg_.node_id) {
        want_learner = p.learner;
        break;
      }
    }
    // Place/SCALE 目标常带 *N：新副本须以 learner 落盘。
    // 仅拆「单副本孤儿 Leader」（Joining 误触 get_or_create）；已在多数派中的 voter
    // 若因 place 重试再次 ENSURE *N，绝不能 wipe，否则会毁掉刚 promote 的副本。
    bool need_recreate = false;
    if (auto* existing = multi_raft_->get(region_id)) {
      const size_t np = existing->peers().size();
      const bool solo_orphan_leader =
          want_learner && existing->is_leader() && np <= 1 && !existing->self_is_learner();
      if (!solo_orphan_leader) return "OK ENSURE exists";
      need_recreate = true;
      SKV_WARN << "ENSURE recreate region=" << region_id
               << " (solo orphan leader, want learner)";
      transport_.unregister_node(region_id);
      multi_raft_->take(region_id);
    }
    if (need_recreate || want_learner) {
      if (raft_engine_) {
        raft_engine_->clean(region_id);
        SKV_INFO << "ENSURE wiped raft engine group=" << region_id;
      }
      const auto persist_dir =
          (std::filesystem::path(cfg_.data_dir) / ("raft_region_" + std::to_string(region_id)))
              .string();
      std::error_code ec;
      if (std::filesystem::exists(persist_dir, ec)) {
        std::filesystem::remove_all(persist_dir, ec);
        if (ec) {
          SKV_WARN << "ENSURE wipe persist failed region=" << region_id
                   << " err=" << ec.message();
        } else {
          SKV_INFO << "ENSURE wiped legacy persist region=" << region_id;
        }
      }
    }
    // 同机兄弟 Region 可能仍占共享引擎：先占位 map，避免 factory get_or_create 二次 open
    try {
      engines_->get_or_create(region_id);
    } catch (const std::exception& e) {
      SKV_WARN << "ENSURE engine warm region=" << region_id << " " << e.what();
      return std::string("ERR ") + e.what();
    }
    {
      std::lock_guard<std::mutex> lock(create_mu_);
      pending_peers_[region_id] = peers.empty() ? bootstrap_peers_ : peers;
    }
    auto& g = multi_raft_->get_or_create(region_id);
    g.recover_apply();
    return "OK ENSURE created";
  } catch (const std::exception& e) {
    SKV_WARN << "ENSURE failed region=" << region_id << " " << e.what();
    return std::string("ERR ") + e.what();
  }
}

std::string StorageNode::admin_add_learner(uint32_t region_id, const raft::Peer& peer) {
  if (!add_learner(region_id, peer)) {
    auto* g = multi_raft_->get(region_id);
    if (!g) return "ERR no_region";
    if (!g->is_leader()) {
      auto addr = peer_address(g->soft_state().leader_id);
      return "ERR not_leader" + (addr.empty() ? "" : (" redirect=" + addr));
    }
    return "ERR add_learner_failed";
  }
  return "OK ADD_LEARNER " + peer.id;
}

std::string StorageNode::admin_promote(uint32_t region_id, const std::string& peer_id) {
  if (!promote_learner(region_id, peer_id)) {
    auto* g = multi_raft_->get(region_id);
    if (!g) return "ERR no_region";
    if (!g->is_leader()) {
      auto addr = peer_address(g->soft_state().leader_id);
      return "ERR not_leader" + (addr.empty() ? "" : (" redirect=" + addr));
    }
    if (g->conf_change_busy()) return "ERR busy class=joint";
    bool found = false;
    bool learner = false;
    for (const auto& p : g->peers()) {
      if (p.id == peer_id) {
        found = true;
        learner = p.learner;
        break;
      }
    }
    // 换主后成员表丢 Learner（快照未带 ConfState）时常见
    if (!found) return "ERR unknown_peer";
    // 已是 voter：幂等成功，避免 Meta 任务卡死
    if (!learner) return "OK PROMOTE " + peer_id;
    return "ERR promote_failed";
  }
  return "OK PROMOTE " + peer_id;
}

std::string StorageNode::admin_remove_peer(uint32_t region_id, const std::string& peer_id) {
  if (!remove_peer(region_id, peer_id)) {
    auto* g = multi_raft_->get(region_id);
    if (!g) return "ERR no_region";
    if (!g->is_leader()) {
      auto addr = peer_address(g->soft_state().leader_id);
      return "ERR not_leader" + (addr.empty() ? "" : (" redirect=" + addr));
    }
    // Joint / LeaveJoint 进行中时 ConfChange 会拒绝，客户端应短暂重试
    if (g->conf_change_busy()) return "ERR busy class=joint";
    return "ERR remove_failed";
  }
  return "OK REMOVE " + peer_id;
}

std::string StorageNode::admin_destroy_region(uint32_t region_id) {
  // 先卸传输，再从 MultiRaft 摘下；shared_ptr 让在途 tick/RPC 自然收尾，避免 mutex UAF。
  transport_.unregister_node(region_id);
  mark_region_destroyed(region_id);
  auto held = multi_raft_->take(region_id);
  if (raft_engine_) raft_engine_->clean(region_id);
  if (!held) {
    return "OK DESTROY absent";
  }
  const auto dying = region_range(region_id);
  // Merge 后左片区间已覆盖右片：共享引擎上不可再 delete_range。
  // Place 踢出右片：左片不覆盖 → 删本区间 key，避免盘占用滞留。
  // 无界 range（DESTROY 后 region_ranges_ 已丢、再 ENSURE 未恢复）：禁止整库扫删。
  const bool unbounded = dying.start_key.empty() && dying.end_key.empty();
  bool covered_by_sibling = false;
  bool has_sibling = false;
  {
    std::lock_guard<std::mutex> lock(range_mu_);
    for (const auto& [oid, orange] : region_ranges_) {
      if (oid == region_id) continue;
      has_sibling = true;
      const bool start_ok =
          orange.start_key.empty() ||
          (!dying.start_key.empty() && orange.start_key <= dying.start_key);
      const bool end_ok = orange.end_key.empty()
                              ? true
                              : (!dying.end_key.empty() && orange.end_key >= dying.end_key);
      if (dying.end_key.empty() && !orange.end_key.empty()) continue;
      if (start_ok && end_ok) {
        covered_by_sibling = true;
        break;
      }
    }
  }
  size_t deleted = 0;
  size_t refs_left = 0;
  if (auto* eng = engines_->get(region_id)) {
    if (unbounded && has_sibling) {
      SKV_WARN << "ADMIN_DESTROY skip delete_range region=" << region_id
               << " unbounded range with sibling (shared-engine safety)";
    } else if (!covered_by_sibling && !unbounded) {
      deleted = eng->delete_range(dying);
    } else if (!covered_by_sibling && unbounded && !has_sibling) {
      // 单 Region 整机：允许清盘
      deleted = eng->delete_range(dying);
    }
    refs_left = engines_->release(region_id);
  }
  {
    std::lock_guard<std::mutex> lock(range_mu_);
    region_ranges_.erase(region_id);
  }
  SKV_INFO << "ADMIN_DESTROY region=" << region_id << " deleted_keys=" << deleted
           << " covered_by_sibling=" << (covered_by_sibling ? 1 : 0)
           << " engine_refs_left=" << refs_left;
  held.reset();
  return "OK DESTROY " + std::to_string(region_id);
}

std::string StorageNode::admin_peer_progress(uint32_t region_id, const std::string& peer_id) {
  auto* g = multi_raft_->get(region_id);
  if (!g) return "ERR no_region";
  if (!g->is_leader()) {
    auto addr = peer_address(g->soft_state().leader_id);
    return "ERR not_leader" + (addr.empty() ? "" : (" redirect=" + addr));
  }
  const uint64_t match = g->match_index_of(peer_id);
  const auto st = g->soft_state();
  const uint64_t last = g->last_index();
  // 已包含全部已提交日志即可视为追赶上（允许 last > match 的未提交尾巴）
  const int caught = (match >= st.commit_index) ? 1 : 0;
  // learner: 0=voter 1=learner 2=本 Leader 成员表中尚无此 peer（换主后常见）
  int learner = 2;
  for (const auto& p : g->peers()) {
    if (p.id == peer_id) {
      learner = p.learner ? 1 : 0;
      break;
    }
  }
  return "OK PROGRESS " + std::to_string(match) + " " + std::to_string(st.commit_index) + " " +
         std::to_string(last) + " " + std::to_string(caught) + " " + std::to_string(learner);
}

std::string StorageNode::admin_transfer_leader(uint32_t region_id, const std::string& target_id) {
  auto* g = multi_raft_->get(region_id);
  if (!g) return "ERR no_region";
  if (!g->is_leader()) {
    auto addr = peer_address(g->soft_state().leader_id);
    return "ERR not_leader" + (addr.empty() ? "" : (" redirect=" + addr));
  }
  if (target_id == cfg_.node_id) return "OK TRANSFER " + target_id;
  std::string target_addr;
  for (const auto& p : g->peers()) {
    if (p.id == target_id) {
      if (p.learner) return "ERR target_is_learner";
      target_addr = p.address.empty() ? peer_address(p.id) : p.address;
      break;
    }
  }
  if (target_addr.empty()) return "ERR unknown_target";

  // C7：让位前等目标追平已提交日志（match >= commit），避免转主后立刻空洞。
  bool caught = false;
  for (int i = 0; i < 100; ++i) {  // ~5s
    g = multi_raft_->get(region_id);
    if (!g || !g->is_leader()) {
      return "ERR lost_leadership";
    }
    const auto st = g->soft_state();
    if (g->match_index_of(target_id) >= st.commit_index) {
      caught = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!caught) {
    SKV_WARN << "TRANSFER region=" << region_id << " target=" << target_id
             << " not caught up (match < commit)";
    return "ERR target_not_caught_up";
  }

  if (!g->abdicate()) return "ERR abdicate_failed";
  bool camp_ok = false;
  try {
    auto resp = tcp_line(target_addr, "ADMIN_CAMPAIGN " + std::to_string(region_id), 2000);
    SKV_INFO << "TRANSFER region=" << region_id << " -> " << target_id << " campaign=" << resp;
    camp_ok = resp.rfind("OK", 0) == 0;
  } catch (const std::exception& e) {
    SKV_WARN << "TRANSFER campaign failed: " << e.what();
  }
  // 等目标（或任一副本）真正成为 Leader，缩短全员 not_leader 窗口
  for (int i = 0; i < 40; ++i) {
    auto* cur = multi_raft_->get(region_id);
    if (cur) {
      const auto st = cur->soft_state();
      if (!st.leader_id.empty()) {
        if (st.leader_id == target_id || camp_ok) {
          return "OK TRANSFER " + target_id;
        }
        // 别人已当选也算转主完成（避免卡死）
        return "OK TRANSFER " + st.leader_id;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  // 目标竞选失败：本节点再抢一轮，避免长期无主
  if (auto* cur = multi_raft_->get(region_id)) {
    if (cur->soft_state().leader_id.empty()) {
      cur->force_election();
      SKV_WARN << "TRANSFER fallback self campaign region=" << region_id;
    }
  }
  return camp_ok ? ("OK TRANSFER " + target_id) : "ERR campaign_failed";
}

std::string StorageNode::admin_campaign(uint32_t region_id) {
  auto* g = multi_raft_->get(region_id);
  if (!g) return "ERR no_region";
  if (!g->force_election()) return "ERR campaign_failed";
  {
    std::lock_guard<std::mutex> lock(campaign_mu_);
    last_campaign_at_[region_id] = std::chrono::steady_clock::now();
  }
  return "OK CAMPAIGN " + std::to_string(region_id);
}

bool StorageNode::maybe_campaign(uint32_t region_id, const std::string& addr,
                                 std::chrono::milliseconds min_gap) {
  if (addr.empty()) return false;
  // 已有明确 Leader 则不必再选
  if (auto* g = multi_raft_->get(region_id)) {
    if (!g->soft_state().leader_id.empty() && g->is_leader()) return false;
    // 本机不是该 Region 成员时只做远程 campaign；若本机已有主则跳过
    if (!g->soft_state().leader_id.empty()) return false;
  }
  {
    std::lock_guard<std::mutex> lock(campaign_mu_);
    auto it = last_campaign_at_.find(region_id);
    const auto now = std::chrono::steady_clock::now();
    if (it != last_campaign_at_.end() && now - it->second < min_gap) return false;
    last_campaign_at_[region_id] = now;
  }
  try {
    auto r = tcp_line(addr, "ADMIN_CAMPAIGN " + std::to_string(region_id), 1500);
    SKV_INFO << "maybe_campaign region=" << region_id << " -> " << r;
    return r.rfind("OK", 0) == 0;
  } catch (const std::exception& e) {
    SKV_WARN << "maybe_campaign FAIL region=" << region_id << " " << e.what();
    return false;
  }
}

std::string StorageNode::peer_address(const std::string& node_id) const {
  auto a = transport_.address_of(node_id);
  if (!a.empty()) return a;
  for (const auto& p : bootstrap_peers_) {
    if (p.id == node_id) return p.address;
  }
  return {};
}

std::string StorageNode::advertise_address() const {
  if (!cfg_.advertise_addr.empty()) return cfg_.advertise_addr;
  std::string adv = cfg_.listen_addr;
  if (adv.rfind("0.0.0.0", 0) == 0) adv.replace(0, 7, "127.0.0.1");
  return adv;
}

std::string StorageNode::handle_raft_vote(uint32_t shard, const std::string& from, uint64_t term,
                                          uint64_t last_idx, uint64_t last_term) {
  if (region_destroyed(shard)) return "ERR destroyed";
  // S2：入站只进 Store/Router，禁止直调协议状态机。
  std::ostringstream payload;
  payload << term << '|' << last_idx << '|' << last_term;
  return multi_raft_->store().apply_inbound_raft(
      shard, raftstore::RaftMessage{from, "RequestVote", payload.str()});
}

std::string StorageNode::handle_raft_pre_vote(uint32_t shard, const std::string& from,
                                              uint64_t term, uint64_t last_idx,
                                              uint64_t last_term) {
  if (region_destroyed(shard)) return "ERR destroyed";
  std::ostringstream payload;
  payload << term << '|' << last_idx << '|' << last_term;
  return multi_raft_->store().apply_inbound_raft(
      shard, raftstore::RaftMessage{from, "RequestPreVote", payload.str()});
}

std::string StorageNode::handle_raft_append(uint32_t shard, const std::string& from, uint64_t term,
                                            uint64_t prev_idx, uint64_t prev_term,
                                            uint64_t leader_commit,
                                            const std::vector<raft::LogEntry>& entries) {
  if (region_destroyed(shard)) return "ERR destroyed";
  std::ostringstream payload;
  payload << term << '|' << prev_idx << '|' << prev_term << '|' << leader_commit << '|'
          << entries.size();
  for (const auto& e : entries) {
    payload << '|' << e.term << '|' << e.index << '|' << to_hex(e.data);
  }
  return multi_raft_->store().apply_inbound_raft(
      shard, raftstore::RaftMessage{from, "AppendEntries", payload.str()});
}

std::string StorageNode::handle_raft_snapshot(uint32_t shard, const std::string& from, uint64_t term,
                                              uint64_t last_idx, uint64_t last_term,
                                              std::string_view data) {
  if (region_destroyed(shard)) return "ERR destroyed";
  std::ostringstream payload;
  payload << term << '|' << last_idx << '|' << last_term << '|' << to_hex(data);
  return multi_raft_->store().apply_inbound_raft(
      shard, raftstore::RaftMessage{from, "InstallSnapshot", payload.str()});
}

std::string StorageNode::handle_raft_snap_chunk(uint32_t shard, const std::string& from,
                                                uint64_t term, uint64_t last_idx,
                                                uint64_t last_term, uint64_t offset, uint64_t total,
                                                bool done, std::string_view chunk) {
  if (region_destroyed(shard)) return "ERR destroyed";
  raft::SnapStreamBuffer::Key key{from, shard, last_idx};
  bool complete = false;
  std::string data;
  uint64_t out_term = 0, out_last_term = 0;
  if (!snap_buf_.append(key, term, last_term, offset, total, done, chunk, &complete, &data,
                        &out_term, &out_last_term)) {
    snap_buf_.drop(key);
    auto* g = multi_raft_->get(shard);
    return "ERR snapc " + std::to_string(g ? g->soft_state().term : 0);
  }
  if (!complete) {
    auto* g = multi_raft_->get(shard);
    return "OK SNAPC " + std::to_string(g ? g->soft_state().term : term) + " 1 " +
           std::to_string(offset + chunk.size());
  }
  return handle_raft_snapshot(shard, from, out_term, last_idx, out_last_term, data);
}

void StorageNode::ensure_region(uint32_t region_id) {
  clear_region_destroyed(region_id);
  const bool created = multi_raft_->get(region_id) == nullptr;
  auto& g = multi_raft_->get_or_create(region_id);
  // 已存在且可能正处 apply 路径时，recover_apply 内部会自行跳过（in_on_apply）
  if (created) g.recover_apply();
  // S1：从引擎恢复 epoch
  if (uint64_t e = load_epoch_from_engine(region_id); e != 0) {
    std::lock_guard<std::mutex> lock(epoch_mu_);
    if (!region_epochs_.count(region_id)) region_epochs_[region_id] = e;
  }
}

uint64_t StorageNode::load_epoch_from_engine(uint32_t region_id) {
  auto* eng = engines_->get(region_id);
  if (!eng) return 0;
  auto rec = eng->get(region_epoch_storage_key(region_id));
  if (!rec || rec->value.size() < sizeof(uint64_t)) return 0;
  uint64_t e = 0;
  std::memcpy(&e, rec->value.data(), sizeof(e));
  return e == 0 ? 1 : e;
}

void StorageNode::persist_epoch(uint32_t region_id, uint64_t epoch) {
  auto& eng = engines_->get_or_create(region_id);
  std::string v(sizeof(epoch), '\0');
  std::memcpy(v.data(), &epoch, sizeof(epoch));
  eng.put(region_epoch_storage_key(region_id), v, /*revision=*/0);
}

opskv::KeyRange StorageNode::region_range(uint32_t region_id) const {
  std::lock_guard<std::mutex> lock(range_mu_);
  auto it = region_ranges_.find(region_id);
  if (it != region_ranges_.end()) return it->second;
  return {};  // 默认全空间
}

void StorageNode::set_region_range(uint32_t region_id, opskv::KeyRange range) {
  {
    std::lock_guard<std::mutex> lock(range_mu_);
    region_ranges_[region_id] = range;
  }
  if (multi_raft_) {
    multi_raft_->update_peer_meta(region_id, std::move(range), region_epoch(region_id));
  }
}

bool StorageNode::key_in_region(uint32_t region_id, std::string_view key) const {
  return region_range(region_id).contains(key);
}

uint64_t StorageNode::region_epoch(uint32_t region_id) const {
  std::lock_guard<std::mutex> lock(epoch_mu_);
  auto it = region_epochs_.find(region_id);
  if (it != region_epochs_.end()) return it->second;
  return 1;
}

void StorageNode::set_region_epoch(uint32_t region_id, uint64_t epoch) {
  const uint64_t e = epoch == 0 ? 1 : epoch;
  {
    std::lock_guard<std::mutex> lock(epoch_mu_);
    region_epochs_[region_id] = e;
  }
  persist_epoch(region_id, e);
  if (multi_raft_) {
    multi_raft_->update_peer_meta(region_id, region_range(region_id), e);
  }
}

void StorageNode::bump_region_epoch(uint32_t region_id) {
  uint64_t e = 1;
  {
    std::lock_guard<std::mutex> lock(epoch_mu_);
    auto& cur = region_epochs_[region_id];
    if (cur == 0) cur = 1;
    ++cur;
    e = cur;
  }
  persist_epoch(region_id, e);
  if (multi_raft_) {
    multi_raft_->update_peer_meta(region_id, region_range(region_id), e);
  }
}

bool StorageNode::propose_encoded(uint32_t shard_id, std::string encoded, std::string* error,
                                  std::string* redirect) {
  // 禁止 get_or_create：写路径误触会在 Joining 节点上用单机 peers 拉起孤儿 Leader。
  auto* g = multi_raft_->get(shard_id);
  if (!g) {
    if (error) *error = "no_region";
    return false;
  }
  if (!g->is_leader()) {
    if (redirect) {
      *redirect = g->leader_address();
      if (redirect->empty()) *redirect = peer_address(g->soft_state().leader_id);
    }
    if (error) *error = "not_leader";
    return false;
  }
  multi_raft_->mark_hot(shard_id);
  uint64_t idx = 0;
  // Scheduler：按 payload 前缀作 latch key（简化）
  const std::string latch =
      encoded.size() > 16 ? encoded.substr(0, 16) : encoded;
  auto write = [&](uint32_t rid, std::string data, uint64_t* out_idx, std::string* err) {
    if (raftkv_) return raftkv_->async_write(rid, std::move(data), out_idx, err);
    if (!g->propose(std::move(data), out_idx)) {
      if (err) *err = "propose_failed";
      return false;
    }
    return true;
  };
  bool ok = false;
  if (scheduler_) {
    scheduler_->set_write_fn(write);
    ok = scheduler_->schedule_write(shard_id, latch, std::move(encoded), &idx, error);
  } else {
    ok = write(shard_id, std::move(encoded), &idx, error);
  }
  if (!ok) return false;
  // gRPC 复制路径下 apply 可能需数秒；与 Go storagetcp 15s 超时对齐。
  for (int i = 0; i < 2000; ++i) {
    if (g->soft_state().last_applied >= idx) {
      auto ar = take_apply_result(idx);
      if (ar && !ar->empty()) {
        if (error) *error = *ar;
        return false;
      }
      return true;
    }
    if (i % 10 == 9) {
      g->tick();
      if (raftkv_) raftkv_->flush_ready(shard_id);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  if (error) *error = "apply_timeout";
  return false;
}

std::string StorageNode::admin_set_epoch(uint32_t region_id, uint64_t epoch) {
  Command cmd;
  cmd.op = CmdOp::kAdminSetEpoch;
  cmd.value.assign(reinterpret_cast<const char*>(&epoch), sizeof(epoch));
  std::string err, redir;
  if (!propose_encoded(region_id, encode_command(cmd), &err, &redir)) {
    return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
  }
  return "OK EPOCH " + std::to_string(region_epoch(region_id));
}

std::string StorageNode::admin_checkpoint(uint32_t region_id, std::string_view path) {
  auto* eng = engines_->get(region_id);
  if (!eng) {
    engines_->get_or_create(region_id);
    eng = engines_->get(region_id);
  }
  if (!eng) return "ERR no_engine";
  std::string dest(path);
  if (dest.empty()) {
    const auto ts = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    dest = (std::filesystem::path(cfg_.data_dir) /
            ("ckpt_region_" + std::to_string(region_id) + "_" + std::to_string(ts)))
               .string();
  }
  if (!eng->create_checkpoint(dest)) {
    return "ERR checkpoint_failed path=" + dest;
  }
  return "OK CHECKPOINT " + std::to_string(region_id) + " " + dest;
}

void StorageNode::set_split_pause(uint32_t region_id, bool on, std::string_view split_key) {
  std::lock_guard<std::mutex> lock(split_pause_mu_);
  if (!on) {
    split_pauses_.erase(region_id);
    return;
  }
  SplitPause sp;
  sp.active = true;
  sp.split_key = std::string(split_key);
  split_pauses_[region_id] = std::move(sp);
}

bool StorageNode::region_splitting(uint32_t region_id) const {
  std::lock_guard<std::mutex> lock(split_pause_mu_);
  auto it = split_pauses_.find(region_id);
  return it != split_pauses_.end() && it->second.active;
}

std::string StorageNode::admin_prepare_split(uint32_t left_region, std::string_view split_key) {
  Command cmd;
  cmd.op = CmdOp::kAdminPrepareSplit;
  cmd.key = std::string(split_key);
  std::string err, redir;
  if (!propose_encoded(left_region, encode_command(cmd), &err, &redir)) {
    return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
  }
  SKV_INFO << "PREPARE_SPLIT region=" << left_region << " pause=on (via raft)";
  return "OK PREPARE_SPLIT " + std::to_string(left_region);
}

std::vector<raft::Peer> StorageNode::resolve_region_peers(uint32_t region_id) {
  if (auto* g = multi_raft_->get(region_id)) {
    auto ps = g->peers();
    if (!ps.empty()) {
      for (auto& p : ps) {
        if (p.address.empty()) p.address = peer_address(p.id);
      }
      return ps;
    }
  }
  std::vector<std::pair<std::string, std::string>> cur;
  if (!cfg_.meta_addr.empty() && meta_region_peers(region_id, &cur, nullptr)) {
    std::vector<raft::Peer> out;
    out.reserve(cur.size());
    for (const auto& [id, addr] : cur) {
      out.push_back({id, addr, false});
    }
    if (!out.empty()) return out;
  }
  return bootstrap_peers_;
}

std::string StorageNode::seed_right_region(uint32_t left_region, uint32_t right_region) {
  auto peers = resolve_region_peers(left_region);
  if (peers.empty()) {
    peers.push_back({cfg_.node_id, cfg_.listen_addr, false});
  }
  for (const auto& p : peers) {
    if (!p.address.empty()) transport_.set_peer(p.id, p.address);
  }

  {
    std::lock_guard<std::mutex> lock(create_mu_);
    if (!multi_raft_->get(right_region)) {
      pending_peers_[right_region] = peers;
    }
  }
  auto local = admin_ensure_region(right_region, peers);
  if (local.rfind("OK", 0) != 0) {
    return "ERR local_ensure " + local;
  }

  std::ostringstream csv;
  for (size_t i = 0; i < peers.size(); ++i) {
    if (i) csv << ',';
    csv << peers[i].id << '@' << peers[i].address;
    if (peers[i].learner) csv << "*N";
  }
  const std::string ensure_req =
      "ADMIN_ENSURE_REGION " + std::to_string(right_region) + " " + csv.str();

  for (const auto& p : peers) {
    if (p.id == cfg_.node_id) continue;
    if (p.address.empty()) continue;
    try {
      auto resp = tcp_line(p.address, ensure_req, 4000);
      SKV_INFO << "seed_right ENSURE peer=" << p.id << " -> " << resp;
      if (resp.rfind("OK", 0) != 0) {
        return "ERR remote_ensure " + p.id + " " + resp;
      }
    } catch (const std::exception& e) {
      return std::string("ERR remote_ensure ") + p.id + " " + e.what();
    }
  }

  // Meta 路由应已含 right（auto_split 先 SPLIT_REGION）；缺失则告警
  if (!cfg_.meta_addr.empty()) {
    std::vector<std::pair<std::string, std::string>> meta_peers;
    if (!meta_region_peers(right_region, &meta_peers, nullptr)) {
      SKV_WARN << "seed_right: right=" << right_region
               << " missing in Meta route (call SPLIT_REGION before ADMIN_SPLIT)";
    }
  }
  return "OK SEED right=" + std::to_string(right_region) + " peers=" + std::to_string(peers.size());
}

std::string StorageNode::admin_split(uint32_t left_region, uint32_t right_region,
                                     std::string_view split_key) {
  std::lock_guard<std::mutex> lock(split_mu_);
  auto seed = seed_right_region(left_region, right_region);
  if (seed.rfind("OK", 0) != 0) {
    return seed;
  }
  Command cmd;
  cmd.op = CmdOp::kAdminSplit;
  cmd.key = std::string(split_key);
  cmd.value.assign(reinterpret_cast<const char*>(&right_region), sizeof(right_region));
  std::string err, redir;
  if (!propose_encoded(left_region, encode_command(cmd), &err, &redir)) {
    return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
  }
  return "OK SPLIT left_epoch=" + std::to_string(region_epoch(left_region)) +
         " right_epoch=" + std::to_string(region_epoch(right_region)) +
         " right_peers=" + std::to_string(resolve_region_peers(right_region).size());
}

std::string StorageNode::admin_merge(uint32_t left_region, uint32_t right_region) {
  std::lock_guard<std::mutex> lock(split_mu_);
  Command cmd;
  cmd.op = CmdOp::kAdminMerge;
  cmd.value.assign(reinterpret_cast<const char*>(&right_region), sizeof(right_region));
  std::string err, redir;
  if (!propose_encoded(left_region, encode_command(cmd), &err, &redir)) {
    return "ERR " + err + (redir.empty() ? "" : (" redirect=" + redir));
  }
  return "OK MERGE epoch=" + std::to_string(region_epoch(left_region));
}

bool StorageNode::meta_split(uint32_t left, const std::string& split_key, uint32_t* right_out) {
  if (cfg_.meta_addr.empty()) return false;
  std::ostringstream req;
  req << "SPLIT_REGION " << left << ' ' << to_hex(split_key);
  try {
    std::string addr = cfg_.meta_addr;
    for (int i = 0; i < 4; ++i) {
      auto resp = meta_exec_line(addr, req.str());
      if (resp.find("not_leader") != std::string::npos) {
        auto p = resp.find("redirect=");
        if (p == std::string::npos) return false;
        addr = resp.substr(p + 9);
        continue;
      }
      std::istringstream iss(resp);
      std::string ok, sp;
      uint32_t l = 0, r = 0;
      if (!(iss >> ok >> sp >> l >> r) || ok != "OK") return false;
      *right_out = r;
      return true;
    }
  } catch (const std::exception& e) {
    SKV_WARN << "meta_split failed: " << e.what();
  }
  return false;
}

void StorageNode::broadcast_prepare_split(uint32_t left, const std::string& split_key) {
  // S1：Prepare 已入 Raft，Leader propose 一次即可全员暂停；不再 TCP 广播。
  auto resp = admin_prepare_split(left, split_key);
  if (resp.rfind("OK", 0) != 0) {
    SKV_WARN << "PREPARE_SPLIT failed: " << resp;
  }
}

void StorageNode::broadcast_admin_split(uint32_t left, uint32_t right,
                                        const std::string& split_key) {
  // S2：Prepare(Raft) → seed 右 Region 成员 → Split(Raft migrate)。
  // 不再向 bootstrap_peers TCP 广播 ADMIN_SPLIT（避免非 Leader 重复 propose）。
  broadcast_prepare_split(left, split_key);
  auto resp = admin_split(left, right, split_key);
  if (resp.rfind("OK", 0) != 0) {
    SKV_WARN << "ADMIN_SPLIT failed left=" << left << " right=" << right << " " << resp;
  } else {
    SKV_INFO << "ADMIN_SPLIT done " << resp;
  }
}

uint64_t StorageNode::fetch_gc_safe_ts() {
  if (cfg_.meta_addr.empty()) return 0;
  try {
    auto resp = meta_rpc("GET_SAFE_POINT");
    // OK SAFE <gc_safe_ts> <next_ts> <min_start>
    std::istringstream iss(resp);
    std::string ok, tag;
    uint64_t safe = 0;
    if (!(iss >> ok >> tag >> safe) || ok != "OK" || tag != "SAFE") return 0;
    return safe;
  } catch (const std::exception& e) {
    SKV_DEBUG << "fetch_gc_safe_ts: " << e.what();
    return 0;
  }
}

void StorageNode::maybe_txn_gc(uint32_t region_id) {
  auto* g = multi_raft_->get(region_id);
  if (!g || !g->is_leader()) return;
  if (region_splitting(region_id)) return;
  auto* eng = engines_->get(region_id);
  if (!eng) return;
  MvccStore mvcc(*eng);
  StoreFacade facade(*this);
  // 1) 过期锁 → Raft rollback
  for (const auto& user : mvcc.list_expired_locks()) {
    auto lk = mvcc.get_lock(user);
    if (!lk) continue;
    auto r = facade.rollback(region_id, user, lk->start_ts, 0);
    if (!r.ok) {
      SKV_DEBUG << "txn_gc rollback failed key=" << user << " err=" << r.error;
    }
  }
  // 2) MVCC GC 经 Raft；水位来自 Meta TSO safe point（0 则跳过）
  const uint64_t safe = fetch_gc_safe_ts();
  if (safe == 0) return;
  Command gc;
  gc.op = CmdOp::kGcBelow;
  gc.value.assign(reinterpret_cast<const char*>(&safe), sizeof(safe));
  facade.propose(region_id, gc);
}

void StorageNode::maybe_auto_split(uint32_t region_id) {
  if (auto_split_keys_ == 0 && auto_split_bytes_ == 0) return;
  auto* g = multi_raft_->get(region_id);
  if (!g || !g->is_leader()) return;
  auto* eng = engines_->get(region_id);
  if (!eng) return;
  const bool over_keys = auto_split_keys_ > 0 && eng->approx_key_count() >= auto_split_keys_;
  const bool over_bytes = auto_split_bytes_ > 0 && eng->approx_bytes() >= auto_split_bytes_;
  if (!over_keys && !over_bytes) return;

  std::string split_key = eng->pick_split_key();
  if (split_key.empty()) return;

  uint32_t right = 0;
  {
    std::lock_guard<std::mutex> lock(split_mu_);
    const bool still_over =
        (auto_split_keys_ > 0 && eng->approx_key_count() >= auto_split_keys_) ||
        (auto_split_bytes_ > 0 && eng->approx_bytes() >= auto_split_bytes_);
    if (!still_over) return;
    if (!meta_split(region_id, split_key, &right)) {
      SKV_WARN << "auto_split meta failed region=" << region_id;
      return;
    }
  }
  SKV_INFO << "auto_split region=" << region_id << " -> " << right
           << " keys=" << eng->approx_key_count() << " bytes=" << eng->approx_bytes();
  broadcast_admin_split(region_id, right, split_key);
}

bool StorageNode::meta_suggest_place(std::vector<std::string>* out,
                                     const std::vector<std::string>& avoid) {
  if (cfg_.meta_addr.empty() || !out) return false;
  std::ostringstream req;
  req << "SUGGEST_PLACE";
  if (!avoid.empty()) {
    req << " avoid=";
    for (size_t i = 0; i < avoid.size(); ++i) {
      if (i) req << ',';
      req << avoid[i];
    }
  }
  try {
    std::string addr = cfg_.meta_addr;
    for (int i = 0; i < 4; ++i) {
      auto resp = meta_exec_line(addr, req.str());
      if (resp.find("not_leader") != std::string::npos) {
        auto p = resp.find("redirect=");
        if (p == std::string::npos) return false;
        addr = resp.substr(p + 9);
        continue;
      }
      std::istringstream iss(resp);
      std::string ok, tag;
      size_t n = 0;
      if (!(iss >> ok >> tag >> n) || ok != "OK") return false;
      out->clear();
      for (size_t k = 0; k < n; ++k) {
        std::string id;
        if (!(iss >> id)) return false;
        out->push_back(id);
      }
      return !out->empty();
    }
  } catch (const std::exception& e) {
    SKV_WARN << "meta_suggest_place: " << e.what();
  }
  return false;
}

bool StorageNode::meta_find_merge_right(uint32_t left, uint32_t* right_out) {
  if (cfg_.meta_addr.empty() || !right_out) return false;
  try {
    std::string addr = cfg_.meta_addr;
    for (int i = 0; i < 4; ++i) {
      auto resp = meta_exec_line(addr, "FIND_MERGE_RIGHT " + std::to_string(left));
      if (resp.find("not_leader") != std::string::npos) {
        auto p = resp.find("redirect=");
        if (p == std::string::npos) return false;
        addr = resp.substr(p + 9);
        continue;
      }
      std::istringstream iss(resp);
      std::string ok, tag;
      uint32_t l = 0, r = 0;
      if (!(iss >> ok >> tag >> l >> r) || ok != "OK") return false;
      *right_out = r;
      return true;
    }
  } catch (...) {
  }
  return false;
}

bool StorageNode::meta_merge(uint32_t left, uint32_t right) {
  if (cfg_.meta_addr.empty()) return false;
  try {
    std::string addr = cfg_.meta_addr;
    for (int i = 0; i < 4; ++i) {
      auto resp =
          meta_exec_line(addr, "MERGE_REGIONS " + std::to_string(left) + " " + std::to_string(right));
      if (resp.find("not_leader") != std::string::npos) {
        auto p = resp.find("redirect=");
        if (p == std::string::npos) return false;
        addr = resp.substr(p + 9);
        continue;
      }
      return resp.rfind("OK MERGE", 0) == 0;
    }
  } catch (...) {
  }
  return false;
}

void StorageNode::maybe_auto_merge(uint32_t region_id) {
  if (auto_merge_keys_ == 0) return;
  auto* g = multi_raft_->get(region_id);
  if (!g || !g->is_leader()) return;
  auto* eng = engines_->get(region_id);
  if (!eng || eng->approx_key_count() == 0 || eng->approx_key_count() >= auto_merge_keys_) {
    return;
  }
  uint32_t right = 0;
  if (!meta_find_merge_right(region_id, &right)) return;
  auto* reng = engines_->get(right);
  if (reng && reng->approx_key_count() >= auto_merge_keys_) return;

  std::lock_guard<std::mutex> lock(split_mu_);
  eng = engines_->get(region_id);
  if (!eng || eng->approx_key_count() >= auto_merge_keys_) return;
  // 先搬数据再改 Meta
  admin_merge(region_id, right);
  for (const auto& p : bootstrap_peers_) {
    if (p.id == cfg_.node_id) continue;
    try {
      tcp_line(p.address, "ADMIN_MERGE " + std::to_string(region_id) + " " + std::to_string(right));
    } catch (...) {
    }
  }
  if (!meta_merge(region_id, right)) {
    SKV_WARN << "auto_merge meta failed left=" << region_id << " right=" << right;
    return;
  }
  admin_destroy_region(right);
  SKV_INFO << "auto_merge left=" << region_id << " right=" << right;
}

std::string StorageNode::meta_rpc(const std::string& req) {
  if (cfg_.meta_addr.empty()) throw std::runtime_error("no meta_addr");
  meta_out_.fetch_add(1, std::memory_order_relaxed);
  std::string addr = cfg_.meta_addr;
  if (addr.find("0.0.0.0") == 0) addr.replace(0, 7, "127.0.0.1");
  // 无 redirect 时：本机开发轮询 8500–8502；K8s/DNS 勿回落 127.0.0.1（会心跳失败→Offline）
  static const char* kMetaFallbacks[] = {"127.0.0.1:8500", "127.0.0.1:8501", "127.0.0.1:8502"};
  const bool local_meta =
      addr.rfind("127.0.0.1", 0) == 0 || addr.rfind("localhost", 0) == 0;
  int fb = 0;
  for (int i = 0; i < 12; ++i) {
    meta_out_peers_.add(addr);
    // Meta 同步写（首注 REGISTER / TSO）在负载下可能 >3s；与 replicate 等待对齐。
    auto resp = meta_exec_line(addr, req, 8000);
    if (resp.find("not_leader") != std::string::npos) {
      auto p = resp.find("redirect=");
      if (p != std::string::npos) {
        std::string next = resp.substr(p + 9);
        const auto sp = next.find_first_of(" \t\r\n");
        if (sp != std::string::npos) next = next.substr(0, sp);
        if (!next.empty() && next != addr) {
          addr = std::move(next);
          continue;
        }
      }
      if (local_meta) {
        addr = kMetaFallbacks[static_cast<size_t>(fb++ % 3)];
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }
    if (resp.rfind("ERR", 0) == 0 && resp.find("not_leader") == std::string::npos) {
      // 业务错误原样返回（unknown_node / heartbeat_stale 等）
      return resp;
    }
    return resp;
  }
  throw std::runtime_error("meta_rpc failed");
}

bool StorageNode::meta_list_storage(std::unordered_map<std::string, std::string>* id_to_addr) {
  if (!id_to_addr) return false;
  try {
    auto resp = meta_rpc("LIST_NODES storage");
    std::istringstream iss(resp);
    std::string ok, tag;
    size_t n = 0;
    if (!(iss >> ok >> tag >> n) || ok != "OK") return false;
    id_to_addr->clear();
    for (size_t i = 0; i < n; ++i) {
      std::string tok;
      if (!(iss >> tok)) return false;
      auto at = tok.find('@');
      if (at == std::string::npos) continue;
      auto r1 = tok.rfind(':');
      if (r1 == std::string::npos || r1 == 0) continue;
      auto r2 = tok.rfind(':', r1 - 1);
      if (r2 == std::string::npos || r2 <= at) continue;
      (*id_to_addr)[tok.substr(0, at)] = tok.substr(at + 1, r2 - at - 1);
    }
    return !id_to_addr->empty();
  } catch (const std::exception& e) {
    SKV_WARN << "meta_list_storage: " << e.what();
    return false;
  }
}

bool StorageNode::meta_place(uint32_t region_id, const std::vector<std::string>& peers) {
  std::ostringstream req;
  req << "PLACE_REGION " << region_id << ' ';
  for (size_t i = 0; i < peers.size(); ++i) {
    if (i) req << ',';
    req << peers[i];
  }
  try {
    auto resp = meta_rpc(req.str());
    return resp.rfind("OK PLACE", 0) == 0;
  } catch (const std::exception& e) {
    SKV_WARN << "meta_place: " << e.what();
    return false;
  }
}

bool StorageNode::meta_region_peers(uint32_t region_id,
                                    std::vector<std::pair<std::string, std::string>>* out,
                                    std::string* leader_id) {
  if (!out) return false;
  try {
    auto resp = meta_rpc("GET_ROUTE");
    std::istringstream iss(resp);
    std::string ok, tag;
    uint64_t ver = 0;
    size_t n1 = 0, n2 = 0;
    if (!(iss >> ok >> tag >> ver >> n1 >> n2) || ok != "OK") return false;
    std::string tok;
    while (iss >> tok) {
      auto c1 = tok.find(':');
      if (c1 == std::string::npos) continue;
      if (static_cast<uint32_t>(std::stoul(tok.substr(0, c1))) != region_id) continue;
      auto c2 = tok.find(':', c1 + 1);
      auto c3 = tok.find(':', c2 == std::string::npos ? tok.size() : c2 + 1);
      if (c2 == std::string::npos || c3 == std::string::npos) continue;
      std::string rest = tok.substr(c3 + 1);
      auto comma = rest.find(',');
      if (comma == std::string::npos) return false;
      out->clear();
      if (leader_id) leader_id->clear();
      std::stringstream ss(rest.substr(comma + 1));
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        const bool is_leader = item.find("*L") != std::string::npos;
        while (!item.empty() && (item.back() == 'L' || item.back() == 'N') &&
               item.size() >= 2 && item[item.size() - 2] == '*') {
          item = item.substr(0, item.size() - 2);
        }
        auto at = item.find('@');
        if (at == std::string::npos) continue;
        out->emplace_back(item.substr(0, at), item.substr(at + 1));
        if (is_leader && leader_id) *leader_id = out->back().first;
      }
      return !out->empty();
    }
  } catch (const std::exception& e) {
    SKV_WARN << "meta_region_peers: " << e.what();
  }
  return false;
}

std::string StorageNode::admin_rpc(const std::string& req,
                                   const std::vector<std::string>& seed_addrs) {
  std::vector<std::string> addrs = seed_addrs;
  for (const auto& p : bootstrap_peers_) {
    if (!p.address.empty()) addrs.push_back(p.address);
  }
  // dedupe
  std::sort(addrs.begin(), addrs.end());
  addrs.erase(std::unique(addrs.begin(), addrs.end()), addrs.end());
  if (addrs.empty()) throw std::runtime_error("no seed addrs");

  std::string hint;
  std::string last_err;
  for (int attempt = 0; attempt < 32; ++attempt) {
    if (!running_) throw std::runtime_error("stopped");
    std::string addr = hint.empty() ? addrs[static_cast<size_t>(attempt) % addrs.size()] : hint;
    try {
      auto resp = tcp_line(addr, req, 8000);
      if (resp.find("not_leader") != std::string::npos) {
        last_err = resp;
        auto p = resp.find("redirect=");
        if (p != std::string::npos) {
          const std::string r = resp.substr(p + 9);
          hint = (r != addr) ? r : "";
        } else {
          hint.clear();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }
      if (resp.find("no_region") != std::string::npos) {
        last_err = resp;
        hint.clear();
        continue;
      }
      return resp;
    } catch (const std::exception& e) {
      last_err = e.what();
      hint.clear();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  throw std::runtime_error("admin_rpc failed: " + last_err);
}

bool StorageNode::wait_peer_caught_up(uint32_t region_id, const std::string& peer_id,
                                      const std::vector<std::string>& seed_addrs) {
  for (int i = 0; i < 400; ++i) {
    if (!running_) return false;
    try {
      auto r = admin_rpc("ADMIN_PEER_PROGRESS " + std::to_string(region_id) + " " + peer_id,
                         seed_addrs);
      std::istringstream iss(r);
      std::string ok, tag;
      uint64_t match = 0, commit = 0, last = 0;
      int caught = 0;
      if (iss >> ok >> tag >> match >> commit >> last >> caught && ok == "OK" && caught == 1) {
        return true;
      }
      SKV_INFO << "place progress region=" << region_id << " peer=" << peer_id << " " << r;
    } catch (const std::exception& e) {
      SKV_WARN << "place progress: " << e.what();
      if (std::string(e.what()).find("not_leader") != std::string::npos) {
        for (const auto& a : seed_addrs) {
          if (a.empty()) continue;
          if (maybe_campaign(region_id, a)) break;
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

bool StorageNode::place_region(uint32_t region_id, const std::vector<std::string>& desired_peers) {
  if (desired_peers.empty()) return false;

  std::unordered_map<std::string, std::string> nodes;
  if (!meta_list_storage(&nodes)) {
    SKV_WARN << "place: LIST_NODES failed";
    return false;
  }
  for (const auto& id : desired_peers) {
    if (!nodes.count(id)) {
      SKV_WARN << "place: unknown node " << id;
      return false;
    }
  }

  std::vector<std::pair<std::string, std::string>> current;
  std::string leader_id;
  if (!meta_region_peers(region_id, &current, &leader_id)) {
    SKV_WARN << "place: region " << region_id << " not in route";
    return false;
  }

  auto rebuild_seeds = [&](const std::string& exclude_addr = {}) {
    std::vector<std::string> seeds;
    auto push = [&](const std::string& a) {
      if (a.empty() || a == exclude_addr) return;
      for (const auto& x : seeds)
        if (x == a) return;
      seeds.push_back(a);
    };
    // 优先目标集（仍存活的 Active），避免 redirect 到正在踢出的 Store
    for (const auto& id : desired_peers) push(nodes[id]);
    for (const auto& [_, a] : current) push(a);
    return seeds;
  };

  const std::vector<std::string> original_ids = [&] {
    std::vector<std::string> ids;
    for (const auto& [nid, _] : current) ids.push_back(nid);
    return ids;
  }();
  auto was_original = [&](const std::string& id) {
    for (const auto& o : original_ids)
      if (o == id) return true;
    return false;
  };

  auto has = [&](const std::string& id) {
    for (const auto& [nid, _] : current)
      if (nid == id) return true;
    return false;
  };

  // 1=joint，0=已确认非 joint，-1=不可达（不可当作已退出）
  auto joint_status = [&](const std::string& exclude_addr = {}) -> int {
    bool saw_ok = false;
    for (const auto& a : rebuild_seeds(exclude_addr)) {
      try {
        auto st = tcp_line(a, "ADMIN_REGION_STATS " + std::to_string(region_id), 1500);
        if (st.rfind("OK", 0) != 0) continue;
        saw_ok = true;
        if (st.find("joint=1") != std::string::npos) return 1;
      } catch (...) {
      }
    }
    return saw_ok ? 0 : -1;
  };

  auto wait_promoted = [&](const std::string& peer_id) -> bool {
    for (int i = 0; i < 150 && running_; ++i) {
      const int js = joint_status();
      try {
        auto r = admin_rpc("ADMIN_PEER_PROGRESS " + std::to_string(region_id) + " " + peer_id,
                           rebuild_seeds());
        std::istringstream iss(r);
        std::string ok, tag;
        uint64_t match = 0, commit = 0, last = 0;
        int caught = 0, learner = 1;
        if (iss >> ok >> tag >> match >> commit >> last >> caught >> learner && ok == "OK" &&
            js == 0 && learner == 0) {
          return true;
        }
        SKV_INFO << "place promote-wait peer=" << peer_id << " " << r << " joint_status=" << js;
      } catch (const std::exception& e) {
        SKV_WARN << "place promote-wait: " << e.what();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
  };

  auto admin_retry = [&](const std::string& req, const std::string& tag,
                         const std::string& exclude_addr = {}) -> bool {
    for (int attempt = 0; attempt < 40; ++attempt) {
      if (!running_) return false;
      std::string r;
      try {
        r = admin_rpc(req, rebuild_seeds(exclude_addr));
      } catch (const std::exception& e) {
        r = std::string("ERR ") + e.what();
      }
      SKV_INFO << "place " << tag << " -> " << r;
      if (r.rfind("OK", 0) == 0) return true;
      if (r.find("busy") != std::string::npos || r.find("not_leader") != std::string::npos ||
          r.find("joint") != std::string::npos || r.find("no_region") != std::string::npos ||
          r.find("remove_failed") != std::string::npos ||
          r.find("promote_failed") != std::string::npos ||
          r.find("add_learner_failed") != std::string::npos) {
        // 无主，或 redirect 多轮仍失败：催目标集竞选（僵死 *L 常见）
        if (r.find("not_leader") != std::string::npos && !desired_peers.empty() &&
            (r.find("redirect=") == std::string::npos || attempt >= 5)) {
          maybe_campaign(region_id, nodes[desired_peers[0]]);
          for (const auto& id : desired_peers) {
            if (id == desired_peers[0]) continue;
            maybe_campaign(region_id, nodes[id], std::chrono::milliseconds(400));
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }
      return false;
    }
    return false;
  };

  // 1) 加缺失副本
  for (const auto& id : desired_peers) {
    if (!running_) return false;
    if (has(id)) continue;
    const std::string& addr = nodes[id];
    try {
      auto r = admin_rpc("ADMIN_PEER_PROGRESS " + std::to_string(region_id) + " " + id,
                         rebuild_seeds());
      std::istringstream iss(r);
      std::string ok, tag;
      uint64_t match = 0, commit = 0, last = 0;
      int caught = 0, learner = 1;
      if (iss >> ok >> tag >> match >> commit >> last >> caught >> learner && ok == "OK" &&
          caught == 1 && learner == 0) {
        SKV_INFO << "place skip add " << id << " (already voter caught-up)";
        current.emplace_back(id, addr);
        continue;
      }
    } catch (...) {
    }
    std::ostringstream peers_csv;
    for (size_t i = 0; i < current.size(); ++i) {
      if (i) peers_csv << ',';
      peers_csv << current[i].first << '@' << current[i].second;
    }
    peers_csv << ',' << id << '@' << addr << "*N";
    try {
      auto er = tcp_line(addr,
                         "ADMIN_ENSURE_REGION " + std::to_string(region_id) + " " + peers_csv.str(),
                         4000);
      SKV_INFO << "place ensure " << id << " -> " << er;
    } catch (const std::exception& e) {
      SKV_WARN << "place ensure " << id << " FAIL " << e.what();
      return false;
    }
    if (!admin_retry("ADMIN_ADD_LEARNER " + std::to_string(region_id) + " " + id + "@" + addr,
                     "add_learner " + id)) {
      return false;
    }
    if (!wait_peer_caught_up(region_id, id, rebuild_seeds())) {
      SKV_WARN << "place catch-up timeout peer=" << id;
      return false;
    }
    if (!admin_retry("ADMIN_PROMOTE " + std::to_string(region_id) + " " + id, "promote " + id)) {
      return false;
    }
    if (!wait_promoted(id)) {
      SKV_WARN << "place promote settle timeout peer=" << id;
      return false;
    }
    current.emplace_back(id, addr);
  }

  // 2) 踢人之前：若还有 victim，强制把 Leader 转到目标集中的原 voter（勿转刚 promote 节点）。
  std::vector<std::pair<std::string, std::string>> victims;
  for (const auto& [nid, naddr] : current) {
    bool keep = false;
    for (const auto& d : desired_peers) {
      if (d == nid) {
        keep = true;
        break;
      }
    }
    if (!keep) victims.emplace_back(nid, naddr);
  }
  if (!victims.empty() && !desired_peers.empty()) {
    std::string target;
    for (const auto& d : desired_peers) {
      if (was_original(d)) {
        target = d;
        break;
      }
    }
    if (target.empty()) target = desired_peers[0];
    std::string lid = leader_id;
    std::string fresh_leader;
    std::vector<std::pair<std::string, std::string>> rp;
    if (meta_region_peers(region_id, &rp, &fresh_leader) && !fresh_leader.empty()) {
      lid = fresh_leader;
    }
    bool kept = false;
    for (const auto& d : desired_peers) {
      if (d == lid) kept = true;
    }
    if (!kept) {
      std::string excl;
      for (const auto& [_, a] : victims) {
        if (!excl.empty()) break;
        excl = a;
      }
      admin_retry("ADMIN_TRANSFER_LEADER " + std::to_string(region_id) + " " + target,
                  "transfer->" + target, excl);
    }
    maybe_campaign(region_id, nodes[target], std::chrono::milliseconds(200));
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
  }

  // 3) 踢多余 + DESTROY（DESTROY 前须等 LeaveJoint，否则旧多数派缺票永久无主）
  for (const auto& [nid, naddr] : victims) {
    if (!running_) return false;
    if (!admin_retry("ADMIN_REMOVE_PEER " + std::to_string(region_id) + " " + nid, "remove " + nid,
                     naddr)) {
      return false;
    }
    bool left_joint = false;
    for (int w = 0; w < 200 && running_; ++w) {
      if (joint_status(naddr) == 0) {
        left_joint = true;
        break;
      }
      if (w > 0 && w % 15 == 0) {
        for (const auto& d : desired_peers) {
          auto it = nodes.find(d);
          if (it == nodes.end()) continue;
          maybe_campaign(region_id, it->second, std::chrono::milliseconds(400));
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!left_joint) {
      SKV_WARN << "place refuse DESTROY " << nid << ": still in joint or unreachable";
      return false;
    }
    try {
      auto dr = tcp_line(naddr, "ADMIN_DESTROY_REGION " + std::to_string(region_id), 3000);
      SKV_INFO << "place destroy " << nid << " -> " << dr;
    } catch (const std::exception& e) {
      SKV_WARN << "place destroy " << nid << " FAIL " << e.what();
    }
    current.erase(std::remove_if(current.begin(), current.end(),
                                 [&](const auto& p) { return p.first == nid; }),
                  current.end());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  // 4) Meta 发布
  if (!meta_place(region_id, desired_peers)) {
    SKV_WARN << "place meta PLACE_REGION failed region=" << region_id;
    return false;
  }
  SKV_INFO << "place done region=" << region_id << " replicas=" << desired_peers.size();
  return true;
}

std::string StorageNode::admin_place_region(uint32_t region_id,
                                            const std::vector<std::string>& desired) {
  if (place_region(region_id, desired)) {
    bump_region_epoch(region_id);
    return "OK PLACE " + std::to_string(region_id) + " " + std::to_string(desired.size()) +
           " epoch=" + std::to_string(region_epoch(region_id));
  }
  return "ERR place_failed";
}

void StorageNode::start() {
  if (running_.exchange(true)) return;

  if (!cfg_.raft_peers.empty()) {
    for (const auto& p : cfg_.raft_peers) {
      bootstrap_peers_.push_back({p.node_id, p.address, false});
      transport_.set_peer(p.node_id, p.address);
    }
  } else {
    bootstrap_peers_.push_back({cfg_.node_id, cfg_.listen_addr, false});
    transport_.set_peer(cfg_.node_id, cfg_.listen_addr);
  }

  transport_.start();
  // 生产路径固定 raftstore Ready 外提（无节点内 IO 双轨）。
  multi_raft_ = std::make_unique<raft::MultiRaft>([this](uint32_t shard_id) {
    engines_->get_or_create(shard_id);
    raft::RaftNode::Options opts;
    opts.shard_id = shard_id;
    opts.self_id = cfg_.node_id;
    opts.external_io = true;
    {
      std::lock_guard<std::mutex> lock(create_mu_);
      auto it = pending_peers_.find(shard_id);
      if (it != pending_peers_.end()) {
        opts.peers = it->second;
        pending_peers_.erase(it);
      } else {
        const bool bootstrapped =
            std::find(cfg_.bootstrap_regions.begin(), cfg_.bootstrap_regions.end(), shard_id) !=
            cfg_.bootstrap_regions.end();
        if (!bootstrapped) {
          // Joining / 非 bootstrap Region：禁止用单机 raft_peers 建成可竞选 voter。
          // 误触 get_or_create 时以 learner 占位，等 ADMIN_ENSURE 再重建。
          opts.peers = {{cfg_.node_id, advertise_address(), true}};
          SKV_WARN << "raft factory shard=" << shard_id
                   << " without ENSURE → learner placeholder (avoid orphan leader)";
        } else {
          opts.peers = bootstrap_peers_;
        }
      }
    }
    opts.election_timeout_ms = cfg_.raft_election_timeout_ms;
    opts.heartbeat_ms = cfg_.raft_heartbeat_ms;
    opts.raft_engine = raft_engine_;
    opts.raft_group_id = shard_id;
    opts.persist_sync = raft::parse_sync_mode(cfg_.persist_sync);
    opts.compact_threshold = 128;
    // 兼容旧 per-region 文本目录 → 导入 RocksRaftEngine
    if (raft_engine_ && !raft_engine_->get_raft_state(shard_id)) {
      const auto legacy =
          (std::filesystem::path(cfg_.data_dir) / ("raft_region_" + std::to_string(shard_id)))
              .string();
      raft::PersistedHardState hs;
      std::vector<raft::LogEntry> ents;
      raft::RaftPersist old(legacy, raft::SyncMode::kNone);
      if (old.load(&hs, &ents)) {
        raft::RaftLog log;
        log.load(hs.first_index, hs.snapshot_term, ents);
        raft::save_group_to_engine(*raft_engine_, shard_id, hs, log,
                                   opts.persist_sync != raft::SyncMode::kNone);
        SKV_INFO << "migrated legacy raft_region_" << shard_id << " -> RocksRaftEngine";
      }
    }
    opts.on_peer_addr = [this](const std::string& id, const std::string& addr) {
      transport_.set_peer(id, addr);
    };

    auto snap_provider = [this, shard_id](uint64_t* last_idx, uint64_t* last_term) {
      auto& eng = engines_->get_or_create(shard_id);
      (void)last_idx;
      (void)last_term;
      const auto range = region_range(shard_id);
      // 共享引擎时只导出本 Region 区间；附带 SKVR1 区间头供对端恢复
      return wrap_snap_range(range, eng.create_snapshot(range));
    };
    auto snap_applier = [this, shard_id](uint64_t last_idx, uint64_t /*last_term*/,
                                         std::string_view data) {
      auto& eng = engines_->get_or_create(shard_id);
      opskv::KeyRange range;
      std::string_view kv = data;
      if (!peel_snap_range(data, &range, &kv)) {
        range = region_range(shard_id);
        kv = data;
      } else if (!range.start_key.empty() || !range.end_key.empty()) {
        set_region_range(shard_id, range);
      } else {
        range = region_range(shard_id);
      }
      eng.apply_snapshot(kv, range);
      SKV_INFO << "region=" << shard_id << " kv snapshot applied last_idx=" << last_idx
               << " bytes=" << kv.size() << " range_bounded="
               << ((!range.start_key.empty() || !range.end_key.empty()) ? 1 : 0);
      return true;
    };

    // IO 经 Store Ready：persist_fn / transport_fn / apply_fn（不在 RaftNode 内回调）
    auto node = std::make_shared<raft::RaftNode>(std::move(opts), nullptr, nullptr,
                                                 std::move(snap_provider),
                                                 std::move(snap_applier));
    transport_.register_node(shard_id, cfg_.node_id, node);
    return node;
  });

  // bootstrap_regions：首批 Region（通常仅 0）；空则等 PLACE/ENSURE
  for (uint32_t rid : cfg_.bootstrap_regions) {
    set_region_range(rid, opskv::KeyRange{});  // 全 key 空间
    auto& g = multi_raft_->get_or_create(rid);
    g.recover_apply();
    if (uint64_t e = load_epoch_from_engine(rid); e != 0) {
      std::lock_guard<std::mutex> lock(epoch_mu_);
      region_epochs_[rid] = e;
    }
    multi_raft_->update_peer_meta(rid, region_range(rid), region_epoch(rid));
  }
  multi_raft_->start();  // BatchSystem + SnapWorker
  // Ready 外提：启动后立刻推进 recover 产生的 committed Ready
  for (uint32_t rid : cfg_.bootstrap_regions) {
    multi_raft_->store().mark_hot(rid);
    multi_raft_->store().schedule_tick();
  }
  raftkv_ = std::make_unique<RaftKV>(&multi_raft_->store());
  raftkv_->set_apply_wait([this](uint32_t rid, uint64_t index, uint32_t timeout_ms) {
    const int steps = static_cast<int>(timeout_ms / 5) + 1;
    for (int i = 0; i < steps; ++i) {
      if (auto* n = multi_raft_->get(rid)) {
        if (n->soft_state().last_applied >= index) return true;
      }
      if (raftkv_) raftkv_->flush_ready(rid);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
  });
  scheduler_ = std::make_unique<Scheduler>();
  multi_raft_->store().set_apply_fn(
      [this](uint32_t rid, uint64_t index, const std::string& data) { on_apply(rid, index, data); });
  multi_raft_->store().set_persist_fn([this](uint32_t rid, const raft::Ready& rd) {
    if (auto* n = multi_raft_->get(rid)) n->persist_ready(rd);
  });
  multi_raft_->store().set_transport_fn(
      [this](uint32_t rid, const std::string& to, const std::string& type,
             const std::string& payload) {
        auto send = transport_.bind(rid);
        if (type == "InstallSnapshotData") {
          // SnapWorker：last_idx|last_term|hex → InstallSnapshot（term|idx|term|hex）
          const auto p1 = payload.find('|');
          const auto p2 = (p1 == std::string::npos) ? std::string::npos : payload.find('|', p1 + 1);
          if (p1 == std::string::npos || p2 == std::string::npos) {
            SKV_WARN << "InstallSnapshotData bad payload region=" << rid;
            return;
          }
          auto* n = multi_raft_->get(rid);
          const uint64_t raft_term = n ? n->soft_state().term : 0;
          std::ostringstream oss;
          oss << raft_term << '|' << payload.substr(0, p1) << '|'
              << payload.substr(p1 + 1, p2 - p1 - 1) << '|' << payload.substr(p2 + 1);
          send(to, "InstallSnapshot", oss.str());
          return;
        }
        send(to, type, payload);
      });
  multi_raft_->store().set_snap_provider(
      [this](uint32_t rid, uint64_t* li, uint64_t* lt) {
        (void)li;
        (void)lt;
        const auto range = region_range(rid);
        return wrap_snap_range(range, engines_->get_or_create(rid).create_snapshot(range));
      });
  tick_thread_ = std::thread([this] { tick_loop(); });
  hb_thread_ = std::thread([this] { register_and_heartbeat(); });
  SKV_INFO << "storage node started id=" << cfg_.node_id
           << " bootstrap_regions=" << cfg_.bootstrap_regions.size()
           << " raft_peers=" << bootstrap_peers_.size() << " split_keys=" << auto_split_keys_
           << " split_bytes=" << auto_split_bytes_ << " merge_keys=" << auto_merge_keys_
           << " persist_sync=" << raft::sync_mode_name(raft::parse_sync_mode(cfg_.persist_sync))
           << " raftstore=BatchSystem+Ready single_store_kv=1 scheduler=1";
}

void StorageNode::stop() {
  if (!running_.exchange(false)) return;
  if (tick_thread_.joinable()) tick_thread_.join();
  if (hb_thread_.joinable()) hb_thread_.join();
  if (multi_raft_) multi_raft_->stop();
  transport_.stop();
}

void StorageNode::tick_loop() {
  int split_ticks = 0;
  int gc_ticks = 0;
  const bool txn_gc = [] {
    if (const char* e = std::getenv(proto::env::kTxnGc)) {
      return std::string(e) != "0" && std::string(e) != "false";
    }
    return true;  // 默认开：过期锁 + 版本 GC
  }();
  while (running_) {
    multi_raft_->tick_all();
    // 周期性检查自动分裂 / 合并（Leader + 阈值）
    if (++split_ticks >= 20) {  // ~1s
      split_ticks = 0;
      auto leaders = multi_raft_->leader_shards();
      for (uint32_t rid : leaders) {
        maybe_auto_split(rid);
        maybe_auto_merge(rid);
      }
    }
    // 事务 GC 较重，约 10s 一次
    if (txn_gc && ++gc_ticks >= 200) {
      gc_ticks = 0;
      for (uint32_t rid : multi_raft_->leader_shards()) {
        maybe_txn_gc(rid);
      }
      // T2：Storage 上报本机 Leader Region 锁的最小 start_ts
      if (!cfg_.meta_addr.empty()) {
        uint64_t min_ts = std::numeric_limits<uint64_t>::max();
        for (uint32_t rid : multi_raft_->leader_shards()) {
          auto* eng = engines_->get(rid);
          if (!eng) continue;
          const uint64_t m = MvccStore(*eng).min_lock_start_ts();
          if (m < min_ts) min_ts = m;
        }
        try {
          meta_rpc("REPORT_MIN_START_TS " + std::to_string(min_ts) + " " + cfg_.node_id);
        } catch (const std::exception& e) {
          SKV_DEBUG << "report min_start: " << e.what();
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

CoprocessorResponse StorageNode::coprocessor_execute(uint32_t region_id,
                                                     const CoprocessorRequest& req) {
  auto* eng = engines_->get(region_id);
  if (!eng) eng = &engines_->get_or_create(region_id);
  Coprocessor cop(eng);
  return cop.execute(req, region_range(region_id));
}

void StorageNode::register_and_heartbeat() {
  // H1：向 Meta 注册并周期上报本机 Leader Region（驱动路由 *L 与存活检测）
  // PD 语义：附带 region 数 / approx keys（供 RegionScheduler 主动均衡）。
  if (cfg_.meta_addr.empty()) return;
  int hb_sec = 2;
  if (const char* e = std::getenv(proto::env::kStoreHbSec)) {
    try {
      int v = std::stoi(e);
      if (v >= 1 && v <= 60) hb_sec = v;
    } catch (...) {
    }
  }
  bool registered = false;
  while (running_) {
    try {
      if (!registered) {
        auto resp = meta_rpc("REGISTER storage " + cfg_.node_id + " " + advertise_address());
        if (resp.rfind("OK REGISTER", 0) == 0) {
          registered = true;
          SKV_INFO << "meta REGISTER ok id=" << cfg_.node_id;
        } else {
          SKV_WARN << "meta REGISTER: " << resp;
        }
      }
      if (registered) {
        auto leaders = multi_raft_->leader_shards();
        size_t approx_keys = 0;
        if (auto* eng = engines_->get(0)) approx_keys = eng->approx_key_count();
        else if (!leaders.empty()) {
          if (auto* eng = engines_->get(leaders.front())) approx_keys = eng->approx_key_count();
        }
        std::ostringstream req;
        // PD 语义：leaders + region 数 + approx keys（RegionScheduler 可作负载信号）
        req << "HEARTBEAT " << cfg_.node_id << ' ' << leaders.size();
        for (uint32_t rid : leaders) req << ' ' << rid;
        req << " regions=" << multi_raft_->all_shards().size() << " keys=" << approx_keys;
        auto resp = meta_rpc(req.str());
        if (resp.rfind("OK HEARTBEAT", 0) != 0) {
          SKV_WARN << "meta HEARTBEAT: " << resp;
          // 仅未知节点强制重注册；瞬时 not_leader/超时不降级，避免 REGISTER 风暴。
          if (resp.find("unknown_node") != std::string::npos) registered = false;
        }
      }
    } catch (const std::exception& e) {
      SKV_WARN << "meta register/heartbeat: " << e.what();
      // connect 失败可能是 Leader 切换：保持 registered，下轮重试 HB/跟随 redirect。
      // 仅明确未注册过时继续走 REGISTER。
      const std::string msg = e.what();
      if (!registered || msg.find("connect") != std::string::npos) {
        // keep flag; REGISTER 仅在从未成功时重试
      }
    }
    for (int i = 0; i < hb_sec * 10 && running_; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

}  // namespace opskv::storage
