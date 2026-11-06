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

// @file store.cpp
// Store：双 FSM 调度 + Hibernate + SnapWorker + 冷卸载钩子。

#include "opskv/raftstore/store.hpp"

#include "opskv/codec.hpp"
#include "opskv/logging.hpp"
#include "opskv/raft/raw_node.hpp"
#include "opskv/raftstore/write_batch_merge.hpp"

#include <chrono>
#include <sstream>
#include <type_traits>
#include <variant>
#include <vector>

namespace opskv::raftstore {
namespace {

std::vector<std::string> split_pipe(const std::string& s) {
  std::vector<std::string> out;
  size_t i = 0;
  while (i <= s.size()) {
    auto p = s.find('|', i);
    if (p == std::string::npos) {
      out.push_back(s.substr(i));
      break;
    }
    out.push_back(s.substr(i, p - i));
    i = p + 1;
  }
  return out;
}

}  // namespace

Store::Store(Options opts, LegacyFactory factory)
    : opts_(opts), factory_(std::move(factory)), router_(std::make_shared<Router>()) {
  batch_ = std::make_unique<BatchSystem>(router_, opts_.batch);
  batch_->set_raft_handler([this](uint32_t rid, std::vector<PeerMsg> msgs) {
    on_raft_batch(rid, std::move(msgs));
  });
  batch_->set_apply_handler([this](uint32_t rid, std::vector<ApplyControl> msgs) {
    on_apply_batch(rid, std::move(msgs));
  });

  SnapWorker::Provider prov = [this](uint32_t rid, raft::SnapshotMeta* meta) -> std::string {
    if (!snap_provider_ || !meta) return {};
    uint64_t li = meta->index;
    uint64_t lt = meta->term;
    auto data = snap_provider_(rid, &li, &lt);
    meta->index = li;
    meta->term = lt;
    return data;
  };
  SnapWorker::Sender send = [this](uint32_t rid, const raft::SnapshotMeta& meta,
                                   const std::string& data) {
    if (!transport_fn_) return;
    // payload: last_idx|last_term|hex(blob)；blob 前可带 SKVC1 ConfState。
    std::string blob = data;
    if (!meta.conf_state.empty()) {
      blob = std::string("SKVC1\n") + meta.conf_state + "\n" + data;
    }
    std::ostringstream oss;
    oss << meta.index << '|' << meta.term << '|' << opskv::to_hex(blob);
    transport_fn_(rid, meta.to, "InstallSnapshotData", oss.str());
    batch_->note_snap_done();
  };
  snap_ = std::make_unique<SnapWorker>(opts_.snap, std::move(prov), std::move(send));
}

Store::~Store() { stop(); }

void Store::start() {
  batch_->start();
  snap_->start();
}

void Store::stop() {
  if (snap_) snap_->stop();
  if (batch_) batch_->stop();
}

void Store::schedule_tick() {
  // BatchSystem 未启动时（单测）：同步 tick，保持 MultiRaft 旧语义。
  if (!batch_ || !batch_->running()) {
    std::vector<std::shared_ptr<Peer>> snap;
    {
      std::lock_guard<std::mutex> lk(mu_);
      for (auto& [_, p] : peers_) snap.push_back(p);
    }
    for (auto& p : snap) handle_tick(*p);
    return;
  }
  router_->broadcast_tick();
}

std::shared_ptr<Peer> Store::peer_locked(uint32_t region_id) {
  auto it = peers_.find(region_id);
  return it == peers_.end() ? nullptr : it->second;
}

raft::RaftNode* Store::get(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto p = peer_locked(region_id);
  return p ? p->legacy() : nullptr;
}

std::shared_ptr<raft::RaftNode> Store::get_shared(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto p = peer_locked(region_id);
  return p ? p->legacy_shared() : nullptr;
}

raft::RaftNode& Store::get_or_create(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  if (auto p = peer_locked(region_id)) {
    if (p->unloaded()) {
      if (reload_fn_) {
        p->mark_loaded(reload_fn_(region_id));
      } else if (factory_) {
        p->mark_loaded(factory_(region_id));
      }
    }
    if (auto* n = p->legacy()) return *n;
  }
  auto node = factory_(region_id);
  auto peer = std::make_shared<Peer>(region_id, node);
  peer->meta().peers = node->peers();
  peers_[region_id] = peer;
  router_->register_peer(region_id, opts_.mailbox_capacity);
  router_->register_apply(region_id, opts_.mailbox_capacity);
  RegionLocalState st;
  st.region_id = region_id;
  meta_.upsert(st);
  return *node;
}

std::shared_ptr<raft::RaftNode> Store::take(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = peers_.find(region_id);
  if (it == peers_.end()) return nullptr;
  auto sp = it->second->legacy_shared();
  peers_.erase(it);
  router_->deregister(region_id);
  meta_.erase(region_id);
  return sp;
}

bool Store::erase(uint32_t region_id) { return static_cast<bool>(take(region_id)); }

void Store::mark_hot(uint32_t region_id) {
  std::shared_ptr<Peer> p;
  {
    std::lock_guard<std::mutex> lk(mu_);
    p = peer_locked(region_id);
  }
  if (!p) return;
  wake_peer(*p);
  router_->send_raft(region_id, SignificantMsg{SignificantMsg::Type::kWake, {}});
}

bool Store::send_raft_message(uint32_t region_id, RaftMessage msg) {
  mark_hot(region_id);
  return router_->send_raft(region_id, std::move(msg));
}

raft::RaftNode* Store::peer_node(Peer& peer) {
  if (auto* raw = peer.raw()) return raw->raft_node();
  return peer.legacy();
}

std::string Store::apply_inbound_raft(uint32_t region_id, RaftMessage msg) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<std::mutex> lk(mu_);
    peer = peer_locked(region_id);
  }
  if (!peer) return "ERR no_region";
  wake_peer(*peer);
  // 同步步进（行协议需立即回包）；同时投递 Router 作唤醒/批处理对齐。
  router_->send_raft(region_id, RaftMessage{msg.from, msg.type, {}});
  return step_raft_message(*peer, msg);
}

std::vector<uint32_t> Store::leader_shards() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  for (const auto& [id, p] : peers_) {
    auto* n = p->legacy();
    if (!n && p->raw()) n = p->raw()->raft_node();
    if (n && n->is_leader()) out.push_back(id);
  }
  return out;
}

std::vector<uint32_t> Store::all_shards() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  out.reserve(peers_.size());
  for (const auto& [id, _] : peers_) out.push_back(id);
  return out;
}

void Store::update_peer_meta(uint32_t region_id, opskv::KeyRange range, uint64_t epoch) {
  std::lock_guard<std::mutex> lk(mu_);
  if (auto p = peer_locked(region_id)) {
    p->meta().range = range;
    p->meta().epoch = epoch;
  }
  meta_.set_range(region_id, range);
  meta_.set_epoch(region_id, epoch);
}

bool Store::apply_queue_overloaded(uint32_t region_id) const {
  return batch_ && batch_->apply_overloaded(region_id);
}

void Store::wake_peer(Peer& peer) {
  peer.set_hibernating(false);
  peer.touch_activity();
  if (peer.unloaded() && (reload_fn_ || factory_)) {
    auto n = reload_fn_ ? reload_fn_(peer.region_id()) : factory_(peer.region_id());
    peer.mark_loaded(std::move(n));
    SKV_INFO << "store reload region=" << peer.region_id();
  }
}

void Store::maybe_hibernate_or_unload(Peer& peer) {
  if (peer.unloaded()) return;
  auto* n = peer_node(peer);
  if (!n) return;
  if (n->is_leader()) {
    peer.set_hibernating(false);
    peer.touch_activity();
    return;
  }
  if (opts_.hibernate_after_ms == 0) return;
  if (!peer.idle_for(std::chrono::milliseconds(opts_.hibernate_after_ms))) return;
  if (!peer.hibernating()) {
    peer.set_hibernating(true);
    SKV_DEBUG << "store hibernate region=" << peer.region_id();
  }
  if (opts_.unload_after_ms > 0 &&
      peer.idle_for(std::chrono::milliseconds(opts_.hibernate_after_ms + opts_.unload_after_ms))) {
    peer.mark_unloaded();
    SKV_INFO << "store unload region=" << peer.region_id();
  }
}

void Store::on_raft_batch(uint32_t region_id, std::vector<PeerMsg> msgs) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<std::mutex> lk(mu_);
    peer = peer_locked(region_id);
  }
  if (!peer) return;
  for (auto& m : msgs) {
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, TickMsg>) {
            handle_tick(*peer);
          } else if constexpr (std::is_same_v<T, RaftMessage>) {
            wake_peer(*peer);
            handle_raft_message(*peer, arg);
          } else if constexpr (std::is_same_v<T, SignificantMsg>) {
            wake_peer(*peer);
            if (arg.type == SignificantMsg::Type::kMsgUnreachable && peer->legacy()) {
              peer->legacy()->on_rpc_transport_fail(arg.peer_id);
            }
          } else if constexpr (std::is_same_v<T, ApplyRes>) {
            peer->touch_activity();
            if (peer->legacy()) {
              meta_.set_leader(region_id, peer->legacy()->soft_state().leader_id);
            }
          }
        },
        m);
  }
}

void Store::handle_tick(Peer& peer) {
  if (peer.unloaded()) {
    maybe_hibernate_or_unload(peer);
    return;
  }
  auto* n = peer_node(peer);
  if (!n) return;
  // Hibernate：多数 tick 跳过；每 16 次仍 tick 一次，避免全体休眠后无法选主。
  if (peer.hibernating()) {
    static thread_local uint64_t cold_n = 0;
    if ((++cold_n & 0xfu) != 0) {
      maybe_hibernate_or_unload(peer);
      return;
    }
  }
  n->tick();
  if (n->consume_activity_flag() || n->is_leader()) {
    peer.touch_activity();
  }
  process_ready_legacy(peer);
  maybe_hibernate_or_unload(peer);
  meta_.set_leader(peer.region_id(), n->soft_state().leader_id);
}

void Store::handle_raft_message(Peer& peer, const RaftMessage& msg) {
  if (msg.payload.empty()) {
    // 仅唤醒（apply_inbound_raft 已同步 step）
    peer.touch_activity();
    process_ready_legacy(peer);
    return;
  }
  (void)step_raft_message(peer, msg);
}

std::string Store::step_raft_message(Peer& peer, const RaftMessage& msg) {
  auto* n = peer_node(peer);
  if (!n) return "ERR no_region";
  peer.touch_activity();
  const auto& type = msg.type;
  const auto parts = split_pipe(msg.payload);
  try {
    if (type == "RequestVote" || type == "RequestPreVote") {
      if (parts.size() < 3) return "ERR bad_vote";
      const uint64_t term = std::stoull(parts[0]);
      const uint64_t last_idx = std::stoull(parts[1]);
      const uint64_t last_term = std::stoull(parts[2]);
      bool granted = false;
      if (type == "RequestPreVote") {
        n->on_request_pre_vote(msg.from, term, last_idx, last_term, &granted);
      } else {
        n->on_request_vote(msg.from, term, last_idx, last_term, &granted);
      }
      process_ready_legacy(peer);
      auto st = n->soft_state();
      return std::string("OK ") + (type == "RequestPreVote" ? "PREVOTE " : "VOTE ") +
             std::to_string(st.term) + " " + (granted ? "1" : "0");
    }
    if (type == "AppendEntries") {
      // term|prev|prev_term|commit|n|[term|idx|hex]*
      if (parts.size() < 5) return "ERR bad_ae";
      const uint64_t term = std::stoull(parts[0]);
      const uint64_t prev_idx = std::stoull(parts[1]);
      const uint64_t prev_term = std::stoull(parts[2]);
      const uint64_t leader_commit = std::stoull(parts[3]);
      const uint64_t nent = std::stoull(parts[4]);
      std::vector<raft::LogEntry> entries;
      entries.reserve(static_cast<size_t>(nent));
      size_t off = 5;
      for (uint64_t i = 0; i < nent; ++i) {
        if (off + 2 >= parts.size()) return "ERR ae_trunc";
        raft::LogEntry e;
        e.term = std::stoull(parts[off]);
        e.index = std::stoull(parts[off + 1]);
        if (!opskv::from_hex(parts[off + 2], &e.data)) return "ERR ae_bad_hex";
        entries.push_back(std::move(e));
        off += 3;
      }
      bool success = false;
      uint64_t match = 0;
      n->on_append_entries(msg.from, term, prev_idx, prev_term, entries, leader_commit, &success,
                           &match);
      process_ready_legacy(peer);
      auto st = n->soft_state();
      return "OK AE " + std::to_string(st.term) + " " + (success ? "1" : "0") + " " +
             std::to_string(match);
    }
    if (type == "InstallSnapshot") {
      // term|last_idx|last_term|hex
      if (parts.size() < 4) return "ERR bad_snap";
      const uint64_t term = std::stoull(parts[0]);
      const uint64_t last_idx = std::stoull(parts[1]);
      const uint64_t last_term = std::stoull(parts[2]);
      std::string data;
      if (!opskv::from_hex(parts[3], &data)) return "ERR snap_bad_hex";
      bool success = false;
      n->on_install_snapshot(msg.from, term, last_idx, last_term, data, &success);
      process_ready_legacy(peer);
      auto st = n->soft_state();
      return "OK SNAP " + std::to_string(st.term) + " " + (success ? "1" : "0") + " " +
             std::to_string(last_idx);
    }
  } catch (...) {
    return "ERR step";
  }
  process_ready_legacy(peer);
  return "ERR unknown_raft_type";
}

void Store::process_ready_legacy(Peer& peer) {
  auto drive = [&](auto take_ready_fn, auto advance_fn, auto has_fn) {
    while (has_fn()) {
      raft::Ready rd = take_ready_fn();
      if (persist_fn_) persist_fn_(peer.region_id(), rd);
      if (transport_fn_) {
        for (const auto& m : rd.messages) {
          if (m.type == "InstallSnapshot" &&
              ((rd.snapshot && rd.snapshot->to == m.to) || m.payload.empty() ||
               m.payload.back() == '|')) {
            // 空 blob / SnapshotMeta：走 SnapWorker，与心跳隔离
            if (batch_ && batch_->snap_overloaded()) {
              SKV_WARN << "snap QoS reject region=" << peer.region_id();
              continue;
            }
            SnapTask t;
            t.region_id = peer.region_id();
            if (rd.snapshot) t.meta = *rd.snapshot;
            else {
              t.meta.to = m.to;
            }
            if (batch_) batch_->note_snap_enqueued();
            if (snap_) snap_->enqueue(std::move(t));
            continue;
          }
          transport_fn_(peer.region_id(), m.to, m.type, m.payload);
        }
      }
      if (!rd.committed_entries.empty()) {
        if (batch_ && batch_->apply_overloaded(peer.region_id())) {
          SKV_WARN << "apply QoS reject region=" << peer.region_id();
        } else {
          ApplyMsg am;
          am.region_id = peer.region_id();
          am.entries = rd.committed_entries;
          router_->send_apply(peer.region_id(), std::move(am));
        }
      }
      advance_fn(rd);
    }
  };

  if (auto* raw = peer.raw()) {
    drive([&] { return raw->ready(); }, [&](const raft::Ready& rd) { raw->advance(rd); },
          [&] { return raw->has_ready(); });
    return;
  }
  auto* n = peer.legacy();
  if (!n || !n->has_ready()) return;
  // external_io RaftNode：Ready 外提
  drive([&] { return n->take_ready(); }, [&](const raft::Ready& rd) { n->advance(rd); },
        [&] { return n->has_ready(); });
}

void Store::on_apply_batch(uint32_t region_id, std::vector<ApplyControl> msgs) {
  // Phase S3：同 worker 一轮内多 Region ApplyMsg 展平后逐条 apply_fn；
  // 引擎侧可用 KvEngine::write_batch 合并（StorageNode on_apply 仍按条语义）。
  std::vector<RegionApplySlice> slices;
  for (auto& m : msgs) {
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, ApplyMsg>) {
            RegionApplySlice sl;
            sl.region_id = arg.region_id ? arg.region_id : region_id;
            sl.entries = arg.entries;
            slices.push_back(std::move(sl));
          } else if constexpr (std::is_same_v<T, SnapTask>) {
            if (batch_ && !batch_->snap_overloaded()) {
              batch_->note_snap_enqueued();
              if (snap_) snap_->enqueue(std::move(arg));
            }
          }
        },
        m);
  }
  if (!apply_fn_ || slices.empty()) return;
  const auto flat = flatten_apply_slices(slices);
  for (const auto& [rid, e] : flat) {
    const bool is_conf = !e.data.empty() && static_cast<unsigned char>(e.data[0]) == 0xFF;
    apply_fn_(rid, e.index, e.data);
    if (auto* n = get(rid)) n->report_applied(e.index);
    if (is_conf) {
      router_->send_raft(rid, ApplyRes{rid, e.index, true});
    }
  }
}

}  // namespace opskv::raftstore
