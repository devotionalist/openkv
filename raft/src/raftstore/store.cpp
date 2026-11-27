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
// 实现：Tick / 入站 step / Ready 合并 / ready_coalesce / Hibernate / 冷卸载。
// on_raft_batch 批末 process_ready（可 pipeline 到 PersistWorker）；request_ready 攒批。

#include "openkv/raftstore/store.hpp"

#include <chrono>
#include <type_traits>
#include <variant>
#include <vector>

#include "openkv/logging.hpp"
#include "openkv/raft/util/snap_stream.hpp"
#include "openkv/raftstore/errors.hpp"
#include "openkv/raftstore/write_batch_merge.hpp"

namespace openkv::raftstore {

Store::Store(Options opts, NodeFactory factory)
    : opts_(opts), factory_(std::move(factory)), router_(std::make_shared<Router>()) {
  batch_ = std::make_unique<BatchSystem>(router_, opts_.batch);
  batch_->set_raft_handler(
      [this](uint32_t rid, std::vector<PeerMsg> msgs) { on_raft_batch(rid, std::move(msgs)); });
  batch_->set_apply_handler([this](uint32_t rid, std::vector<ApplyControl> msgs) {
    on_apply_batch(rid, std::move(msgs));
  });
  // mailbox 投递 → schedule 唤醒对应 worker（不再每轮扫全表）。
  router_->set_raft_notify([this](uint32_t rid) { batch_->schedule_raft(rid); });
  router_->set_apply_notify([this](uint32_t rid) { batch_->schedule_apply(rid); });

  SnapWorker::Provider prov = [this](uint32_t rid, raft::SnapshotMeta* meta) -> std::string {
    if (!snap_provider_ || !meta) {
      return {};
    }
    uint64_t li = meta->index;
    uint64_t lt = meta->term;
    auto data = snap_provider_(rid, &li, &lt);
    meta->index = li;
    meta->term = lt;
    return data;
  };
  // 【出网入口·快照】SnapWorker 拉完 blob 后组 InstallSnapshotReq，交给 transport_fn_。
  SnapWorker::Sender send = [this](uint32_t rid, const raft::SnapshotMeta& meta,
                                   const std::string& data) {
    if (!transport_fn_) {
      return;
    }
    std::string blob = data;
    if (!meta.conf_state.empty()) {
      blob = std::string(raft::kMagicSnapConf) + meta.conf_state + "\n" + data;
    }
    uint64_t raft_term = 0;
    if (auto* n = get(rid)) {
      raft_term = n->soft_state().term;
    }
    raft::Message msg;
    msg.to = meta.to;
    msg.type = raft::MsgType::kInstallSnapshot;
    raft::InstallSnapshotReq req;
    req.term = raft_term;
    req.last_index = meta.index;
    req.last_term = meta.term;
    req.data = std::move(blob);
    msg.payload = std::move(req);
    transport_fn_(rid, std::move(msg));
    batch_->note_snap_done();
  };
  snap_ = std::make_unique<SnapWorker>(opts_.snap, std::move(prov), std::move(send));
  if (opts_.persist_workers > 0) {
    persist_ = std::make_unique<PersistWorker>(opts_.persist_workers);
  }
}

Store::~Store() { stop(); }

void Store::start() {
  batch_->start();
  snap_->start();
  if (persist_) {
    persist_->start();
  }
  const bool need_coalesce = opts_.ready_coalesce_n > 1 || opts_.ready_coalesce_ms > 0;
  if (need_coalesce && !coalesce_running_.exchange(true)) {
    coalesce_thr_ = std::thread([this] { coalesce_loop(); });
  }
}

void Store::stop() {
  if (coalesce_running_.exchange(false)) {
    coalesce_cv_.notify_all();
    if (coalesce_thr_.joinable()) {
      coalesce_thr_.join();
    }
  }
  // 冲刷残留攒批，避免 stop 后悬空 propose。
  std::vector<uint32_t> flush_ids;
  {
    std::lock_guard<std::mutex> lk(coalesce_mu_);
    for (auto& [rid, st] : coalesce_) {
      if (st.pending > 0 || st.armed) {
        flush_ids.push_back(rid);
      }
      st.pending = 0;
      st.armed = false;
    }
  }
  for (auto rid : flush_ids) {
    drive_ready(rid);
  }
  // 先停 PersistWorker（排空 IO），再停 BatchSystem，避免 PersistDone 无人收。
  if (persist_) {
    persist_->stop();
  }
  if (snap_) {
    snap_->stop();
  }
  if (batch_) {
    batch_->stop();
  }
}

bool Store::async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                        std::string* error) {
  // 写路径唯一入口（原 storage::RaftKV）：propose 只改内存日志 → request_ready 攒批。
  auto* n = get(region_id);
  if (!n) {
    if (error) {
      *error = err::kNoRegion;
    }
    return false;
  }
  if (!n->is_leader()) {
    if (error) {
      *error = err::kNotLeader;
    }
    return false;
  }
  if (!n->propose(std::move(data), assigned_index)) {
    if (error) {
      *error = err::kProposeFailed;
    }
    return false;
  }
  request_ready(region_id);
  return true;
}

void Store::request_ready(uint32_t region_id) {
  // Propose 热路径调用：把多次 propose 合成一次 Ready/persist（group commit）。
  // 触发条件：pending>=ready_coalesce_n，或 coalesce_loop 等到 ready_coalesce_ms。
  // 关闭攒批（n≤1 且 ms=0）：立即 drive_ready → 邮箱 kWake → on_raft_batch 批末落盘。
  if (opts_.ready_coalesce_n <= 1 && opts_.ready_coalesce_ms == 0) {
    drive_ready(region_id);
    return;
  }
  bool should_flush = false;
  {
    std::lock_guard<std::mutex> lk(coalesce_mu_);
    auto& st = coalesce_[region_id];
    st.pending++;
    const bool count_hit = opts_.ready_coalesce_n > 1 && st.pending >= opts_.ready_coalesce_n;
    if (count_hit) {
      st.pending = 0;
      st.armed = false;
      should_flush = true;
    } else if (opts_.ready_coalesce_ms > 0) {
      const auto dl =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(opts_.ready_coalesce_ms);
      if (!st.armed) {
        st.armed = true;
        st.deadline = dl;
        coalesce_cv_.notify_one();
      } else if (dl < st.deadline) {
        st.deadline = dl;
      }
    }
    // 仅计数模式且未满：继续攒，等满 N 或定时器。
  }
  if (should_flush) {
    drive_ready(region_id);
  }
}

void Store::coalesce_loop() {
  // 专职「按时间窗」把多次 propose 合成一次 Ready：
  // request_ready 在未满 ready_coalesce_n 时只 armed+设 deadline 并 notify 本线程；
  // 本循环睡到最早 deadline，把到期 Region 列表拉出来，锁外逐个 drive_ready。
  // （满 N 条那条路径在 request_ready 里直接 flush，不进这里。）
  using clock = std::chrono::steady_clock;
  while (coalesce_running_.load(std::memory_order_acquire)) {
    std::vector<uint32_t> due;
    {
      std::unique_lock<std::mutex> lk(coalesce_mu_);

      // —— 1. 扫一遍：找出当前最早的 deadline（最近一票要到期的 Region）——
      clock::time_point next = clock::time_point::max();
      for (auto& [rid, st] : coalesce_) {
        if (st.armed && st.deadline < next) {
          next = st.deadline;
        }
      }

      // —— 2. 睡：没人 armed 就无限等 notify；有人则睡到 earliest deadline ——
      if (next == clock::time_point::max()) {
        // 全表空闲：等 request_ready 第一次 armed 时 coalesce_cv_.notify_one()，或 stop。
        coalesce_cv_.wait(lk, [this] {
          if (!coalesce_running_.load(std::memory_order_acquire)) {
            return true;
          }
          for (auto& [rid, st] : coalesce_) {
            (void)rid;
            if (st.armed) {
              return true;
            }
          }
          return false;
        });
      } else {
        // 有定时器：最多睡到 next；stop 时也会被 notify_all 提前叫醒。
        coalesce_cv_.wait_until(
            lk, next, [this] { return !coalesce_running_.load(std::memory_order_acquire); });
      }

      // —— 3. 停机检查 ——
      if (!coalesce_running_.load(std::memory_order_acquire)) {
        break;
      }

      // —— 4. 收集已到期的 Region：清 pending/armed，记下 rid（锁内只记账）——
      const auto now = clock::now();
      for (auto& [rid, st] : coalesce_) {
        if (st.armed && st.deadline <= now) {
          st.pending = 0;
          st.armed = false;
          due.push_back(rid);
        }
      }
    }  // 释放 coalesce_mu_：后面 drive_ready 会进 Router/邮箱，不能持这把锁

    // —— 5. 锁外 flush：对每个到期 Region 投 kWake，走 mailbox → raft worker → Ready ——
    for (auto rid : due) {
      drive_ready(rid);
    }
  }
}

void Store::schedule_tick() {
  // 一律经 Router → BatchSystem；调用方须先 Store::start()。
  router_->broadcast_tick();
}

void Store::drive_ready(uint32_t region_id) {
  // 只对本 Region 投递 Wake；同批多个 propose 合并为一次 Ready/sync。
  // mark_hot = wake + kWake；勿再 schedule_tick（避免每 Put 全表 Tick）。
  // 须先 Store::start()，否则邮箱无人 drain。
  mark_hot(region_id);
}

std::shared_ptr<Peer> Store::peer_locked(uint32_t region_id) {
  auto it = peers_.find(region_id);
  return it == peers_.end() ? nullptr : it->second;
}

raft::RaftNode* Store::get(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto p = peer_locked(region_id);
  return p ? p->raft_node() : nullptr;
}

std::shared_ptr<raft::RaftNode> Store::get_shared(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto p = peer_locked(region_id);
  return p ? p->raft_node_shared() : nullptr;
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
    if (auto* n = p->raft_node()) {
      return *n;
    }
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
  if (it == peers_.end()) {
    return nullptr;
  }
  auto sp = it->second->raft_node_shared();
  peers_.erase(it);
  router_->deregister(region_id);
  meta_.erase(region_id);
  return sp;
}

bool Store::erase(uint32_t region_id) { return static_cast<bool>(take(region_id)); }

// 点亮 peer：本地 wake + 邮箱 kWake（不等于 drive_ready / 攒批 flush）。
void Store::mark_hot(uint32_t region_id) {
  std::shared_ptr<Peer> p;
  {
    std::lock_guard<std::mutex> lk(mu_);
    p = peer_locked(region_id);
  }
  if (!p) {
    return;
  }
  wake_peer(*p);
  router_->send_raft(region_id, SignificantMsg{SignificantMsg::Type::kWake, {}});
}

bool Store::send_raft_message(uint32_t region_id, RaftMessage msg) {
  mark_hot(region_id);
  return router_->send_raft(region_id, std::move(msg));
}

raft::RaftNode* Store::peer_node(Peer& peer) { return peer.raft_node(); }

raft::RaftReply Store::apply_inbound_raft(uint32_t region_id, RaftMessage msg) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<std::mutex> lk(mu_);
    peer = peer_locked(region_id);
  }
  if (!peer) {
    return raft::RaftReply::error(openkv::err::line(err::kNoRegion));
  }
  wake_peer(*peer);
  // 同步步进（gRPC 需立即回包）；同时投递 Router 作唤醒/批处理对齐。
  RaftMessage wake;
  wake.from = msg.from;
  wake.type = msg.type;
  router_->send_raft(region_id, std::move(wake));
  return step_raft_message(*peer, msg, /*run_ready=*/true);
}

std::vector<uint32_t> Store::leader_shards() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  for (const auto& [id, p] : peers_) {
    auto* n = p->raft_node();
    if (n && n->is_leader()) {
      out.push_back(id);
    }
  }
  return out;
}

std::vector<uint32_t> Store::all_shards() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  out.reserve(peers_.size());
  for (const auto& [id, _] : peers_) {
    out.push_back(id);
  }
  return out;
}

void Store::update_peer_meta(uint32_t region_id, openkv::KeyRange range, uint64_t epoch) {
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
  if (peer.unloaded()) {
    return;
  }
  auto* n = peer_node(peer);
  if (!n) {
    return;
  }
  if (n->is_leader()) {
    peer.set_hibernating(false);
    peer.touch_activity();
    return;
  }
  if (opts_.hibernate_after_ms == 0) {
    return;
  }
  if (!peer.idle_for(std::chrono::milliseconds(opts_.hibernate_after_ms))) {
    return;
  }
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
  // BatchSystem raft worker 回调：同 Region 串行 drain 邮箱。
  // 先 step/tick；批末 process_ready（可异步 persist）；kPersistDone 收尾发网/apply。
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<std::mutex> lk(mu_);
    peer = peer_locked(region_id);
  }
  if (!peer) {
    return;
  }
  // Group commit：同批 Tick/Wake/Msg 只 process_ready 一次。
  bool did_tick = false;
  bool need_ready = false;
  for (auto& m : msgs) {
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, TickMsg>) {
            if (!did_tick) {
              handle_tick(*peer, /*run_ready=*/false);
              did_tick = true;
            }
            need_ready = true;
          } else if constexpr (std::is_same_v<T, RaftMessage>) {
            wake_peer(*peer);
            handle_raft_message(*peer, arg);
            need_ready = true;
          } else if constexpr (std::is_same_v<T, SignificantMsg>) {
            if (arg.type == SignificantMsg::Type::kPersistDone) {
              finish_persist_pipeline(*peer);
              need_ready = true;  // 可能还有后续 Ready
              return;
            }
            wake_peer(*peer);
            if (arg.type == SignificantMsg::Type::kMsgUnreachable && peer->raft_node()) {
              peer->raft_node()->on_rpc_transport_fail(arg.peer_id);
            }
            need_ready = true;  // kWake / TimeoutNow 等：批末 Ready
          } else if constexpr (std::is_same_v<T, ApplyRes>) {
            peer->touch_activity();
            if (peer->raft_node()) {
              meta_.set_leader(region_id, peer->raft_node()->soft_state().leader_id);
            }
          }
        },
        m);
  }
  if (need_ready) {
    process_ready(*peer, persist_pipeline_enabled());
  }
}

void Store::handle_tick(Peer& peer, bool run_ready) {
  if (peer.unloaded()) {
    maybe_hibernate_or_unload(peer);
    return;
  }
  auto* n = peer_node(peer);
  if (!n) {
    return;
  }
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
  if (run_ready) {
    process_ready(peer, /*pipeline=*/false);
  }
  maybe_hibernate_or_unload(peer);
  meta_.set_leader(peer.region_id(), n->soft_state().leader_id);
}

void Store::handle_raft_message(Peer& peer, const RaftMessage& msg) {
  if (msg.is_wake()) {
    // 仅唤醒（apply_inbound_raft 已同步 step）；Ready 留给批末。
    peer.touch_activity();
    return;
  }
  (void)step_raft_message(peer, msg, /*run_ready=*/false);
}

raft::RaftReply Store::step_raft_message(Peer& peer, const RaftMessage& msg, bool run_ready) {
  auto* n = peer_node(peer);
  if (!n) {
    return raft::RaftReply::error(openkv::err::line(err::kNoRegion));
  }
  peer.touch_activity();
  auto maybe_ready = [&] {
    // 入站须同步落盘后再回包，禁止 pipeline。
    if (run_ready) {
      process_ready(peer, /*pipeline=*/false);
    }
  };
  try {
    if (msg.type == raft::MsgType::kRequestVote || msg.type == raft::MsgType::kRequestPreVote) {
      const auto* v = std::get_if<raft::VoteReq>(&msg.payload);
      if (!v) {
        return raft::RaftReply::error(openkv::err::line(err::kBadVote));
      }
      bool granted = false;
      if (msg.type == raft::MsgType::kRequestPreVote) {
        n->on_request_pre_vote(msg.from, v->term, v->last_log_index, v->last_log_term, &granted);
      } else {
        n->on_request_vote(msg.from, v->term, v->last_log_index, v->last_log_term, &granted);
      }
      maybe_ready();
      raft::RaftReply r;
      r.kind = msg.type == raft::MsgType::kRequestPreVote ? raft::RaftReply::Kind::kPreVote
                                                          : raft::RaftReply::Kind::kVote;
      r.term = n->soft_state().term;
      r.ok = granted;
      return r;
    }
    if (msg.type == raft::MsgType::kAppendEntries) {
      const auto* ae = std::get_if<raft::AppendEntriesReq>(&msg.payload);
      if (!ae) {
        return raft::RaftReply::error(openkv::err::line(err::kBadAe));
      }
      bool success = false;
      uint64_t match = 0;
      n->on_append_entries(msg.from, ae->term, ae->prev_log_index, ae->prev_log_term, ae->entries,
                           ae->leader_commit, &success, &match);
      maybe_ready();
      raft::RaftReply r;
      r.kind = raft::RaftReply::Kind::kAppend;
      r.term = n->soft_state().term;
      r.ok = success;
      r.match_index = match;
      return r;
    }
    if (msg.type == raft::MsgType::kInstallSnapshot) {
      const auto* sn = std::get_if<raft::InstallSnapshotReq>(&msg.payload);
      if (!sn) {
        return raft::RaftReply::error(openkv::err::line(err::kBadSnap));
      }
      bool success = false;
      n->on_install_snapshot(msg.from, sn->term, sn->last_index, sn->last_term, sn->data, &success);
      maybe_ready();
      raft::RaftReply r;
      r.kind = raft::RaftReply::Kind::kSnap;
      r.term = n->soft_state().term;
      r.ok = success;
      r.match_index = sn->last_index;
      return r;
    }
  } catch (...) {
    return raft::RaftReply::error(openkv::err::line(err::kStep));
  }
  maybe_ready();
  return raft::RaftReply::error(openkv::err::line(err::kUnknownRaftType));
}

void Store::dispatch_ready_after_persist(Peer& peer, raft::Ready& rd) {
  // persist 已完成；下面才进入「出站」。mailbox/ReadyPump 此前都还不是网络。
  if (transport_fn_) {
    for (auto& m : rd.messages) {
      const auto* snap_req = std::get_if<raft::InstallSnapshotReq>(&m.payload);
      const bool snap_shell =
          m.type == raft::MsgType::kInstallSnapshot &&
          ((rd.snapshot && rd.snapshot->to == m.to) || !snap_req || snap_req->data.empty());
      if (snap_shell) {
        if (batch_ && batch_->snap_overloaded()) {
          SKV_WARN << "snap QoS reject region=" << peer.region_id();
          continue;
        }
        // 【非网络】快照壳先扔给 SnapWorker；真出网在 SnapWorker::Sender → transport_fn_。
        SnapTask t;
        t.region_id = peer.region_id();
        if (rd.snapshot) {
          t.meta = *rd.snapshot;
        } else {
          t.meta.to = m.to;
        }
        if (batch_) {
          batch_->note_snap_enqueued();
        }
        if (snap_) {
          snap_->enqueue(std::move(t));
        }
        continue;
      }
      // 【出网入口】AE/Vote/已带 blob 的 InstallSnapshot → 调用方 transport_fn_（如 gRPC bind）。
      transport_fn_(peer.region_id(), std::move(m));
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
  if (auto* n = peer.raft_node()) {
    n->advance(rd);
  }
}

void Store::finish_persist_pipeline(Peer& peer) {
  auto rd = peer.take_pending_ready();
  peer.set_persist_inflight(false);
  if (!rd) {
    return;
  }
  dispatch_ready_after_persist(peer, *rd);
}

void Store::process_ready(Peer& peer, bool pipeline) {
  auto* n = peer.raft_node();
  if (!n || !n->has_ready()) {
    return;
  }

  // 落盘：自定义 persist_fn_ 优先；否则模块内默认 node->persist_ready（通用库闭环）。
  auto do_persist = [this](uint32_t rid, raft::RaftNode* node, const raft::Ready& rd) {
    if (persist_fn_) {
      persist_fn_(rid, rd);
    } else if (node) {
      node->persist_ready(rd);
    }
  };

  if (pipeline && persist_pipeline_enabled()) {
    if (peer.persist_inflight()) {
      return;
    }
    // 每次只提交一批 Ready 到 IO；完成后由 PersistDone 再发网/apply/advance。
    // 落盘线程只需 HS/entries/must_sync；messages 只留在 pending（勿整份 Ready 拷去 IO 线程）。
    raft::Ready rd = n->take_ready();
    const uint32_t rid = peer.region_id();
    auto rd_io = std::make_shared<raft::Ready>();
    rd_io->hard_state = rd.hard_state;
    rd_io->entries = rd.entries;
    rd_io->must_sync = rd.must_sync;
    peer.set_pending_ready(std::move(rd));
    peer.set_persist_inflight(true);
    persist_->submit([this, rid, rd_io] {
      // PersistWorker 线程：用 get(rid) 取节点，勿捕获 Peer&。
      if (persist_fn_) {
        persist_fn_(rid, *rd_io);
      } else if (auto* node = get(rid)) {
        node->persist_ready(*rd_io);
      }
      router_->send_raft(rid, SignificantMsg{SignificantMsg::Type::kPersistDone, {}});
    });
    return;
  }

  // 同步路径：入站 RPC / persist_workers=0。
  while (n->has_ready()) {
    raft::Ready rd = n->take_ready();
    do_persist(peer.region_id(), n, rd);
    dispatch_ready_after_persist(peer, rd);
  }
}

void Store::on_apply_batch(uint32_t region_id, std::vector<ApplyControl> msgs) {
  // 同 Region 多 ApplyMsg 展平后优先 ApplyBatchFn（一次 WriteBatch）；否则逐条 ApplyFn。
  std::vector<RegionApplySlice> slices;
  for (auto& m : msgs) {
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, ApplyMsg>) {
            RegionApplySlice sl;
            sl.region_id = arg.region_id ? arg.region_id : region_id;
            sl.entries = std::move(arg.entries);
            slices.push_back(std::move(sl));
          } else if constexpr (std::is_same_v<T, SnapTask>) {
            if (batch_ && !batch_->snap_overloaded()) {
              batch_->note_snap_enqueued();
              if (snap_) {
                snap_->enqueue(std::move(arg));
              }
            }
          }
        },
        m);
  }
  if (slices.empty()) {
    return;
  }

  auto finish_conf = [&](uint32_t rid, const raft::LogEntry& e) {
    const bool is_conf = !e.data.empty() && static_cast<unsigned char>(e.data[0]) == 0xFF;
    if (is_conf) {
      router_->send_raft(rid, ApplyRes{rid, e.index, true});
    }
  };

  if (apply_batch_fn_) {
    for (auto& sl : slices) {
      if (sl.entries.empty()) {
        continue;
      }
      apply_batch_fn_(sl.region_id, sl.entries);
      for (const auto& e : sl.entries) {
        finish_conf(sl.region_id, e);
      }
      // 整批只 report 一次：maybe_compact / persist 按批触发，避免每条空转 compact。
      if (auto* n = get(sl.region_id)) {
        n->report_applied(sl.entries.back().index);
      }
    }
    return;
  }
  if (!apply_fn_) {
    return;
  }
  const auto flat = flatten_apply_slices(slices);
  uint32_t cur_rid = 0;
  uint64_t cur_last = 0;
  bool have = false;
  for (const auto& [rid, e] : flat) {
    apply_fn_(rid, e.index, e.data);
    finish_conf(rid, e);
    if (!have || rid != cur_rid) {
      if (have) {
        if (auto* n = get(cur_rid)) {
          n->report_applied(cur_last);
        }
      }
      cur_rid = rid;
      cur_last = e.index;
      have = true;
    } else {
      cur_last = e.index;
    }
  }
  if (have) {
    if (auto* n = get(cur_rid)) {
      n->report_applied(cur_last);
    }
  }
}

}  // namespace openkv::raftstore
