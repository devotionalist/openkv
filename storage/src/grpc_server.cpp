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

// @file grpc_server.cpp
// Storage gRPC：Store/Admin/Raft 一律 CallbackService；Finish@apply / reactor，无 WorkPool。

#include "openkv/storage/grpc_server.hpp"

#include "openkv/admin_acl.hpp"
#include "openkv/grpc_auth.hpp"
#include "openkv/grpc_shutdown.hpp"
#include "openkv/logging.hpp"
#include "openkv/protocol.hpp"
#include "openkv/storage/storage_node.hpp"
#include "openkv/storage/watch_log.hpp"

#include <sstream>
#include <stdexcept>
#include <utility>

#include <grpcpp/grpcpp.h>
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace openkv::storage {
namespace {

std::string extract_redirect(std::string_view s) {
  auto p = s.find("redirect=");
  if (p == std::string_view::npos) return {};
  auto rest = s.substr(p + 9);
  auto sp = rest.find_first_of(" \t\r\n");
  if (sp != std::string_view::npos) rest = rest.substr(0, sp);
  return std::string(rest);
}

using grpc::CallbackServerContext;
using grpc::ServerContext;
using grpc::ServerReadReactor;
using grpc::ServerUnaryReactor;
using grpc::Status;
namespace pb = ::storage::v1;

void fill_propose_response(pb::ProposeResponse* resp, const ProposeResult& r) {
  resp->set_ok(r.ok);
  resp->set_revision(r.revision);
  resp->set_error(r.error);
  resp->set_redirect_to(r.redirect_to);
  resp->set_cas_failed(r.cas_failed);
  resp->set_current_revision(r.current_revision);
}

Command command_from_propose_req(const pb::ProposeRequest& req) {
  Command cmd;
  cmd.op = req.op() == pb::ProposeRequest::DELETE ? CmdOp::kDelete : CmdOp::kPut;
  cmd.key = req.key();
  cmd.value = req.value();
  cmd.ttl_ms = stamp_expire_at_ms(req.ttl_ms());
  cmd.cas = req.cas();
  cmd.expected_revision = req.expected_revision();
  return cmd;
}

class StoreServiceImpl final : public pb::StoreService::CallbackService {
 public:
  StoreServiceImpl(StoreFacade& f, std::string token) : facade_(f), token_(std::move(token)) {}

  ServerUnaryReactor* Get(CallbackServerContext* ctx, const pb::StoreGetRequest* req,
                          pb::StoreGetResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    if (req->start_ts() == 0) {
      auto r = facade_.get(req->shard_id(), req->key(), req->linearizable());
      resp->set_found(r.found);
      resp->set_value(r.value);
      resp->set_revision(r.revision);
      resp->set_is_leader(r.is_leader);
      resp->set_redirect_to(r.redirect_to);
    } else {
      auto r = facade_.txn_get(req->shard_id(), req->key(), req->start_ts(), req->linearizable());
      resp->set_found(r.found);
      resp->set_value(r.value);
      resp->set_revision(r.revision);
      resp->set_is_leader(r.is_leader);
      resp->set_redirect_to(r.redirect_to);
      resp->set_locked(r.locked);
      if (r.locked) {
        resp->set_lock_primary(r.blocking_lock.primary);
        resp->set_lock_ts(r.blocking_lock.start_ts);
      }
      if (!r.error.empty()) resp->set_error(r.error);
    }
    reactor->Finish(Status::OK);
    return reactor;
  }

  // Finish-on-apply：callback 线程只发起 async，apply/超时后再 Finish。
  ServerUnaryReactor* Propose(CallbackServerContext* ctx, const pb::ProposeRequest* req,
                              pb::ProposeResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto finish = [reactor, resp](ProposeResult r) {
      fill_propose_response(resp, r);
      reactor->Finish(Status::OK);
    };
    const std::string& primary = req->primary().empty() ? req->key() : req->primary();
    switch (req->op()) {
      case pb::ProposeRequest::ONEPC:
        facade_.one_pc_async(req->shard_id(), req->key(), req->start_ts(), req->commit_ts(), primary,
                             static_cast<uint8_t>(req->mvcc_op()), req->value(), req->epoch(),
                             req->cas(), req->expected_revision(), req->ttl_ms(), std::move(finish));
        break;
      case pb::ProposeRequest::PREWRITE:
        facade_.prewrite_async(req->shard_id(), req->key(), req->start_ts(), primary,
                               static_cast<uint8_t>(req->mvcc_op()), req->value(), req->epoch(),
                               req->cas(), req->expected_revision(), req->ttl_ms(),
                               std::move(finish));
        break;
      case pb::ProposeRequest::COMMIT:
        facade_.commit_async(req->shard_id(), req->key(), req->start_ts(), req->commit_ts(),
                             req->epoch(), std::move(finish));
        break;
      case pb::ProposeRequest::ROLLBACK:
        facade_.rollback_async(req->shard_id(), req->key(), req->start_ts(), req->epoch(),
                               std::move(finish));
        break;
      case pb::ProposeRequest::PESSIMISTIC_LOCK:
        facade_.pessimistic_lock_async(req->shard_id(), req->key(), req->start_ts(),
                                       req->for_update_ts(), primary, req->lock_ttl_ms(),
                                       req->epoch(), std::move(finish));
        break;
      case pb::ProposeRequest::PESSIMISTIC_ROLLBACK:
        facade_.pessimistic_rollback_async(req->shard_id(), req->key(), req->start_ts(),
                                           req->epoch(), std::move(finish));
        break;
      case pb::ProposeRequest::RESOLVE_LOCK:
        facade_.resolve_lock_async(req->shard_id(), req->key(), req->start_ts(), req->commit_ts(),
                                   req->epoch(), std::move(finish));
        break;
      case pb::ProposeRequest::PUT:
      case pb::ProposeRequest::DELETE:
      default:
        facade_.propose_async(req->shard_id(), command_from_propose_req(*req), std::move(finish),
                              req->epoch());
        break;
    }
    return reactor;
  }

  ServerUnaryReactor* Scan(CallbackServerContext* ctx, const pb::StoreScanRequest* req,
                           pb::StoreScanResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto r = facade_.txn_scan(req->shard_id(), req->start_key(), req->end_key(), req->start_after(),
                              req->start_ts(), req->limit(), req->keys_only(), req->linearizable());
    resp->set_truncated(r.truncated);
    resp->set_next_key(r.next_key);
    resp->set_is_leader(r.is_leader);
    resp->set_redirect_to(r.redirect_to);
    if (!r.error.empty()) resp->set_error(r.error);
    for (const auto& p : r.pairs) {
      auto* out = resp->add_pairs();
      out->set_key(p.key);
      out->set_value(p.value);
      out->set_revision(p.revision);
    }
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* Ttl(CallbackServerContext* ctx, const pb::StoreTtlRequest* req,
                          pb::StoreTtlResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto r = facade_.txn_ttl(req->shard_id(), req->key(), req->start_ts(), req->linearizable());
    resp->set_ttl_ms(r.ttl_ms);
    resp->set_locked(r.locked);
    resp->set_is_leader(r.is_leader);
    resp->set_redirect_to(r.redirect_to);
    if (r.locked) {
      resp->set_lock_primary(r.blocking_lock.primary);
      resp->set_lock_ts(r.blocking_lock.start_ts);
    }
    if (!r.error.empty()) resp->set_error(r.error);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* CheckTxnStatus(CallbackServerContext* ctx,
                                     const pb::StoreCheckTxnRequest* req,
                                     pb::StoreCheckTxnResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto r = facade_.check_txn_status(req->shard_id(), req->primary(), req->lock_ts());
    resp->set_is_leader(r.is_leader);
    resp->set_redirect_to(r.redirect_to);
    resp->set_commit_ts(r.commit_ts);
    if (!r.error.empty()) resp->set_error(r.error);
    // locked|expired|committed|rolledback
    const char* st = "rolledback";
    if (r.status == TxnStatus::kLocked) st = r.lock_expired ? "expired" : "locked";
    else if (r.status == TxnStatus::kCommitted) st = "committed";
    resp->set_status(st);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* WatchHead(CallbackServerContext* ctx, const pb::StoreWatchHeadRequest* req,
                                pb::StoreWatchHeadResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto* g = facade_.node().multi_raft().get(req->shard_id());
    if (!g) {
      resp->set_redirect_to("no_region");
      resp->set_error("no_region");
    } else {
      resp->set_revision(g->soft_state().last_applied);
    }
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* WatchPoll(CallbackServerContext* ctx, const pb::StoreWatchPollRequest* req,
                                pb::StoreWatchPollResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    const uint32_t shard = req->shard_id();
    const uint64_t since = req->since();
    size_t limit = req->limit() > 0 ? static_cast<size_t>(req->limit()) : 64;
    auto* g = facade_.node().multi_raft().get(shard);
    if (!g) {
      resp->set_redirect_to("no_region");
      resp->set_error("no_region");
      resp->set_max_revision(since);
      reactor->Finish(Status::OK);
      return reactor;
    }
    auto& eng = facade_.node().engines().get_or_create(shard);
    auto evs = WatchLog::poll(eng, shard, since, limit);
    if (evs.empty()) {
      evs = facade_.node().watch_hub().poll(shard, since, limit);
    }
    uint64_t max_rev = since;
    for (const auto& e : evs) {
      if (e.revision > max_rev) max_rev = e.revision;
      auto* out = resp->add_events();
      out->set_key(e.key);
      out->set_value(e.value);
      out->set_revision(e.revision);
      out->set_deleted(e.deleted);
    }
    resp->set_max_revision(max_rev);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* TransferLeadership(CallbackServerContext* ctx, const pb::TransferRequest* req,
                                         pb::TransferResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    if (!grpc_admin_allowed(*ctx, token_)) {
      reactor->Finish(Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized"));
      return reactor;
    }
    auto msg = facade_.node().admin_transfer_leader(req->shard_id(), req->target_peer());
    resp->set_ok(proto::starts_with_ok(msg));
    if (!resp->ok()) {
      resp->set_error(msg);
      resp->set_redirect_to(extract_redirect(msg));
    }
    reactor->Finish(Status::OK);
    return reactor;
  }

 private:
  StoreFacade& facade_;
  std::string token_;
};

void parse_vote_line(std::string_view line, pb::RequestVoteResponse* resp) {
  resp->set_vote_granted(false);
  resp->set_term(0);
  if (!proto::starts_with_ok(line)) return;
  std::istringstream iss{std::string(line)};
  std::string ok, tag;
  uint64_t term = 0;
  int granted = 0;
  if (iss >> ok >> tag >> term >> granted) {
    resp->set_term(term);
    resp->set_vote_granted(granted != 0);
  }
}

void parse_ae_line(std::string_view line, pb::AppendEntriesResponse* resp) {
  resp->set_success(false);
  resp->set_term(0);
  resp->set_match_index(0);
  std::istringstream iss{std::string(line)};
  std::string a, b;
  uint64_t term = 0, match = 0;
  int success = 0;
  if (iss >> a >> b >> term >> success >> match) {
    resp->set_term(term);
    resp->set_success(success != 0);
    resp->set_match_index(match);
  }
}

void fill_install_snapshot_resp(std::string_view line, uint64_t term, uint64_t last_idx,
                                pb::InstallSnapshotResponse* resp) {
  std::istringstream iss{std::string(line)};
  std::string ok, tag;
  uint64_t out_term = term;
  int success = 0;
  uint64_t match = last_idx;
  if ((iss >> ok >> tag >> out_term >> success >> match) && ok == "OK") {
    resp->set_ok(success != 0);
    resp->set_term(out_term);
    resp->set_match_index(match);
  } else {
    resp->set_ok(proto::starts_with_ok(line));
    resp->set_term(term);
    if (resp->ok()) resp->set_match_index(last_idx);
  }
}

class InstallSnapshotReactor final : public ServerReadReactor<pb::SnapshotChunk> {
 public:
  InstallSnapshotReactor(StorageNode& node, pb::InstallSnapshotResponse* resp)
      : node_(node), resp_(resp) {
    StartRead(&chunk_);
  }

  void OnReadDone(bool ok) override {
    if (ok) {
      shard_ = chunk_.shard_id();
      term_ = chunk_.term();
      from_ = chunk_.leader_id();
      last_idx_ = chunk_.last_included_index();
      last_term_ = chunk_.last_included_term();
      buf_.append(chunk_.data());
      if (chunk_.done()) {
        apply_and_finish();
        return;
      }
      StartRead(&chunk_);
      return;
    }
    apply_and_finish();
  }

  void OnDone() override { delete this; }

 private:
  void apply_and_finish() {
    auto line = node_.handle_raft_snapshot(shard_, from_, term_, last_idx_, last_term_, buf_);
    fill_install_snapshot_resp(line, term_, last_idx_, resp_);
    Finish(Status::OK);
  }

  StorageNode& node_;
  pb::InstallSnapshotResponse* resp_;
  pb::SnapshotChunk chunk_;
  std::string buf_;
  std::string from_;
  uint32_t shard_{0};
  uint64_t term_{0};
  uint64_t last_idx_{0};
  uint64_t last_term_{0};
};

class RaftServiceImpl final : public pb::RaftService::CallbackService {
 public:
  explicit RaftServiceImpl(StorageNode& n) : node_(n) {}

  ServerUnaryReactor* AppendEntries(CallbackServerContext* ctx, const pb::AppendEntriesRequest* req,
                                    pb::AppendEntriesResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    std::vector<raft::LogEntry> ents;
    ents.reserve(req->entries_size());
    for (const auto& e : req->entries()) {
      ents.push_back(raft::LogEntry{e.term(), e.index(), e.data()});
    }
    auto line = node_.handle_raft_append(req->shard_id(), req->leader_id(), req->term(),
                                         req->prev_log_index(), req->prev_log_term(),
                                         req->leader_commit(), ents);
    parse_ae_line(line, resp);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* RequestVote(CallbackServerContext* ctx, const pb::RequestVoteRequest* req,
                                  pb::RequestVoteResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto line = node_.handle_raft_vote(req->shard_id(), req->candidate_id(), req->term(),
                                       req->last_log_index(), req->last_log_term());
    parse_vote_line(line, resp);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* RequestPreVote(CallbackServerContext* ctx, const pb::RequestVoteRequest* req,
                                     pb::RequestVoteResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    auto line = node_.handle_raft_pre_vote(req->shard_id(), req->candidate_id(), req->term(),
                                           req->last_log_index(), req->last_log_term());
    parse_vote_line(line, resp);
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerReadReactor<pb::SnapshotChunk>* InstallSnapshot(CallbackServerContext*,
                                                        pb::InstallSnapshotResponse* resp) override {
    return new InstallSnapshotReactor(node_, resp);
  }

 private:
  StorageNode& node_;
};

pb::AdminSimpleResponse make_admin_resp(std::string_view msg) {
  pb::AdminSimpleResponse r;
  r.set_ok(proto::starts_with_ok(msg));
  r.set_message(std::string(msg));
  r.set_redirect_to(extract_redirect(msg));
  return r;
}

class AdminServiceImpl final : public pb::AdminService::CallbackService {
 public:
  AdminServiceImpl(StorageNode& n, std::string token) : node_(n), token_(std::move(token)) {}

  Status ensure_auth(CallbackServerContext* ctx) {
    if (!grpc_admin_allowed(*ctx, token_)) {
      return Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized");
    }
    return Status::OK;
  }

  /// Auth → run body → Finish(OK)。失败时已 Finish，调用方直接 return reactor。
  template <typename Fn>
  ServerUnaryReactor* with_auth(CallbackServerContext* ctx, Fn&& fn) {
    auto* reactor = ctx->DefaultReactor();
    if (auto st = ensure_auth(ctx); !st.ok()) {
      reactor->Finish(st);
      return reactor;
    }
    fn();
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* EnsureRegion(CallbackServerContext* ctx, const pb::EnsureRegionRequest* req,
                                   pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      std::vector<raft::Peer> peers;
      for (const auto& p : req->peers()) {
        peers.push_back(raft::Peer{p.node_id(), p.address(), p.learner()});
      }
      *resp = make_admin_resp(node_.admin_ensure_region(req->region_id(), peers));
    });
  }

  ServerUnaryReactor* AddLearner(CallbackServerContext* ctx, const pb::PeerOpRequest* req,
                                 pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_add_learner(
          req->region_id(), raft::Peer{req->node_id(), req->address(), true}));
    });
  }

  ServerUnaryReactor* Promote(CallbackServerContext* ctx, const pb::PeerOpRequest* req,
                              pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_promote(req->region_id(), req->node_id()));
    });
  }

  ServerUnaryReactor* RemovePeer(CallbackServerContext* ctx, const pb::PeerOpRequest* req,
                                 pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_remove_peer(req->region_id(), req->node_id()));
    });
  }

  ServerUnaryReactor* DestroyRegion(CallbackServerContext* ctx, const pb::DestroyRegionRequest* req,
                                    pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_destroy_region(req->region_id()));
    });
  }

  ServerUnaryReactor* PeerProgress(CallbackServerContext* ctx, const pb::PeerOpRequest* req,
                                   pb::PeerProgressResponse* resp) override {
    return with_auth(ctx, [&] {
      auto msg = node_.admin_peer_progress(req->region_id(), req->node_id());
      resp->set_ok(proto::starts_with_ok(msg));
      resp->set_message(msg);
      resp->set_redirect_to(extract_redirect(msg));
      std::istringstream iss(msg);
      std::string ok, tag;
      uint64_t match = 0, commit = 0, last = 0;
      int caught = 0, learner = 0;
      if (iss >> ok >> tag >> match >> commit >> last >> caught >> learner) {
        resp->set_match_index(match);
        resp->set_commit_index(commit);
        resp->set_last_index(last);
        resp->set_caught_up(caught != 0);
        resp->set_is_learner(learner != 0);
      }
    });
  }

  ServerUnaryReactor* TransferLeader(CallbackServerContext* ctx, const pb::TransferRequest* req,
                                     pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_transfer_leader(req->shard_id(), req->target_peer()));
    });
  }

  ServerUnaryReactor* Campaign(CallbackServerContext* ctx, const pb::CampaignRequest* req,
                               pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_campaign(req->region_id()));
    });
  }

  ServerUnaryReactor* PlaceRegion(CallbackServerContext* ctx, const pb::PlaceRegionRequest* req,
                                  pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      std::vector<std::string> peers(req->desired_peers().begin(), req->desired_peers().end());
      *resp = make_admin_resp(node_.admin_place_region(req->region_id(), peers));
    });
  }

  ServerUnaryReactor* PrepareSplit(CallbackServerContext* ctx, const pb::PrepareSplitRequest* req,
                                   pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_prepare_split(req->left_region(), req->split_key()));
    });
  }

  ServerUnaryReactor* Split(CallbackServerContext* ctx, const pb::SplitRequest* req,
                            pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(
          node_.admin_split(req->left_region(), req->right_region(), req->split_key()));
    });
  }

  ServerUnaryReactor* Merge(CallbackServerContext* ctx, const pb::MergeRequest* req,
                            pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_merge(req->left_region(), req->right_region()));
    });
  }

  ServerUnaryReactor* SetEpoch(CallbackServerContext* ctx, const pb::SetEpochRequest* req,
                               pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_set_epoch(req->region_id(), req->epoch()));
    });
  }

  ServerUnaryReactor* Checkpoint(CallbackServerContext* ctx, const pb::CheckpointRequest* req,
                                 pb::AdminSimpleResponse* resp) override {
    return with_auth(ctx, [&] {
      *resp = make_admin_resp(node_.admin_checkpoint(req->region_id(), req->path()));
    });
  }

  ServerUnaryReactor* RegionStats(CallbackServerContext* ctx, const pb::RegionStatsRequest* req,
                                  pb::RegionStatsResponse* resp) override {
    return with_auth(ctx, [&] {
      resp->set_ok(true);
      resp->set_message("OK");
      resp->set_epoch(node_.region_epoch(req->region_id()));
      if (auto* eng = node_.engines().get(req->region_id())) {
        resp->set_key_count(eng->approx_key_count());
        resp->set_approx_bytes(eng->approx_bytes());
      }
    });
  }

  ServerUnaryReactor* Ping(CallbackServerContext* ctx, const pb::PingRequest*,
                           pb::PingResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    resp->set_message("PONG");
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerUnaryReactor* Stats(CallbackServerContext* ctx, const pb::StatsRequest*,
                            pb::StatsResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    const auto snap = node_.stats_snapshot();
    resp->set_requests(snap.requests);
    resp->set_gw(snap.gw);
    resp->set_raft(snap.raft);
    resp->set_sync(snap.sync);
    resp->set_admin(snap.admin);
    resp->set_meta_out(snap.meta_out);
    resp->set_client(snap.client);
    resp->set_qdepth(snap.qdepth);
    resp->set_qdrop(snap.qdrop);
    for (const auto& kv : snap.peer_raft_out) (*resp->mutable_peer_raft_out())[kv.first] = kv.second;
    for (const auto& kv : snap.peer_sync_out) (*resp->mutable_peer_sync_out())[kv.first] = kv.second;
    for (const auto& kv : snap.meta_out_peers) (*resp->mutable_meta_out_peers())[kv.first] = kv.second;
    reactor->Finish(Status::OK);
    return reactor;
  }

 private:
  StorageNode& node_;
  std::string token_;
};

}  // namespace

struct StorageGrpcServer::Impl {
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<StoreServiceImpl> store;
  std::unique_ptr<RaftServiceImpl> raft;
  std::unique_ptr<AdminServiceImpl> admin;
};

StorageGrpcServer::StorageGrpcServer(StoreFacade& facade, std::string listen_addr)
    : facade_(facade),
      listen_addr_(std::move(listen_addr)),
      admin_token_(admin_token_from_env()),
      impl_(std::make_unique<Impl>()) {}

StorageGrpcServer::~StorageGrpcServer() { stop(); }

void StorageGrpcServer::start() {
  if (running_.exchange(true)) return;
  // Store/Admin/Raft 均为 CallbackService：热路径 Finish@apply / reactor，无 WorkPool。
  impl_->store = std::make_unique<StoreServiceImpl>(facade_, admin_token_);
  impl_->raft = std::make_unique<RaftServiceImpl>(facade_.node());
  impl_->admin = std::make_unique<AdminServiceImpl>(facade_.node(), admin_token_);

  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(64 << 20);
  builder.SetMaxSendMessageSize(64 << 20);
  builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials());
  builder.RegisterService(impl_->store.get());
  builder.RegisterService(impl_->raft.get());
  builder.RegisterService(impl_->admin.get());
  impl_->server = builder.BuildAndStart();
  if (!impl_->server) {
    running_ = false;
    throw std::runtime_error("StorageGrpcServer failed to listen on " + listen_addr_);
  }
  SKV_INFO << "storage gRPC listening " << listen_addr_ << " (callback)";
}

void StorageGrpcServer::stop() {
  if (!running_.exchange(false)) return;
  // 先拒新 RPC 并排空在途（apply 回调仍可 Finish）；再拆 service。
  if (impl_ && impl_->server) {
    openkv::graceful_grpc_shutdown(impl_->server);
    impl_->server.reset();
  }
  impl_->admin.reset();
  impl_->store.reset();
  impl_->raft.reset();
}

}  // namespace openkv::storage
