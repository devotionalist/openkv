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
// Storage gRPC Server 实现（强制 gRPC）。

#include "opskv/storage/grpc_server.hpp"

#include "opskv/admin_acl.hpp"
#include "opskv/grpc_auth.hpp"
#include "opskv/logging.hpp"
#include "opskv/protocol.hpp"
#include "opskv/storage/storage_node.hpp"

#include <sstream>
#include <stdexcept>
#include <utility>

#include <grpcpp/grpcpp.h>
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace opskv::storage {
namespace {

std::string extract_redirect(std::string_view s) {
  auto p = s.find("redirect=");
  if (p == std::string_view::npos) return {};
  auto rest = s.substr(p + 9);
  auto sp = rest.find_first_of(" \t\r\n");
  if (sp != std::string_view::npos) rest = rest.substr(0, sp);
  return std::string(rest);
}

}  // namespace


namespace {

using grpc::ServerContext;
using grpc::Status;
namespace pb = ::storage::v1;

class StoreServiceImpl final : public pb::StoreService::Service {
 public:
  StoreServiceImpl(StoreFacade& f, std::string token) : facade_(f), token_(std::move(token)) {}

  Status Get(ServerContext*, const pb::StoreGetRequest* req,
             pb::StoreGetResponse* resp) override {
    auto r = facade_.get(req->shard_id(), req->key(), req->linearizable());
    resp->set_found(r.found);
    resp->set_value(r.value);
    resp->set_revision(r.revision);
    resp->set_is_leader(r.is_leader);
    resp->set_redirect_to(r.redirect_to);
    return Status::OK;
  }

  Status Propose(ServerContext*, const pb::ProposeRequest* req,
                 pb::ProposeResponse* resp) override {
    Command cmd;
    cmd.op = req->op() == pb::ProposeRequest::DELETE ? CmdOp::kDelete : CmdOp::kPut;
    cmd.key = req->key();
    cmd.value = req->value();
    cmd.ttl_ms = req->ttl_ms();
    cmd.request_id = req->request_id();
    auto r = facade_.propose(req->shard_id(), cmd);
    resp->set_ok(r.ok);
    resp->set_revision(r.revision);
    resp->set_error(r.error);
    resp->set_redirect_to(r.redirect_to);
    return Status::OK;
  }

  Status TransferLeadership(ServerContext* ctx, const pb::TransferRequest* req,
                             pb::TransferResponse* resp) override {
    if (!grpc_admin_allowed(*ctx, token_)) {
      return Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized");
    }
    auto msg = facade_.node().admin_transfer_leader(req->shard_id(), req->target_peer());
    resp->set_ok(proto::starts_with_ok(msg));
    if (!resp->ok()) {
      resp->set_error(msg);
      resp->set_redirect_to(extract_redirect(msg));
    }
    return Status::OK;
  }

 private:
  StoreFacade& facade_;
  std::string token_;
};

void parse_vote_line(std::string_view line, pb::RequestVoteResponse* resp) {
  // OK VOTE term granted | ERR ...
  resp->set_vote_granted(false);
  resp->set_term(0);
  if (!proto::starts_with_ok(line)) return;
  // "OK VOTE <term> <0|1>" roughly — see exec_line_handler / storage handlers
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
  if (!proto::starts_with_ok(line)) {
    // still try parse term
  }
  std::istringstream iss{std::string(line)};
  std::string a, b;
  uint64_t term = 0, match = 0;
  int success = 0;
  // OK AE term success match
  if (iss >> a >> b >> term >> success >> match) {
    resp->set_term(term);
    resp->set_success(success != 0);
    resp->set_match_index(match);
  }
}

class RaftServiceImpl final : public pb::RaftService::Service {
 public:
  explicit RaftServiceImpl(StorageNode& n) : node_(n) {}

  Status AppendEntries(ServerContext*, const pb::AppendEntriesRequest* req,
                       pb::AppendEntriesResponse* resp) override {
    std::vector<raft::LogEntry> ents;
    ents.reserve(req->entries_size());
    for (const auto& e : req->entries()) {
      ents.push_back(raft::LogEntry{e.term(), e.index(), e.data()});
    }
    auto line = node_.handle_raft_append(req->shard_id(), req->leader_id(), req->term(),
                                         req->prev_log_index(), req->prev_log_term(),
                                         req->leader_commit(), ents);
    parse_ae_line(line, resp);
    return Status::OK;
  }

  Status RequestVote(ServerContext*, const pb::RequestVoteRequest* req,
                     pb::RequestVoteResponse* resp) override {
    auto line = node_.handle_raft_vote(req->shard_id(), req->candidate_id(), req->term(),
                                       req->last_log_index(), req->last_log_term());
    parse_vote_line(line, resp);
    return Status::OK;
  }

  Status RequestPreVote(ServerContext*, const pb::RequestVoteRequest* req,
                        pb::RequestVoteResponse* resp) override {
    auto line = node_.handle_raft_pre_vote(req->shard_id(), req->candidate_id(), req->term(),
                                           req->last_log_index(), req->last_log_term());
    parse_vote_line(line, resp);
    return Status::OK;
  }

  Status InstallSnapshot(ServerContext*, grpc::ServerReader<pb::SnapshotChunk>* reader,
                         pb::InstallSnapshotResponse* resp) override {
    pb::SnapshotChunk chunk;
    std::string buf;
    uint32_t shard = 0;
    uint64_t term = 0, last_idx = 0, last_term = 0;
    std::string from;
    while (reader->Read(&chunk)) {
      shard = chunk.shard_id();
      term = chunk.term();
      from = chunk.leader_id();
      last_idx = chunk.last_included_index();
      last_term = chunk.last_included_term();
      buf.append(chunk.data());
      if (chunk.done()) break;
    }
    auto line = node_.handle_raft_snapshot(shard, from, term, last_idx, last_term, buf);
    // OK SNAP <term> <success> <match>
    std::istringstream iss(line);
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
    return Status::OK;
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

class AdminServiceImpl final : public pb::AdminService::Service {
 public:
  AdminServiceImpl(StorageNode& n, ExecLineHandler* line, std::string token)
      : node_(n), line_(line), token_(std::move(token)) {}

  Status ensure_auth(ServerContext* ctx) {
    if (!grpc_admin_allowed(*ctx, token_)) {
      return Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized");
    }
    return Status::OK;
  }

  Status EnsureRegion(ServerContext* ctx, const pb::EnsureRegionRequest* req,
                      pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    std::vector<raft::Peer> peers;
    for (const auto& p : req->peers()) {
      peers.push_back(raft::Peer{p.node_id(), p.address(), p.learner()});
    }
    *resp = make_admin_resp(node_.admin_ensure_region(req->region_id(), peers));
    return Status::OK;
  }

  Status AddLearner(ServerContext* ctx, const pb::PeerOpRequest* req,
                    pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(
        node_.admin_add_learner(req->region_id(), raft::Peer{req->node_id(), req->address(), true}));
    return Status::OK;
  }

  Status Promote(ServerContext* ctx, const pb::PeerOpRequest* req,
                 pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_promote(req->region_id(), req->node_id()));
    return Status::OK;
  }

  Status RemovePeer(ServerContext* ctx, const pb::PeerOpRequest* req,
                    pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_remove_peer(req->region_id(), req->node_id()));
    return Status::OK;
  }

  Status DestroyRegion(ServerContext* ctx, const pb::DestroyRegionRequest* req,
                       pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_destroy_region(req->region_id()));
    return Status::OK;
  }

  Status PeerProgress(ServerContext* ctx, const pb::PeerOpRequest* req,
                      pb::PeerProgressResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    auto msg = node_.admin_peer_progress(req->region_id(), req->node_id());
    resp->set_ok(proto::starts_with_ok(msg));
    resp->set_message(msg);
    resp->set_redirect_to(extract_redirect(msg));
    // OK PROGRESS match commit last caught learner
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
    return Status::OK;
  }

  Status TransferLeader(ServerContext* ctx, const pb::TransferRequest* req,
                        pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_transfer_leader(req->shard_id(), req->target_peer()));
    return Status::OK;
  }

  Status Campaign(ServerContext* ctx, const pb::CampaignRequest* req,
                  pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_campaign(req->region_id()));
    return Status::OK;
  }

  Status PlaceRegion(ServerContext* ctx, const pb::PlaceRegionRequest* req,
                     pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    std::vector<std::string> peers(req->desired_peers().begin(), req->desired_peers().end());
    *resp = make_admin_resp(node_.admin_place_region(req->region_id(), peers));
    return Status::OK;
  }

  Status PrepareSplit(ServerContext* ctx, const pb::PrepareSplitRequest* req,
                      pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_prepare_split(req->left_region(), req->split_key()));
    return Status::OK;
  }

  Status Split(ServerContext* ctx, const pb::SplitRequest* req,
               pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(
        node_.admin_split(req->left_region(), req->right_region(), req->split_key()));
    return Status::OK;
  }

  Status Merge(ServerContext* ctx, const pb::MergeRequest* req,
               pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_merge(req->left_region(), req->right_region()));
    return Status::OK;
  }

  Status SetEpoch(ServerContext* ctx, const pb::SetEpochRequest* req,
                  pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_set_epoch(req->region_id(), req->epoch()));
    return Status::OK;
  }

  Status Checkpoint(ServerContext* ctx, const pb::CheckpointRequest* req,
                    pb::AdminSimpleResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    *resp = make_admin_resp(node_.admin_checkpoint(req->region_id(), req->path()));
    return Status::OK;
  }

  Status RegionStats(ServerContext* ctx, const pb::RegionStatsRequest* req,
                     pb::RegionStatsResponse* resp) override {
    if (auto st = ensure_auth(ctx); !st.ok()) return st;
    (void)req;
    resp->set_ok(true);
    resp->set_message("OK");
    resp->set_epoch(node_.region_epoch(req->region_id()));
    if (auto* eng = node_.engines().get(req->region_id())) {
      resp->set_key_count(eng->approx_key_count());
      resp->set_approx_bytes(eng->approx_bytes());
    }
    return Status::OK;
  }

  Status Ping(ServerContext*, const pb::PingRequest*, pb::PingResponse* resp) override {
    resp->set_message("PONG");
    return Status::OK;
  }

  Status ExecLine(ServerContext* ctx, const pb::ExecLineRequest* req,
                  pb::ExecLineResponse* resp) override {
    if (!line_) {
      return Status(grpc::StatusCode::UNIMPLEMENTED, "ExecLine: no line handler");
    }
    // ADMIN_* / AUTH 走 token；普通 KV/Txn 行不强制 metadata。
    const auto& line = req->line();
    if (line.rfind("ADMIN_", 0) == 0 || line.rfind("AUTH ", 0) == 0) {
      if (auto st = ensure_auth(ctx); !st.ok()) return st;
    }
    AdminSession sess;
    if (admin_acl_enabled(token_)) {
      // metadata 已校验时视为已认证会话
      const auto& md = ctx->client_metadata();
      if (md.find(kGrpcAdminTokenMeta) != md.end()) {
        sess.authenticated = true;
      }
    }
    resp->set_body(line_->process_line(line, &sess));
    return Status::OK;
  }

 private:
  StorageNode& node_;
  ExecLineHandler* line_;
  std::string token_;
};

}  // namespace

struct StorageGrpcServer::Impl {
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<StoreServiceImpl> store;
  std::unique_ptr<RaftServiceImpl> raft;
  std::unique_ptr<AdminServiceImpl> admin;
};

StorageGrpcServer::StorageGrpcServer(StoreFacade& facade, std::string listen_addr,
                                     ExecLineHandler* line_server)
    : facade_(facade),
      line_server_(line_server),
      listen_addr_(std::move(listen_addr)),
      admin_token_(admin_token_from_env()),
      impl_(std::make_unique<Impl>()) {}

StorageGrpcServer::~StorageGrpcServer() { stop(); }

void StorageGrpcServer::start() {
  if (running_.exchange(true)) return;
  impl_->store = std::make_unique<StoreServiceImpl>(facade_, admin_token_);
  impl_->raft = std::make_unique<RaftServiceImpl>(facade_.node());
  impl_->admin = std::make_unique<AdminServiceImpl>(facade_.node(), line_server_, admin_token_);

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
  SKV_INFO << "storage gRPC listening " << listen_addr_;
}

void StorageGrpcServer::stop() {
  if (!running_.exchange(false)) return;
  if (impl_ && impl_->server) {
    impl_->server->Shutdown();
    impl_->server.reset();
  }
}

}  // namespace opskv::storage
