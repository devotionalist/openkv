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
// Meta gRPC：CallbackService + WorkPool；GetTs/ExecLine(GET_TS) 走 alloc_ts_async。

#include "openkv/meta/grpc_server.hpp"

#include "openkv/admin_acl.hpp"
#include "openkv/codec.hpp"
#include "openkv/grpc_auth.hpp"
#include "openkv/logging.hpp"
#include "openkv/protocol.hpp"
#include "openkv/raft/raft_node.hpp"
#include "openkv/work_pool.hpp"

#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include <grpcpp/grpcpp.h>
#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace openkv::meta {

namespace {

using grpc::CallbackServerContext;
using grpc::ServerContext;
using grpc::ServerUnaryReactor;
using grpc::ServerWriteReactor;
using grpc::Status;
namespace pb = ::meta::v1;

bool line_ok(std::string_view s) { return proto::starts_with_ok(s); }

void fill_ts_response(pb::GetTsResponse* resp, std::string_view line) {
  std::istringstream iss{std::string(line)};
  std::string ok, tag;
  uint64_t ts = 0;
  if (iss >> ok >> tag >> ts) resp->set_ts(ts);
}

bool line_is_get_ts(std::string_view line) {
  return line == "GET_TS" || line.rfind("GET_TS ", 0) == 0;
}

uint32_t parse_get_ts_count(std::string_view line) {
  if (line == "GET_TS") return 1;
  std::istringstream iss{std::string(line)};
  std::string op;
  uint32_t count = 1;
  if (iss >> op >> count) {
    if (count == 0) count = 1;
    if (count > 10000) count = 10000;
    return count;
  }
  return 1;
}

class MetaServiceImpl final : public pb::MetaService::CallbackService {
 public:
  MetaServiceImpl(MetaNode& m, std::string token, openkv::WorkPool& pool)
      : meta_(m), token_(std::move(token)), pool_(pool) {}

  Status auth_write(CallbackServerContext* ctx) {
    if (!grpc_admin_allowed(*ctx, token_)) {
      return Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized");
    }
    return Status::OK;
  }

  /// Auth early-return，否则把 body 丢到 WorkPool 再 Finish。
  template <typename Fn>
  ServerUnaryReactor* defer(CallbackServerContext* ctx, bool need_auth, Fn&& fn) {
    auto* reactor = ctx->DefaultReactor();
    if (need_auth) {
      if (auto st = auth_write(ctx); !st.ok()) {
        reactor->Finish(st);
        return reactor;
      }
    }
    pool_.post([reactor, fn = std::forward<Fn>(fn)]() mutable {
      fn();
      reactor->Finish(Status::OK);
    });
    return reactor;
  }

  ServerUnaryReactor* Ping(CallbackServerContext* ctx, const pb::PingRequest*,
                           pb::PingResponse* resp) override {
    return defer(ctx, false, [this, resp] { resp->set_message(meta_.handle_line("PING")); });
  }

  ServerUnaryReactor* RegisterNode(CallbackServerContext* ctx, const pb::RegisterNodeRequest* req,
                                   pb::RegisterNodeResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      const auto& n = req->node();
      std::ostringstream oss;
      oss << "REGISTER " << n.node_id() << " " << n.address() << " STORAGE";
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      resp->set_assigned_id(n.node_id());
      if (!resp->ok()) resp->set_error(line);
    });
  }

  ServerUnaryReactor* Heartbeat(CallbackServerContext* ctx, const pb::HeartbeatRequest* req,
                                pb::HeartbeatResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      std::ostringstream oss;
      oss << "HEARTBEAT " << req->node_id();
      for (uint32_t s : req->leader_shards()) oss << " " << s;
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      if (!resp->ok()) resp->set_error(line);
      std::string err, redir;
      auto rt = meta_.routing_table(&err, &redir);
      resp->set_routing_version(rt.version);
    });
  }

  void fill_routing_table(pb::RoutingTable* resp) {
    auto line = meta_.handle_line("GET_ROUTE");
    std::string err, redir;
    auto rt = meta_.routing_table(&err, &redir);
    resp->set_version(rt.version);
    resp->set_shard_count(static_cast<uint32_t>(rt.regions.size()));
    for (const auto& region : rt.regions) {
      auto* sh = resp->add_shards();
      sh->set_shard_id(region.region_id);
      sh->set_raft_group_id(region.region_id);
      sh->set_start_key(region.range.start_key);
      sh->set_end_key(region.range.end_key);
      sh->set_epoch(region.epoch);
      for (const auto& r : region.replicas) {
        auto* rp = sh->add_replicas();
        rp->set_node_id(r.node_id);
        rp->set_address(r.address);
        rp->set_is_leader(r.is_leader);
        rp->set_is_learner(r.is_learner);
      }
    }
    (void)line;
  }

  ServerUnaryReactor* GetRoutingTable(CallbackServerContext* ctx, const pb::GetRoutingTableRequest*,
                                      pb::RoutingTable* resp) override {
    return defer(ctx, false, [this, resp] { fill_routing_table(resp); });
  }

  ServerWriteReactor<pb::RoutingTable>* WatchRoutingTable(
      CallbackServerContext*, const pb::WatchRoutingRequest* req) override {
    // WATCH_ROUTE 可能长等 route_cv_：丢到 WorkPool，勿堵 callback 接收线程。
    class Reactor final : public ServerWriteReactor<pb::RoutingTable> {
     public:
      pb::RoutingTable tbl;
      void OnWriteDone(bool) override { Finish(Status::OK); }
      void OnDone() override { delete this; }
    };
    auto* reactor = new Reactor();
    const uint64_t from_version = req->from_version();
    const int timeout_ms = req->timeout_ms();
    pool_.post([this, reactor, from_version, timeout_ms] {
      std::ostringstream oss;
      oss << "WATCH_ROUTE " << from_version;
      if (timeout_ms > 0) oss << " " << timeout_ms;
      meta_.handle_line(oss.str());
      fill_routing_table(&reactor->tbl);
      reactor->StartWrite(&reactor->tbl);
    });
    return reactor;
  }

  ServerUnaryReactor* ScaleOut(CallbackServerContext* ctx, const pb::ScaleOutRequest* req,
                               pb::ScaleOutResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      std::ostringstream oss;
      oss << "SCALE_OUT " << req->new_storage_node_id();
      if (!req->shard_ids().empty()) {
        oss << " ";
        for (int i = 0; i < req->shard_ids_size(); ++i) {
          if (i) oss << ",";
          oss << req->shard_ids(i);
        }
      }
      auto line = meta_.handle_line(oss.str());
      resp->set_accepted(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* GetScaleTask(CallbackServerContext* ctx, const pb::GetScaleTaskRequest* req,
                                   pb::ScaleTask* resp) override {
    return defer(ctx, false, [this, req, resp] {
      auto line = meta_.handle_line("GET_SCALE " + req->task_id());
      resp->set_task_id(req->task_id());
      resp->set_error(line_ok(line) ? "" : line);
    });
  }

  ServerUnaryReactor* ListScaleTasks(CallbackServerContext* ctx, const pb::ListScaleTasksRequest*,
                                     pb::ListScaleTasksResponse*) override {
    return defer(ctx, false, [this] { meta_.handle_line("LIST_SCALE"); });
  }

  ServerUnaryReactor* ListNodes(CallbackServerContext* ctx, const pb::ListNodesRequest*,
                                pb::ListNodesResponse* resp) override {
    return defer(ctx, false, [this, resp] {
      meta_.handle_line("LIST_NODES");
      (void)resp;
    });
  }

#define META_NODE_RPC(Name, Op)                                                                   \
  ServerUnaryReactor* Name(CallbackServerContext* ctx, const pb::NodeIdRequest* req,              \
                           pb::SimpleResponse* resp) override {                                   \
    return defer(ctx, true, [this, req, resp] {                                                   \
      auto line = meta_.handle_line(std::string(Op) + " " + req->node_id());                      \
      resp->set_ok(line_ok(line));                                                                \
      resp->set_message(line);                                                                    \
      if (!resp->ok()) resp->set_error(line);                                                     \
    });                                                                                           \
  }

  META_NODE_RPC(DrainNode, "DRAIN_NODE")
  META_NODE_RPC(ActivateNode, "ACTIVATE_NODE")
  META_NODE_RPC(OfflineNode, "OFFLINE_NODE")
  META_NODE_RPC(JoinNode, "JOIN_NODE")
  META_NODE_RPC(KickNode, "KICK_NODE")
  META_NODE_RPC(RemoveTombstone, "REMOVE_TOMBSTONE")
  META_NODE_RPC(StoreDelete, "STORE_DELETE")
#undef META_NODE_RPC

  ServerUnaryReactor* SplitRegion(CallbackServerContext* ctx, const pb::SplitRegionRequest* req,
                                  pb::SplitRegionResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      std::ostringstream oss;
      oss << "SPLIT_REGION " << req->region_id() << " " << openkv::to_hex(req->split_key());
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      if (!resp->ok()) resp->set_error(line);
    });
  }

  ServerUnaryReactor* MergeRegions(CallbackServerContext* ctx, const pb::MergeRegionsRequest* req,
                                   pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      std::ostringstream oss;
      oss << "MERGE_REGIONS " << req->left_region_id() << " " << req->right_region_id();
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* PlaceRegion(CallbackServerContext* ctx, const pb::PlaceRegionRequest* req,
                                  pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      std::ostringstream oss;
      oss << "PLACE_REGION " << req->region_id() << " ";
      for (int i = 0; i < req->peer_node_ids_size(); ++i) {
        if (i) oss << ",";
        oss << req->peer_node_ids(i);
      }
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* ScheduleMigrate(CallbackServerContext* ctx,
                                      const pb::ScheduleMigrateRequest* req,
                                      pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      std::ostringstream oss;
      oss << "SCHEDULE_MIGRATE " << req->region_id() << " " << req->from_node() << " "
          << req->to_node();
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* CancelMigrate(CallbackServerContext* ctx, const pb::CancelMigrateRequest* req,
                                    pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      auto line = meta_.handle_line("CANCEL_MIGRATE " + req->task_id());
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* ListMigrate(CallbackServerContext* ctx, const pb::ListMigrateRequest*,
                                  pb::ListMigrateResponse*) override {
    return defer(ctx, false, [this] { meta_.handle_line("LIST_MIGRATE"); });
  }

  ServerUnaryReactor* Balance(CallbackServerContext* ctx, const pb::BalanceRequest*,
                              pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, resp] {
      auto line = meta_.handle_line("BALANCE");
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* SuggestPlace(CallbackServerContext* ctx, const pb::SuggestPlaceRequest* req,
                                   pb::SuggestPlaceResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      auto line = meta_.handle_line("SUGGEST_PLACE " + std::to_string(req->count()));
      (void)line;
      (void)resp;
    });
  }

  ServerUnaryReactor* BumpRoute(CallbackServerContext* ctx, const pb::BumpRouteRequest*,
                                pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, resp] {
      auto line = meta_.handle_line("BUMP_ROUTE");
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  // 热路径：接收线程只发起 alloc_ts_async，apply 回调里 Finish。
  ServerUnaryReactor* GetTs(CallbackServerContext* ctx, const pb::GetTsRequest*,
                            pb::GetTsResponse* resp) override {
    auto* reactor = ctx->DefaultReactor();
    meta_.alloc_ts_async(1, [reactor, resp](std::string line) {
      fill_ts_response(resp, line);
      reactor->Finish(Status::OK);
    });
    return reactor;
  }

  ServerUnaryReactor* GetSafePoint(CallbackServerContext* ctx, const pb::GetSafePointRequest*,
                                   pb::GetSafePointResponse* resp) override {
    return defer(ctx, false, [this, resp] {
      auto line = meta_.handle_line("GET_SAFE_POINT");
      std::istringstream iss(line);
      std::string ok, tag;
      uint64_t sp = 0;
      if (iss >> ok >> tag >> sp) resp->set_safe_point(sp);
    });
  }

  ServerUnaryReactor* AdvanceSafePoint(CallbackServerContext* ctx,
                                       const pb::AdvanceSafePointRequest* req,
                                       pb::SimpleResponse* resp) override {
    return defer(ctx, true, [this, req, resp] {
      auto line = meta_.handle_line("ADVANCE_SAFE_POINT " + std::to_string(req->safe_point()));
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* ReportMinStartTs(CallbackServerContext* ctx,
                                       const pb::ReportMinStartTsRequest* req,
                                       pb::SimpleResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      std::ostringstream oss;
      oss << "REPORT_MIN_START_TS " << req->min_start_ts() << " " << req->node_id();
      auto line = meta_.handle_line(oss.str());
      resp->set_ok(line_ok(line));
      resp->set_message(line);
    });
  }

  ServerUnaryReactor* Locate(CallbackServerContext* ctx, const pb::LocateRequest* req,
                             pb::LocateResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      auto line = meta_.handle_line("LOCATE " + openkv::to_hex(req->key()));
      resp->set_ok(line_ok(line));
      if (!resp->ok()) resp->set_error(line);
    });
  }

  ServerUnaryReactor* WhoLeader(CallbackServerContext* ctx, const pb::WhoLeaderRequest*,
                                pb::WhoLeaderResponse* resp) override {
    return defer(ctx, false, [this, resp] {
      resp->set_address(meta_.leader_address());
      resp->set_leader_id(meta_.is_leader() ? meta_.bootstrap().node_id : "");
    });
  }

  ServerUnaryReactor* StoreStatus(CallbackServerContext* ctx, const pb::StoreStatusRequest* req,
                                  pb::StoreStatusResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      auto line = meta_.handle_line("STORE_STATUS " + req->node_id());
      resp->set_ok(line_ok(line));
      resp->set_status(line);
      if (!resp->ok()) resp->set_error(line);
    });
  }

  ServerUnaryReactor* FindMergeRight(CallbackServerContext* ctx,
                                     const pb::FindMergeRightRequest* req,
                                     pb::FindMergeRightResponse* resp) override {
    return defer(ctx, false, [this, req, resp] {
      auto line = meta_.handle_line("FIND_MERGE_RIGHT " + std::to_string(req->left_region_id()));
      resp->set_ok(line_ok(line));
      if (!resp->ok()) resp->set_error(line);
    });
  }

  ServerUnaryReactor* ExecLine(CallbackServerContext* ctx, const pb::ExecLineRequest* req,
                               pb::ExecLineResponse* resp) override {
    const auto& line = req->line();
    const bool need_auth =
        line.rfind("AUTH ", 0) == 0 || line.rfind("SCALE_", 0) == 0 ||
        line.rfind("DRAIN_", 0) == 0 || line.rfind("ACTIVATE_", 0) == 0 ||
        line.rfind("OFFLINE_", 0) == 0 || line.rfind("JOIN_", 0) == 0 ||
        line.rfind("KICK_", 0) == 0 || line.rfind("SPLIT_", 0) == 0 ||
        line.rfind("MERGE_", 0) == 0 || line.rfind("PLACE_", 0) == 0 ||
        line.rfind("SCHEDULE_", 0) == 0 || line.rfind("CANCEL_", 0) == 0 ||
        line.rfind("BALANCE", 0) == 0 || line.rfind("BUMP_", 0) == 0 ||
        line.rfind("ADVANCE_", 0) == 0 || line.rfind("REMOVE_", 0) == 0 ||
        line.rfind("STORE_DELETE", 0) == 0;
    if (need_auth) {
      if (auto st = auth_write(ctx); !st.ok()) {
        auto* reactor = ctx->DefaultReactor();
        reactor->Finish(st);
        return reactor;
      }
    }
    // GET_TS 热路径：接收线程只发起 alloc_ts_async。
    if (line_is_get_ts(line)) {
      auto* reactor = ctx->DefaultReactor();
      meta_.alloc_ts_async(parse_get_ts_count(line), [reactor, resp](std::string body) {
        resp->set_body(std::move(body));
        reactor->Finish(Status::OK);
      });
      return reactor;
    }
    AdminSession sess;
    if (need_auth) sess.authenticated = true;
    return defer(ctx, false, [this, line = std::string(line), sess, resp]() mutable {
      resp->set_body(meta_.handle_line(line, &sess));
    });
  }

 private:
  MetaNode& meta_;
  std::string token_;
  openkv::WorkPool& pool_;
};

void parse_vote_line(std::string_view line, ::storage::v1::RequestVoteResponse* resp) {
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

void parse_ae_line(std::string_view line, ::storage::v1::AppendEntriesResponse* resp) {
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

/// Meta 组内 Raft 与 Storage 共用 storage.v1.RaftService（shard 固定 0）。
class MetaRaftServiceImpl final : public ::storage::v1::RaftService::Service {
 public:
  explicit MetaRaftServiceImpl(MetaNode& m) : meta_(m) {}

  Status AppendEntries(ServerContext*, const ::storage::v1::AppendEntriesRequest* req,
                       ::storage::v1::AppendEntriesResponse* resp) override {
    std::vector<raft::LogEntry> ents;
    ents.reserve(req->entries_size());
    for (const auto& e : req->entries()) {
      ents.push_back(raft::LogEntry{e.term(), e.index(), e.data()});
    }
    auto line = meta_.handle_raft_append(req->leader_id(), req->term(), req->prev_log_index(),
                                         req->prev_log_term(), req->leader_commit(), ents);
    parse_ae_line(line, resp);
    return Status::OK;
  }

  Status RequestVote(ServerContext*, const ::storage::v1::RequestVoteRequest* req,
                     ::storage::v1::RequestVoteResponse* resp) override {
    auto line = meta_.handle_raft_vote(req->candidate_id(), req->term(), req->last_log_index(),
                                       req->last_log_term());
    parse_vote_line(line, resp);
    return Status::OK;
  }

  Status RequestPreVote(ServerContext*, const ::storage::v1::RequestVoteRequest* req,
                        ::storage::v1::RequestVoteResponse* resp) override {
    auto line = meta_.handle_raft_pre_vote(req->candidate_id(), req->term(), req->last_log_index(),
                                           req->last_log_term());
    parse_vote_line(line, resp);
    return Status::OK;
  }

  Status InstallSnapshot(ServerContext*, grpc::ServerReader<::storage::v1::SnapshotChunk>* reader,
                         ::storage::v1::InstallSnapshotResponse* resp) override {
    ::storage::v1::SnapshotChunk chunk;
    std::string buf;
    uint64_t term = 0, last_idx = 0, last_term = 0;
    std::string from;
    while (reader->Read(&chunk)) {
      term = chunk.term();
      from = chunk.leader_id();
      last_idx = chunk.last_included_index();
      last_term = chunk.last_included_term();
      buf.append(chunk.data());
      if (chunk.done()) break;
    }
    auto line = meta_.handle_raft_snapshot(from, term, last_idx, last_term, buf);
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
  MetaNode& meta_;
};

}  // namespace

struct MetaGrpcServer::Impl {
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<openkv::WorkPool> pool;
  std::unique_ptr<MetaServiceImpl> svc;
  std::unique_ptr<MetaRaftServiceImpl> raft;
};

MetaGrpcServer::MetaGrpcServer(MetaNode& meta, std::string listen_addr)
    : meta_(meta),
      listen_addr_(std::move(listen_addr)),
      admin_token_(admin_token_from_env()),
      impl_(std::make_unique<Impl>()) {}

MetaGrpcServer::~MetaGrpcServer() { stop(); }

void MetaGrpcServer::start() {
  if (running_.exchange(true)) return;
  size_t pool_n = 16;
  if (const char* e = std::getenv("SKV_META_GRPC_WORKERS")) {
    const auto v = std::strtoul(e, nullptr, 10);
    if (v > 0) pool_n = v;
  }
  impl_->pool = std::make_unique<openkv::WorkPool>(pool_n);
  impl_->svc = std::make_unique<MetaServiceImpl>(meta_, admin_token_, *impl_->pool);
  impl_->raft = std::make_unique<MetaRaftServiceImpl>(meta_);
  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(64 << 20);
  builder.SetMaxSendMessageSize(64 << 20);
  builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials());
  builder.RegisterService(impl_->svc.get());
  builder.RegisterService(impl_->raft.get());
  impl_->server = builder.BuildAndStart();
  if (!impl_->server) {
    running_ = false;
    throw std::runtime_error("MetaGrpcServer failed to listen on " + listen_addr_);
  }
  SKV_INFO << "meta gRPC listening " << listen_addr_ << " (callback + workers=" << pool_n << ")";
}

void MetaGrpcServer::stop() {
  if (!running_.exchange(false)) return;
  if (impl_ && impl_->server) {
    impl_->server->Shutdown();
    impl_->server.reset();
  }
  if (impl_) {
    impl_->svc.reset();
    impl_->raft.reset();
    impl_->pool.reset();
  }
}

}  // namespace openkv::meta
