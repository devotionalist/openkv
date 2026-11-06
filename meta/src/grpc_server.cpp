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
// Meta gRPC：多数 RPC 桥接到 handle_line，避免双实现。

#include "opskv/meta/grpc_server.hpp"

#include "opskv/admin_acl.hpp"
#include "opskv/codec.hpp"
#include "opskv/grpc_auth.hpp"
#include "opskv/logging.hpp"
#include "opskv/protocol.hpp"
#include "opskv/raft/raft_node.hpp"

#include <sstream>
#include <stdexcept>

#include <grpcpp/grpcpp.h>
#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace opskv::meta {


namespace {

using grpc::ServerContext;
using grpc::Status;
namespace pb = ::meta::v1;

bool line_ok(std::string_view s) { return proto::starts_with_ok(s); }

class MetaServiceImpl final : public pb::MetaService::Service {
 public:
  MetaServiceImpl(MetaNode& m, std::string token) : meta_(m), token_(std::move(token)) {}

  Status auth_write(ServerContext* ctx) {
    if (!grpc_admin_allowed(*ctx, token_)) {
      return Status(grpc::StatusCode::UNAUTHENTICATED, "unauthorized");
    }
    return Status::OK;
  }

  Status Ping(ServerContext*, const pb::PingRequest*, pb::PingResponse* resp) override {
    resp->set_message(meta_.handle_line("PING"));
    return Status::OK;
  }

  Status RegisterNode(ServerContext*, const pb::RegisterNodeRequest* req,
                      pb::RegisterNodeResponse* resp) override {
    const auto& n = req->node();
    std::ostringstream oss;
    oss << "REGISTER " << n.node_id() << " " << n.address() << " STORAGE";
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    resp->set_assigned_id(n.node_id());
    if (!resp->ok()) resp->set_error(line);
    return Status::OK;
  }

  Status Heartbeat(ServerContext*, const pb::HeartbeatRequest* req,
                   pb::HeartbeatResponse* resp) override {
    std::ostringstream oss;
    oss << "HEARTBEAT " << req->node_id();
    for (uint32_t s : req->leader_shards()) oss << " " << s;
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    if (!resp->ok()) resp->set_error(line);
    std::string err, redir;
    auto rt = meta_.routing_table(&err, &redir);
    resp->set_routing_version(rt.version);
    return Status::OK;
  }

  Status GetRoutingTable(ServerContext*, const pb::GetRoutingTableRequest*,
                         pb::RoutingTable* resp) override {
    auto line = meta_.handle_line("GET_ROUTE");
    // 最小：version 从 ClusterState 读
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
    return Status::OK;
  }

  Status WatchRoutingTable(ServerContext*, const pb::WatchRoutingRequest* req,
                           grpc::ServerWriter<pb::RoutingTable>* writer) override {
    std::ostringstream oss;
    oss << "WATCH_ROUTE " << req->from_version();
    if (req->timeout_ms() > 0) oss << " " << req->timeout_ms();
    meta_.handle_line(oss.str());
    pb::RoutingTable tbl;
    GetRoutingTable(nullptr, nullptr, &tbl);
    writer->Write(tbl);
    return Status::OK;
  }

  Status ScaleOut(ServerContext* ctx, const pb::ScaleOutRequest* req,
                  pb::ScaleOutResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
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
    return Status::OK;
  }

  Status GetScaleTask(ServerContext*, const pb::GetScaleTaskRequest* req,
                      pb::ScaleTask* resp) override {
    auto line = meta_.handle_line("GET_SCALE " + req->task_id());
    resp->set_task_id(req->task_id());
    resp->set_error(line_ok(line) ? "" : line);
    return Status::OK;
  }

  Status ListScaleTasks(ServerContext*, const pb::ListScaleTasksRequest*,
                        pb::ListScaleTasksResponse*) override {
    meta_.handle_line("LIST_SCALE");
    return Status::OK;
  }

  Status ListNodes(ServerContext*, const pb::ListNodesRequest*,
                   pb::ListNodesResponse* resp) override {
    meta_.handle_line("LIST_NODES");
    // 填充最小空列表；完整解析可后续增强
    (void)resp;
    return Status::OK;
  }

#define META_NODE_RPC(Name, Op)                                                                \
  Status Name(ServerContext* ctx, const pb::NodeIdRequest* req, pb::SimpleResponse* resp)      \
      override {                                                                               \
    if (auto st = auth_write(ctx); !st.ok()) return st;                                        \
    auto line = meta_.handle_line(std::string(Op) + " " + req->node_id());                     \
    resp->set_ok(line_ok(line));                                                               \
    resp->set_message(line);                                                                   \
    if (!resp->ok()) resp->set_error(line);                                                    \
    return Status::OK;                                                                         \
  }

  META_NODE_RPC(DrainNode, "DRAIN_NODE")
  META_NODE_RPC(ActivateNode, "ACTIVATE_NODE")
  META_NODE_RPC(OfflineNode, "OFFLINE_NODE")
  META_NODE_RPC(JoinNode, "JOIN_NODE")
  META_NODE_RPC(KickNode, "KICK_NODE")
  META_NODE_RPC(RemoveTombstone, "REMOVE_TOMBSTONE")
  META_NODE_RPC(StoreDelete, "STORE_DELETE")
#undef META_NODE_RPC

  Status SplitRegion(ServerContext* ctx, const pb::SplitRegionRequest* req,
                     pb::SplitRegionResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    std::ostringstream oss;
    oss << "SPLIT_REGION " << req->region_id() << " " << opskv::to_hex(req->split_key());
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    if (!resp->ok()) resp->set_error(line);
    return Status::OK;
  }

  Status MergeRegions(ServerContext* ctx, const pb::MergeRegionsRequest* req,
                      pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    std::ostringstream oss;
    oss << "MERGE_REGIONS " << req->left_region_id() << " " << req->right_region_id();
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status PlaceRegion(ServerContext* ctx, const pb::PlaceRegionRequest* req,
                     pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    std::ostringstream oss;
    oss << "PLACE_REGION " << req->region_id() << " ";
    for (int i = 0; i < req->peer_node_ids_size(); ++i) {
      if (i) oss << ",";
      oss << req->peer_node_ids(i);
    }
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status ScheduleMigrate(ServerContext* ctx, const pb::ScheduleMigrateRequest* req,
                         pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    std::ostringstream oss;
    oss << "SCHEDULE_MIGRATE " << req->region_id() << " " << req->from_node() << " "
        << req->to_node();
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status CancelMigrate(ServerContext* ctx, const pb::CancelMigrateRequest* req,
                       pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    auto line = meta_.handle_line("CANCEL_MIGRATE " + req->task_id());
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status ListMigrate(ServerContext*, const pb::ListMigrateRequest*,
                     pb::ListMigrateResponse*) override {
    meta_.handle_line("LIST_MIGRATE");
    return Status::OK;
  }

  Status Balance(ServerContext* ctx, const pb::BalanceRequest*,
                 pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    auto line = meta_.handle_line("BALANCE");
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status SuggestPlace(ServerContext*, const pb::SuggestPlaceRequest* req,
                      pb::SuggestPlaceResponse* resp) override {
    auto line = meta_.handle_line("SUGGEST_PLACE " + std::to_string(req->count()));
    (void)line;
    (void)resp;
    return Status::OK;
  }

  Status BumpRoute(ServerContext* ctx, const pb::BumpRouteRequest*,
                   pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    auto line = meta_.handle_line("BUMP_ROUTE");
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status GetTs(ServerContext*, const pb::GetTsRequest*, pb::GetTsResponse* resp) override {
    auto line = meta_.handle_line("GET_TS");
    std::istringstream iss(line);
    std::string ok, tag;
    uint64_t ts = 0;
    if (iss >> ok >> tag >> ts) resp->set_ts(ts);
    return Status::OK;
  }

  Status GetSafePoint(ServerContext*, const pb::GetSafePointRequest*,
                      pb::GetSafePointResponse* resp) override {
    auto line = meta_.handle_line("GET_SAFE_POINT");
    std::istringstream iss(line);
    std::string ok, tag;
    uint64_t sp = 0;
    if (iss >> ok >> tag >> sp) resp->set_safe_point(sp);
    return Status::OK;
  }

  Status AdvanceSafePoint(ServerContext* ctx, const pb::AdvanceSafePointRequest* req,
                          pb::SimpleResponse* resp) override {
    if (auto st = auth_write(ctx); !st.ok()) return st;
    auto line = meta_.handle_line("ADVANCE_SAFE_POINT " + std::to_string(req->safe_point()));
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status ReportMinStartTs(ServerContext*, const pb::ReportMinStartTsRequest* req,
                          pb::SimpleResponse* resp) override {
    std::ostringstream oss;
    oss << "REPORT_MIN_START_TS " << req->min_start_ts() << " " << req->node_id();
    auto line = meta_.handle_line(oss.str());
    resp->set_ok(line_ok(line));
    resp->set_message(line);
    return Status::OK;
  }

  Status Locate(ServerContext*, const pb::LocateRequest* req, pb::LocateResponse* resp) override {
    auto line = meta_.handle_line("LOCATE " + opskv::to_hex(req->key()));
    resp->set_ok(line_ok(line));
    if (!resp->ok()) resp->set_error(line);
    return Status::OK;
  }

  Status WhoLeader(ServerContext*, const pb::WhoLeaderRequest*,
                   pb::WhoLeaderResponse* resp) override {
    resp->set_address(meta_.leader_address());
    resp->set_leader_id(meta_.is_leader() ? meta_.bootstrap().node_id : "");
    return Status::OK;
  }

  Status StoreStatus(ServerContext*, const pb::StoreStatusRequest* req,
                     pb::StoreStatusResponse* resp) override {
    auto line = meta_.handle_line("STORE_STATUS " + req->node_id());
    resp->set_ok(line_ok(line));
    resp->set_status(line);
    if (!resp->ok()) resp->set_error(line);
    return Status::OK;
  }

  Status FindMergeRight(ServerContext*, const pb::FindMergeRightRequest* req,
                        pb::FindMergeRightResponse* resp) override {
    auto line = meta_.handle_line("FIND_MERGE_RIGHT " + std::to_string(req->left_region_id()));
    resp->set_ok(line_ok(line));
    if (!resp->ok()) resp->set_error(line);
    return Status::OK;
  }

  Status ExecLine(ServerContext* ctx, const pb::ExecLineRequest* req,
                  pb::ExecLineResponse* resp) override {
    AdminSession sess;
    const auto& line = req->line();
    // 写运维需 token；REGISTER/HEARTBEAT/读路径放行
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
      if (auto st = auth_write(ctx); !st.ok()) return st;
      sess.authenticated = true;
    }
    resp->set_body(meta_.handle_line(line, &sess));
    return Status::OK;
  }

 private:
  MetaNode& meta_;
  std::string token_;
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
  impl_->svc = std::make_unique<MetaServiceImpl>(meta_, admin_token_);
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
  SKV_INFO << "meta gRPC listening " << listen_addr_;
}

void MetaGrpcServer::stop() {
  if (!running_.exchange(false)) return;
  if (impl_ && impl_->server) {
    impl_->server->Shutdown();
    impl_->server.reset();
  }
}

}  // namespace opskv::meta
