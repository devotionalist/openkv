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

// @file admin_rpc.hpp
// Storage AdminService 类型化调用。
#pragma once

#include "openkv/codec.hpp"
#include "openkv/grpc_auth.hpp"
#include "openkv/protocol.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace openkv {

inline std::vector<std::string> admin_split_ws(std::string_view s) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == ' ' || c == '\t') {
      if (!cur.empty()) {
        out.push_back(cur);
        cur.clear();
      }
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty()) out.push_back(cur);
  return out;
}

inline std::string admin_follow_redirect_body(std::string_view body) {
  auto p = body.find("redirect=");
  if (p == std::string_view::npos) return {};
  auto rest = body.substr(p + 9);
  auto sp = rest.find_first_of(" \t\r\n");
  if (sp != std::string_view::npos) rest = rest.substr(0, sp);
  return std::string(rest);
}

/// 将历史 ADMIN_* / PING / STATS 行映射为 AdminService 类型化 RPC；返回与旧行协议兼容的正文。
inline std::string admin_typed_call(std::string_view addr, std::string_view req, std::string_view token,
                                    int timeout_ms) {
  auto parts = admin_split_ws(req);
  if (parts.empty()) throw std::runtime_error("admin empty");
  const std::string& op = parts[0];

  auto ch = grpc::CreateChannel(std::string(addr), grpc::InsecureChannelCredentials());
  auto stub = ::storage::v1::AdminService::NewStub(ch);
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&ctx, std::string(token));

  auto simple_msg = [](const ::storage::v1::AdminSimpleResponse& r) -> std::string {
    if (!r.message().empty()) return r.message();
    if (!r.redirect_to().empty()) return "ERR not_leader redirect=" + r.redirect_to();
    return r.ok() ? "OK" : "ERR";
  };

  if (op == proto::kOpPing || op == "PING") {
    ::storage::v1::PingRequest ping;
    ::storage::v1::PingResponse resp;
    auto st = stub->Ping(&ctx, ping, &resp);
    if (!st.ok()) throw std::runtime_error("admin Ping: " + st.error_message());
    // 与历史行协议对齐：OK PONG
    return "OK PONG";
  }
  if (op == proto::kOpStats || op == "STATS") {
    ::storage::v1::StatsRequest sreq;
    ::storage::v1::StatsResponse sresp;
    auto st = stub->Stats(&ctx, sreq, &sresp);
    if (!st.ok()) throw std::runtime_error("admin Stats: " + st.error_message());
    std::ostringstream oss;
    oss << "OK STATS requests=" << sresp.requests() << " gw=" << sresp.gw()
        << " raft=" << sresp.raft() << " sync=" << sresp.sync() << " admin=" << sresp.admin()
        << " meta_out=" << sresp.meta_out() << " client=" << sresp.client()
        << " qdepth=" << sresp.qdepth() << " qdrop=" << sresp.qdrop();
    auto emit = [&](const char* key, const auto& m) {
      if (m.empty()) return;
      oss << ' ' << key << '=';
      bool first = true;
      for (const auto& kv : m) {
        if (!first) oss << ',';
        first = false;
        oss << kv.first << ':' << kv.second;
      }
    };
    emit("peer_raft", sresp.peer_raft());
    emit("peer_sync", sresp.peer_sync());
    emit("peer_raft_out", sresp.peer_raft_out());
    emit("peer_sync_out", sresp.peer_sync_out());
    emit("meta_out_peers", sresp.meta_out_peers());
    return oss.str();
  }
  if (op == proto::kOpAdminCampaign && parts.size() >= 2) {
    ::storage::v1::CampaignRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->Campaign(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin Campaign: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminDestroyRegion && parts.size() >= 2) {
    ::storage::v1::DestroyRegionRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->DestroyRegion(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin DestroyRegion: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminPromote && parts.size() >= 3) {
    ::storage::v1::PeerOpRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_node_id(parts[2]);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->Promote(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin Promote: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminRemovePeer && parts.size() >= 3) {
    ::storage::v1::PeerOpRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_node_id(parts[2]);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->RemovePeer(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin RemovePeer: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminPeerProgress && parts.size() >= 3) {
    ::storage::v1::PeerOpRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_node_id(parts[2]);
    ::storage::v1::PeerProgressResponse resp;
    auto st = stub->PeerProgress(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin PeerProgress: " + st.error_message());
    if (!resp.message().empty()) return resp.message();
    if (!resp.redirect_to().empty()) return "ERR not_leader redirect=" + resp.redirect_to();
    std::ostringstream oss;
    oss << "OK PROGRESS " << resp.match_index() << ' ' << resp.commit_index() << ' '
        << resp.last_index() << ' ' << (resp.caught_up() ? 1 : 0) << ' '
        << (resp.is_learner() ? 1 : 0);
    return oss.str();
  }
  if (op == proto::kOpAdminTransferLeader && parts.size() >= 3) {
    ::storage::v1::TransferRequest r;
    r.set_shard_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_target_peer(parts[2]);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->TransferLeader(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin TransferLeader: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminAddLearner && parts.size() >= 3) {
    // id@addr
    const auto& tok = parts[2];
    auto at = tok.find('@');
    if (at == std::string::npos) throw std::runtime_error("ADMIN_ADD_LEARNER bad peer");
    ::storage::v1::PeerOpRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_node_id(tok.substr(0, at));
    r.set_address(tok.substr(at + 1));
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->AddLearner(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin AddLearner: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminEnsureRegion && parts.size() >= 2) {
    ::storage::v1::EnsureRegionRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    if (parts.size() >= 3) {
      std::stringstream ss(parts[2]);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        bool learner = false;
        if (item.size() >= 2 && item.substr(item.size() - 2) == "*N") {
          learner = true;
          item = item.substr(0, item.size() - 2);
        }
        if (item.size() >= 2 && item.substr(item.size() - 2) == "*L") {
          item = item.substr(0, item.size() - 2);
        }
        auto at = item.find('@');
        if (at == std::string::npos) continue;
        auto* p = r.add_peers();
        p->set_node_id(item.substr(0, at));
        p->set_address(item.substr(at + 1));
        p->set_learner(learner);
      }
    }
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->EnsureRegion(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin EnsureRegion: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminPlaceRegion && parts.size() >= 3) {
    ::storage::v1::PlaceRegionRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    std::stringstream ss(parts[2]);
    std::string id;
    while (std::getline(ss, id, ',')) {
      if (!id.empty()) r.add_desired_peers(id);
    }
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->PlaceRegion(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin PlaceRegion: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminPrepareSplit && parts.size() >= 3) {
    ::storage::v1::PrepareSplitRequest r;
    r.set_left_region(static_cast<uint32_t>(std::stoul(parts[1])));
    std::string key;
    if (!from_hex(parts[2], &key)) throw std::runtime_error("ADMIN_PREPARE_SPLIT bad hex");
    r.set_split_key(key);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->PrepareSplit(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin PrepareSplit: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminSplit && parts.size() >= 4) {
    ::storage::v1::SplitRequest r;
    r.set_left_region(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_right_region(static_cast<uint32_t>(std::stoul(parts[2])));
    std::string key;
    if (!from_hex(parts[3], &key)) throw std::runtime_error("ADMIN_SPLIT bad hex");
    r.set_split_key(key);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->Split(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin Split: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminMerge && parts.size() >= 3) {
    ::storage::v1::MergeRequest r;
    r.set_left_region(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_right_region(static_cast<uint32_t>(std::stoul(parts[2])));
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->Merge(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin Merge: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminSetEpoch && parts.size() >= 3) {
    ::storage::v1::SetEpochRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_epoch(std::stoull(parts[2]));
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->SetEpoch(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin SetEpoch: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminCheckpoint && parts.size() >= 2) {
    ::storage::v1::CheckpointRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    if (parts.size() >= 3) r.set_path(parts[2]);
    ::storage::v1::AdminSimpleResponse resp;
    auto st = stub->Checkpoint(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin Checkpoint: " + st.error_message());
    return simple_msg(resp);
  }
  if (op == proto::kOpAdminRegionStats && parts.size() >= 2) {
    ::storage::v1::RegionStatsRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    ::storage::v1::RegionStatsResponse resp;
    auto st = stub->RegionStats(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("admin RegionStats: " + st.error_message());
    if (!resp.message().empty() && resp.message() != "OK") return resp.message();
    std::ostringstream oss;
    oss << "OK REGION_STATS " << resp.key_count() << ' ' << resp.approx_bytes() << ' '
        << resp.epoch();
    return oss.str();
  }
  throw std::runtime_error(std::string("unsupported admin op: ") + op);
}

/// 跟随 not_leader redirect（最多数跳）。
inline std::string admin_typed_rpc(std::string addr, std::string_view req, std::string_view token,
                                   int timeout_ms) {
  for (int hop = 0; hop < 6; ++hop) {
    if (addr.rfind("0.0.0.0", 0) == 0) addr.replace(0, 7, "127.0.0.1");
    std::string body = admin_typed_call(addr, req, token, timeout_ms);
    if (body.find("not_leader") != std::string::npos) {
      auto next = admin_follow_redirect_body(body);
      if (next.empty()) throw std::runtime_error(body);
      addr = std::move(next);
      continue;
    }
    return body;
  }
  throw std::runtime_error("admin_typed_rpc redirect loop");
}

/// 异步 Admin Ping（不占调用线程；回调在 gRPC CQ）。
inline void admin_ping_async(std::string addr, std::string_view token, int timeout_ms,
                             std::function<void(std::string)> cb) {
  if (!cb) return;
  if (addr.rfind("0.0.0.0", 0) == 0) addr.replace(0, 7, "127.0.0.1");
  struct Call {
    std::shared_ptr<grpc::Channel> ch;
    std::unique_ptr<::storage::v1::AdminService::Stub> stub;
    grpc::ClientContext ctx;
    ::storage::v1::PingRequest req;
    ::storage::v1::PingResponse resp;
  };
  auto call = std::make_shared<Call>();
  call->ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  call->stub = ::storage::v1::AdminService::NewStub(call->ch);
  call->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&call->ctx, std::string(token));
  call->stub->async()->Ping(&call->ctx, &call->req, &call->resp,
                            [call, cb = std::move(cb)](grpc::Status st) {
                              if (!st.ok()) {
                                cb("ERR unreachable " + st.error_message());
                                return;
                              }
                              cb("OK PONG");
                            });
}

/// 异步 PeerProgress；跟随 not_leader redirect（最多数跳）。
inline void admin_peer_progress_async(std::string addr, uint32_t region_id, std::string node_id,
                                      std::string_view token, int timeout_ms, int hops_left,
                                      std::function<void(std::string)> cb) {
  if (!cb) return;
  if (hops_left <= 0) {
    cb("ERR admin_typed_rpc redirect loop");
    return;
  }
  if (addr.rfind("0.0.0.0", 0) == 0) addr.replace(0, 7, "127.0.0.1");
  struct Call {
    std::shared_ptr<grpc::Channel> ch;
    std::unique_ptr<::storage::v1::AdminService::Stub> stub;
    grpc::ClientContext ctx;
    ::storage::v1::PeerOpRequest req;
    ::storage::v1::PeerProgressResponse resp;
  };
  auto call = std::make_shared<Call>();
  call->ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  call->stub = ::storage::v1::AdminService::NewStub(call->ch);
  call->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&call->ctx, std::string(token));
  call->req.set_region_id(region_id);
  call->req.set_node_id(node_id);
  const std::string tok(token);
  call->stub->async()->PeerProgress(
      &call->ctx, &call->req, &call->resp,
      [call, cb = std::move(cb), region_id, node_id = std::move(node_id), tok, timeout_ms,
       hops_left](grpc::Status st) mutable {
        if (!st.ok()) {
          cb("ERR " + st.error_message());
          return;
        }
        std::string body;
        if (!call->resp.message().empty()) {
          body = call->resp.message();
        } else if (!call->resp.redirect_to().empty()) {
          body = "ERR not_leader redirect=" + call->resp.redirect_to();
        } else {
          std::ostringstream oss;
          oss << "OK PROGRESS " << call->resp.match_index() << ' ' << call->resp.commit_index()
              << ' ' << call->resp.last_index() << ' ' << (call->resp.caught_up() ? 1 : 0) << ' '
              << (call->resp.is_learner() ? 1 : 0);
          body = oss.str();
        }
        if (body.find("not_leader") != std::string::npos) {
          auto next = admin_follow_redirect_body(body);
          if (next.empty()) {
            cb(std::move(body));
            return;
          }
          admin_peer_progress_async(std::move(next), region_id, std::move(node_id), tok, timeout_ms,
                                    hops_left - 1, std::move(cb));
          return;
        }
        cb(std::move(body));
      });
}

}  // namespace openkv
