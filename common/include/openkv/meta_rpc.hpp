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

// @file meta_rpc.hpp
// MetaService 类型化调用；返回与旧行协议兼容的正文供内部解析。
#pragma once

#include "openkv/admin_rpc.hpp"
#include "openkv/codec.hpp"
#include "openkv/grpc_auth.hpp"
#include "openkv/protocol.hpp"

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"

namespace openkv {

inline std::string format_routing_table_line(const ::meta::v1::RoutingTable& rt) {
  // 与 MetaNode::format_route 一致：rid:start:end:epoch:nrep,id@addr*L,...
  // Storage recover_local_regions_from_meta 依赖 nrep, 前缀解析副本；缺逗号会 ensured=0。
  std::ostringstream oss;
  oss << "OK ROUTE " << rt.version() << ' ' << rt.shards_size() << ' ' << rt.shards_size();
  for (const auto& sh : rt.shards()) {
    oss << ' ' << sh.shard_id() << ':' << to_hex(sh.start_key()) << ':' << to_hex(sh.end_key())
        << ':' << sh.epoch() << ':' << sh.replicas_size();
    for (const auto& r : sh.replicas()) {
      oss << ',' << r.node_id() << '@' << r.address();
      if (r.is_leader()) oss << "*L";
      if (r.is_learner()) oss << "*N";
    }
  }
  return oss.str();
}

inline std::string meta_typed_call(std::string_view addr, std::string_view req, std::string_view token,
                                   int timeout_ms) {
  auto parts = admin_split_ws(req);
  if (parts.empty()) throw std::runtime_error("meta empty");
  const std::string& op = parts[0];

  auto ch = grpc::CreateChannel(std::string(addr), grpc::InsecureChannelCredentials());
  auto stub = ::meta::v1::MetaService::NewStub(ch);
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(&ctx, std::string(token));

  auto simple_line = [](const ::meta::v1::SimpleResponse& r) -> std::string {
    if (!r.message().empty()) return r.message();
    if (!r.error().empty()) return r.error();
    return r.ok() ? "OK" : "ERR";
  };

  if (op == proto::kOpPing || op == "PING") {
    ::meta::v1::PingRequest ping;
    ::meta::v1::PingResponse resp;
    auto st = stub->Ping(&ctx, ping, &resp);
    if (!st.ok()) throw std::runtime_error("meta Ping: " + st.error_message());
    return resp.message().empty() ? "OK PONG" : resp.message();
  }
  if (op == "WHO_LEADER") {
    ::meta::v1::WhoLeaderRequest r;
    ::meta::v1::WhoLeaderResponse resp;
    auto st = stub->WhoLeader(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta WhoLeader: " + st.error_message());
    return "OK LEADER " + resp.leader_id() + " " + resp.address();
  }
  if (op == "GET_ROUTE") {
    ::meta::v1::GetRoutingTableRequest r;
    ::meta::v1::RoutingTable resp;
    auto st = stub->GetRoutingTable(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta GetRoutingTable: " + st.error_message());
    return format_routing_table_line(resp);
  }
  if (op == "GET_TS") {
    ::meta::v1::GetTsRequest r;
    uint32_t count = 1;
    if (parts.size() >= 2) {
      count = static_cast<uint32_t>(std::stoul(parts[1]));
      if (count == 0) count = 1;
    }
    r.set_count(count);
    ::meta::v1::GetTsResponse resp;
    auto st = stub->GetTs(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta GetTs: " + st.error_message());
    uint32_t got = resp.count() == 0 ? 1 : resp.count();
    return "OK TS " + std::to_string(resp.ts()) + " " + std::to_string(got);
  }
  if (op == "GET_SAFE_POINT" || op == "GET_SAFE") {
    ::meta::v1::GetSafePointRequest r;
    ::meta::v1::GetSafePointResponse resp;
    auto st = stub->GetSafePoint(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta GetSafePoint: " + st.error_message());
    return "OK SAFE " + std::to_string(resp.safe_point()) + " " + std::to_string(resp.next_ts()) +
           " " + std::to_string(resp.min_start_hint());
  }
  if (op == "STATS") {
    ::meta::v1::StatsRequest r;
    ::meta::v1::StatsResponse resp;
    auto st = stub->Stats(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta Stats: " + st.error_message());
    std::ostringstream oss;
    oss << "OK STATS requests=" << resp.requests() << " gw=" << resp.gw()
        << " store=" << resp.store() << " raft=" << resp.raft() << " sync=" << resp.sync()
        << " admin=" << resp.admin() << " client=" << resp.client()
        << " route_ver=" << resp.route_ver() << " next_ts=" << resp.next_ts()
        << " tso_alloc=" << resp.tso_alloc() << " watch_waits=" << resp.watch_waits()
        << " migrates=" << resp.migrates();
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
    emit("peer_raft", resp.peer_raft());
    emit("peer_sync", resp.peer_sync());
    emit("peer_raft_out", resp.peer_raft_out());
    emit("peer_sync_out", resp.peer_sync_out());
    emit("peer_store", resp.peer_store());
    return oss.str();
  }
  if (op == "LIST_NODES") {
    ::meta::v1::ListNodesRequest r;
    if (parts.size() >= 2) {
      if (parts[1] == "storage") r.set_role(::meta::v1::STORAGE);
      else if (parts[1] == "compute") r.set_role(::meta::v1::COMPUTE);
    }
    ::meta::v1::ListNodesResponse resp;
    auto st = stub->ListNodes(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ListNodes: " + st.error_message());
    std::ostringstream oss;
    oss << "OK NODES " << resp.nodes_size();
    for (const auto& n : resp.nodes()) {
      char role = n.role() == ::meta::v1::STORAGE ? 'S' : 'C';
      int state = 0;
      switch (n.state()) {
        case ::meta::v1::JOINING: state = 0; break;
        case ::meta::v1::ACTIVE: state = 1; break;
        case ::meta::v1::DRAINING: state = 2; break;
        case ::meta::v1::OFFLINE: state = 3; break;
        default: state = 4; break;
      }
      oss << ' ' << n.node_id() << '@' << n.address() << ':' << role << ':' << state;
    }
    return oss.str();
  }
  if (op == "REGISTER" && parts.size() >= 4) {
    // REGISTER storage|compute <id> <addr>
    ::meta::v1::RegisterNodeRequest r;
    auto* n = r.mutable_node();
    n->set_role(parts[1] == "compute" ? ::meta::v1::COMPUTE : ::meta::v1::STORAGE);
    n->set_node_id(parts[2]);
    n->set_address(parts[3]);
    ::meta::v1::RegisterNodeResponse resp;
    auto st = stub->RegisterNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta RegisterNode: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR register" : resp.error();
    return "OK REGISTER " + (resp.assigned_id().empty() ? n->node_id() : resp.assigned_id());
  }
  if (op == "HEARTBEAT" && parts.size() >= 3) {
    // HEARTBEAT <id> <n> <shard...> [regions=…] [keys=…]
    ::meta::v1::HeartbeatRequest r;
    r.set_node_id(parts[1]);
    size_t n = 0;
    try {
      n = static_cast<size_t>(std::stoul(parts[2]));
    } catch (...) {
      throw std::runtime_error("meta HEARTBEAT bad count");
    }
    if (parts.size() < 3 + n) throw std::runtime_error("meta HEARTBEAT truncated");
    for (size_t i = 0; i < n; ++i) {
      r.add_leader_shards(static_cast<uint32_t>(std::stoul(parts[3 + i])));
    }
    for (size_t i = 3 + n; i < parts.size(); ++i) {
      auto eq = parts[i].find('=');
      if (eq == std::string::npos) continue;
      (*r.mutable_metrics())[parts[i].substr(0, eq)] = std::stod(parts[i].substr(eq + 1));
    }
    ::meta::v1::HeartbeatResponse resp;
    auto st = stub->Heartbeat(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta Heartbeat: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR heartbeat" : resp.error();
    return "OK HEARTBEAT";
  }
  if (op == "REPORT_MIN_START_TS" && parts.size() >= 3) {
    ::meta::v1::ReportMinStartTsRequest r;
    r.set_min_start_ts(std::stoull(parts[1]));
    r.set_node_id(parts[2]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->ReportMinStartTs(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ReportMinStartTs: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "SPLIT_REGION" && parts.size() >= 3) {
    ::meta::v1::SplitRegionRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    std::string key;
    if (!from_hex(parts[2], &key)) throw std::runtime_error("SPLIT_REGION bad hex");
    r.set_split_key(key);
    ::meta::v1::SplitRegionResponse resp;
    auto st = stub->SplitRegion(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta SplitRegion: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR split" : resp.error();
    return "OK SPLIT " + parts[1] + " " + std::to_string(resp.right_region_id());
  }
  if (op == "MERGE_REGIONS" && parts.size() >= 3) {
    ::meta::v1::MergeRegionsRequest r;
    r.set_left_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    r.set_right_region_id(static_cast<uint32_t>(std::stoul(parts[2])));
    ::meta::v1::SimpleResponse resp;
    auto st = stub->MergeRegions(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta MergeRegions: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "FIND_MERGE_RIGHT" && parts.size() >= 2) {
    ::meta::v1::FindMergeRightRequest r;
    r.set_left_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    ::meta::v1::FindMergeRightResponse resp;
    auto st = stub->FindMergeRight(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta FindMergeRight: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR" : resp.error();
    return "OK MERGE_RIGHT " + parts[1] + " " + std::to_string(resp.right_region_id());
  }
  if (op == "SUGGEST_PLACE") {
    ::meta::v1::SuggestPlaceRequest r;
    if (parts.size() >= 2 && parts[1].rfind("avoid=", 0) == 0) {
      std::stringstream ss(parts[1].substr(6));
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) r.add_avoid_node_ids(item);
      }
    } else if (parts.size() >= 2) {
      r.set_count(static_cast<uint32_t>(std::stoul(parts[1])));
    }
    ::meta::v1::SuggestPlaceResponse resp;
    auto st = stub->SuggestPlace(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta SuggestPlace: " + st.error_message());
    std::ostringstream oss;
    oss << "OK SUGGEST " << resp.node_ids_size();
    for (const auto& id : resp.node_ids()) oss << ' ' << id;
    return oss.str();
  }
  if (op == "PLACE_REGION" && parts.size() >= 3) {
    ::meta::v1::PlaceRegionRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    std::stringstream ss(parts[2]);
    std::string id;
    while (std::getline(ss, id, ',')) {
      if (!id.empty()) r.add_peer_node_ids(id);
    }
    ::meta::v1::SimpleResponse resp;
    auto st = stub->PlaceRegion(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta PlaceRegion: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "JOIN_NODE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->JoinNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta JoinNode: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "KICK_NODE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->KickNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta KickNode: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "STORE_DELETE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->StoreDelete(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta StoreDelete: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "DRAIN_NODE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->DrainNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta DrainNode: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "ACTIVATE_NODE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->ActivateNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ActivateNode: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "OFFLINE_NODE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->OfflineNode(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta OfflineNode: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "REMOVE_TOMBSTONE" && parts.size() >= 2) {
    ::meta::v1::NodeIdRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->RemoveTombstone(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta RemoveTombstone: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "BALANCE") {
    ::meta::v1::BalanceRequest r;
    if (parts.size() >= 2) r.set_count(static_cast<uint32_t>(std::stoul(parts[1])));
    ::meta::v1::SimpleResponse resp;
    auto st = stub->Balance(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta Balance: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "BUMP_ROUTE") {
    ::meta::v1::BumpRouteRequest r;
    ::meta::v1::SimpleResponse resp;
    auto st = stub->BumpRoute(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta BumpRoute: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "ADVANCE_SAFE_POINT") {
    ::meta::v1::AdvanceSafePointRequest r;
    if (parts.size() >= 2) r.set_safe_point(std::stoull(parts[1]));
    ::meta::v1::SimpleResponse resp;
    auto st = stub->AdvanceSafePoint(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta AdvanceSafePoint: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "STORE_STATUS" && parts.size() >= 2) {
    ::meta::v1::StoreStatusRequest r;
    r.set_node_id(parts[1]);
    ::meta::v1::StoreStatusResponse resp;
    auto st = stub->StoreStatus(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta StoreStatus: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR" : resp.error();
    return resp.status().empty() ? "OK STORE " + parts[1] : resp.status();
  }
  if (op == "LOCATE" && parts.size() >= 2) {
    ::meta::v1::LocateRequest r;
    std::string key;
    if (!from_hex(parts[1], &key)) key = parts[1];
    r.set_key(key);
    ::meta::v1::LocateResponse resp;
    auto st = stub->Locate(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta Locate: " + st.error_message());
    if (!resp.ok()) return resp.error().empty() ? "ERR" : resp.error();
    const auto& sh = resp.shard();
    std::ostringstream oss;
    oss << "OK LOCATE " << sh.shard_id() << ' ' << to_hex(sh.start_key()) << ' '
        << to_hex(sh.end_key()) << ' ' << sh.epoch();
    return oss.str();
  }
  if (op == "SCALE_OUT" && parts.size() >= 2) {
    ::meta::v1::ScaleOutRequest r;
    r.set_new_storage_node_id(parts[1]);
    if (parts.size() >= 3) {
      std::stringstream ss(parts[2]);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) r.add_shard_ids(static_cast<uint32_t>(std::stoul(item)));
      }
    }
    ::meta::v1::ScaleOutResponse resp;
    auto st = stub->ScaleOut(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ScaleOut: " + st.error_message());
    return resp.message().empty() ? (resp.accepted() ? "OK SCALE" : "ERR") : resp.message();
  }
  if (op == "CANCEL_SCALE" && parts.size() >= 2) {
    ::meta::v1::CancelScaleRequest r;
    r.set_node_id(parts[1]);
    if (parts.size() >= 3) r.set_region_id(static_cast<uint32_t>(std::stoul(parts[2])));
    ::meta::v1::SimpleResponse resp;
    auto st = stub->CancelScale(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta CancelScale: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "GET_SCALE" && parts.size() >= 2) {
    ::meta::v1::GetScaleTaskRequest r;
    r.set_task_id(parts[1]);
    ::meta::v1::ScaleTask resp;
    auto st = stub->GetScaleTask(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta GetScaleTask: " + st.error_message());
    if (!resp.error().empty() && resp.task_id().empty()) return resp.error();
    std::ostringstream oss;
    oss << "OK SCALE " << resp.task_id() << ' ' << static_cast<int>(resp.state()) << ' '
        << resp.progress() << " node=" << resp.new_node_id() << " shards=";
    for (int i = 0; i < resp.shard_ids_size(); ++i) {
      if (i) oss << ',';
      oss << resp.shard_ids(i);
    }
    if (!resp.error().empty()) oss << " error=" << resp.error();
    return oss.str();
  }
  if (op == "LIST_SCALE") {
    ::meta::v1::ListScaleTasksRequest r;
    ::meta::v1::ListScaleTasksResponse resp;
    auto st = stub->ListScaleTasks(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ListScaleTasks: " + st.error_message());
    std::ostringstream oss;
    oss << "OK SCALE_LIST " << resp.tasks_size();
    for (const auto& t : resp.tasks()) {
      oss << ' ' << t.task_id() << ':' << t.new_node_id() << ':' << static_cast<int>(t.state()) << ':'
          << t.progress();
      if (!t.shard_ids().empty()) {
        oss << ':';
        for (int i = 0; i < t.shard_ids_size(); ++i) {
          if (i) oss << ',';
          oss << t.shard_ids(i);
        }
      }
    }
    return oss.str();
  }
  if (op == "SCHEDULE_MIGRATE" && parts.size() >= 2) {
    ::meta::v1::ScheduleMigrateRequest r;
    r.set_region_id(static_cast<uint32_t>(std::stoul(parts[1])));
    // 旧行：SCHEDULE_MIGRATE rid [desired_csv]；typed 用 from/to 时可空
    ::meta::v1::SimpleResponse resp;
    auto st = stub->ScheduleMigrate(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ScheduleMigrate: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "CANCEL_MIGRATE" && parts.size() >= 2) {
    ::meta::v1::CancelMigrateRequest r;
    r.set_task_id(parts[1]);
    ::meta::v1::SimpleResponse resp;
    auto st = stub->CancelMigrate(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta CancelMigrate: " + st.error_message());
    return simple_line(resp);
  }
  if (op == "LIST_MIGRATE") {
    ::meta::v1::ListMigrateRequest r;
    ::meta::v1::ListMigrateResponse resp;
    auto st = stub->ListMigrate(&ctx, r, &resp);
    if (!st.ok()) throw std::runtime_error("meta ListMigrate: " + st.error_message());
    std::ostringstream oss;
    oss << "OK MIGRATE_LIST " << resp.tasks_size();
    for (const auto& t : resp.tasks()) {
      oss << ' ' << t.task_id() << ':' << t.region_id() << ':' << t.state();
    }
    return oss.str();
  }
  throw std::runtime_error(std::string("unsupported meta op: ") + op);
}

inline std::string meta_typed_rpc(std::string addr, std::string_view req, std::string_view token,
                                  int timeout_ms) {
  static const char* kMetaFallbacks[] = {"127.0.0.1:8500", "127.0.0.1:8501", "127.0.0.1:8502"};
  if (addr.rfind("0.0.0.0", 0) == 0) addr.replace(0, 7, "127.0.0.1");
  const bool local_meta = addr.rfind("127.0.0.1", 0) == 0 || addr.rfind("localhost", 0) == 0;
  int fb = 0;
  for (int i = 0; i < 12; ++i) {
    try {
      std::string body = meta_typed_call(addr, req, token, timeout_ms);
      if (body.find("not_leader") != std::string::npos) {
        auto next = admin_follow_redirect_body(body);
        if (!next.empty() && next != addr) {
          addr = std::move(next);
          continue;
        }
        if (local_meta) addr = kMetaFallbacks[static_cast<size_t>(fb++ % 3)];
        continue;
      }
      return body;
    } catch (const std::exception&) {
      if (local_meta) addr = kMetaFallbacks[static_cast<size_t>(fb++ % 3)];
      else throw;
    }
  }
  throw std::runtime_error("meta_typed_rpc exhausted");
}

}  // namespace openkv
