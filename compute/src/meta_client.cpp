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

// @file meta_client.cpp
// MetaService 类型化客户端：GetTs / GetRoutingTable / WatchRoutingTable / ReportMinStartTs。

#include "openkv/compute/meta_client.hpp"

#include "openkv/codec.hpp"
#include "openkv/compute/grpc_channel.hpp"

#include "meta/v1/meta.pb.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

namespace openkv::compute {
namespace {

std::vector<std::string> split_ws(std::string_view s) {
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

RoutingTable from_pb_routing(const ::meta::v1::RoutingTable& pb) {
  RoutingTable t;
  t.version = pb.version();
  t.shard_count = pb.shard_count();
  for (const auto& sh : pb.shards()) {
    RegionRoute rr;
    rr.region_id = sh.shard_id();
    rr.range.start_key = sh.start_key();
    rr.range.end_key = sh.end_key();
    rr.epoch = sh.epoch() == 0 ? 1 : sh.epoch();
    for (const auto& r : sh.replicas()) {
      ReplicaEndpoint ep;
      ep.node_id = r.node_id();
      ep.address = r.address();
      ep.is_leader = r.is_leader();
      rr.replicas.push_back(std::move(ep));
    }
    t.regions.emplace(rr.region_id, std::move(rr));
  }
  if (t.regions.empty()) {
    RegionRoute rr;
    rr.region_id = 0;
    rr.epoch = 1;
    t.regions.emplace(0, std::move(rr));
  }
  if (t.shard_count == 0) t.shard_count = static_cast<uint32_t>(t.regions.size());
  return t;
}

/// 单点 GetTs；失败清缓存并抛错。count=0/1 取一个。
std::pair<uint64_t, uint32_t> meta_get_ts_range_one(const std::string& addr, uint32_t count,
                                                    int timeout_ms) {
  auto stub = cached_meta_stub(addr);
  ::meta::v1::GetTsRequest req;
  if (count == 0) count = 1;
  req.set_count(count);
  ::meta::v1::GetTsResponse resp;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->GetTs(&ctx, req, &resp);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("meta GetTs: ") + st.error_message());
  }
  if (resp.ts() == 0) throw std::runtime_error("meta GetTs: empty ts");
  uint32_t got = resp.count() == 0 ? 1 : resp.count();
  return {resp.ts(), got};
}

uint64_t meta_get_ts_one(const std::string& addr, int timeout_ms) {
  return meta_get_ts_range_one(addr, 1, timeout_ms).first;
}

/// 多 seed × 多 hop：跟随 UNAVAILABLE / 换址重试 GetTs。
uint64_t meta_get_ts_with_retry(const std::vector<std::string>& addrs, int timeout_ms) {
  std::string last_err = "no meta";
  int backoff_ms = 20;
  for (int attempt = 0; attempt < 3; ++attempt) {
    for (const auto& seed : addrs) {
      try {
        return meta_get_ts_one(seed, timeout_ms);
      } catch (const std::exception& e) {
        last_err = e.what();
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    if (backoff_ms < 500) backoff_ms *= 2;
  }
  throw std::runtime_error(last_err);
}

RoutingTable meta_get_routing_one(const std::string& addr, int timeout_ms) {
  auto stub = cached_meta_stub(addr);
  ::meta::v1::GetRoutingTableRequest req;
  ::meta::v1::RoutingTable resp;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->GetRoutingTable(&ctx, req, &resp);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("meta GetRoutingTable: ") + st.error_message());
  }
  return from_pb_routing(resp);
}

RoutingTable meta_watch_routing_one(const std::string& addr, uint64_t since, int timeout_ms) {
  auto stub = cached_meta_stub(addr);
  ::meta::v1::WatchRoutingRequest req;
  req.set_from_version(since);
  req.set_timeout_ms(timeout_ms);
  // 流式 RPC 可能长等，deadline 略大于 timeout。
  const int deadline = timeout_ms > 0 ? timeout_ms + 2000 : 7000;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, deadline);
  std::unique_ptr<grpc::ClientReader<::meta::v1::RoutingTable>> reader(
      stub->WatchRoutingTable(&ctx, req));
  ::meta::v1::RoutingTable tbl;
  if (!reader->Read(&tbl)) {
    auto st = reader->Finish();
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("meta WatchRoutingTable: ") +
                             (st.ok() ? "empty stream" : st.error_message()));
  }
  auto st = reader->Finish();
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("meta WatchRoutingTable finish: ") + st.error_message());
  }
  return from_pb_routing(tbl);
}

}  // namespace

RoutingTable parse_route(std::string_view resp) {
  auto parts = split_ws(resp);
  if (parts.size() < 4 || parts[0] != "OK" || parts[1] != "ROUTE") {
    throw std::runtime_error(std::string("bad route: ") + std::string(resp));
  }
  RoutingTable t;
  t.version = std::stoull(parts[2]);
  const int n = std::stoi(parts[3]);
  for (size_t pi = 5; pi < parts.size(); ++pi) {
    const auto& tok = parts[pi];
    std::vector<std::string> colon;
    {
      size_t start = 0;
      int fields = 0;
      for (size_t i = 0; i < tok.size() && fields < 4; ++i) {
        if (tok[i] == ':') {
          colon.emplace_back(tok.substr(start, i - start));
          start = i + 1;
          ++fields;
        }
      }
      colon.emplace_back(tok.substr(start));
    }
    if (colon.size() < 4) continue;
    RegionRoute rr;
    rr.region_id = static_cast<openkv::RegionId>(std::stoul(colon[0]));
    if (!colon[1].empty()) {
      if (!from_hex(colon[1], &rr.range.start_key)) {
        throw std::runtime_error("bad route start hex");
      }
    }
    if (!colon[2].empty()) {
      if (!from_hex(colon[2], &rr.range.end_key)) {
        throw std::runtime_error("bad route end hex");
      }
    }
    std::string rest;
    if (colon.size() >= 5) {
      rr.epoch = std::stoull(colon[3]);
      rest = colon[4];
    } else {
      rr.epoch = 1;
      rest = colon[3];
    }
    size_t pos = 0;
    bool first = true;
    while (pos <= rest.size()) {
      size_t comma = rest.find(',', pos);
      std::string item =
          comma == std::string::npos ? rest.substr(pos) : rest.substr(pos, comma - pos);
      if (first) {
        first = false;
      } else if (!item.empty()) {
        const bool leader = item.find("*L") != std::string::npos;
        auto scrub = [](std::string s) {
          for (const char* tag : {"*L", "*N"}) {
            for (;;) {
              auto p = s.find(tag);
              if (p == std::string::npos) break;
              s.erase(p, 2);
            }
          }
          return s;
        };
        item = scrub(std::move(item));
        auto at = item.find('@');
        if (at != std::string::npos) {
          ReplicaEndpoint ep;
          ep.node_id = item.substr(0, at);
          ep.address = item.substr(at + 1);
          ep.is_leader = leader;
          rr.replicas.push_back(std::move(ep));
        }
      }
      if (comma == std::string::npos) break;
      pos = comma + 1;
    }
    t.regions.emplace(rr.region_id, rr);
    if (static_cast<int>(t.regions.size()) >= n) break;
  }
  if (t.regions.empty()) {
    RegionRoute rr;
    rr.region_id = 0;
    rr.epoch = 1;
    t.regions.emplace(0, std::move(rr));
  }
  t.shard_count = static_cast<uint32_t>(t.regions.size());
  return t;
}

std::pair<uint64_t, uint32_t> alloc_ts_range(const std::vector<std::string>& meta_addrs,
                                             uint32_t count) {
  if (count == 0) count = 1;
  std::string last_err = "no meta";
  for (const auto& seed : meta_addrs) {
    try {
      return meta_get_ts_range_one(seed, count, 3000);
    } catch (const std::exception& e) {
      last_err = e.what();
    }
  }
  throw std::runtime_error(last_err);
}

RoutingTable fetch_route(const std::vector<std::string>& meta_addrs, int timeout_ms) {
  std::string last = "no meta";
  for (const auto& addr : meta_addrs) {
    try {
      return meta_get_routing_one(addr, timeout_ms);
    } catch (const std::exception& e) {
      last = e.what();
    }
  }
  throw std::runtime_error(last);
}

RoutingTable watch_route(const std::vector<std::string>& meta_addrs, uint64_t since,
                         int timeout_ms) {
  if (timeout_ms <= 0) timeout_ms = 5000;
  std::string last = "no meta";
  for (const auto& addr : meta_addrs) {
    try {
      return meta_watch_routing_one(addr, since, timeout_ms);
    } catch (const std::exception& e) {
      last = e.what();
    }
  }
  throw std::runtime_error(last);
}

void report_min_start_ts(const std::vector<std::string>& meta_addrs, uint64_t min_start,
                         std::string_view reporter) {
  std::string last = "no meta";
  for (const auto& addr : meta_addrs) {
    try {
      auto stub = cached_meta_stub(addr);
      ::meta::v1::ReportMinStartTsRequest req;
      req.set_min_start_ts(min_start);
      req.set_node_id(reporter.empty() ? "gateway" : std::string(reporter));
      ::meta::v1::SimpleResponse resp;
      grpc::ClientContext ctx;
      attach_deadline_and_token(&ctx, 3000);
      auto st = stub->ReportMinStartTs(&ctx, req, &resp);
      if (!st.ok()) {
        drop_cached_channel(addr);
        last = st.error_message();
        continue;
      }
      if (!resp.ok()) {
        last = resp.error().empty() ? resp.message() : resp.error();
        continue;
      }
      return;
    } catch (const std::exception& e) {
      last = e.what();
    }
  }
  throw std::runtime_error(std::string("report min start: ") + last);
}

void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb) {
  if (!cb) return;
  auto stub = cached_meta_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::meta::v1::GetTsRequest>();
  req->set_count(1);
  auto resp = std::make_shared<::meta::v1::GetTsResponse>();
  stub->async()->GetTs(ctx.get(), req.get(), resp.get(),
                       [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
                         if (!st.ok()) drop_cached_channel(addr);
                         cb(st, resp->ts());
                       });
}

void meta_alloc_ts_range_async(
    const std::vector<std::string>& meta_addrs, uint32_t count, int timeout_ms,
    std::function<void(grpc::Status, uint64_t start, uint32_t got)> cb) {
  if (!cb) return;
  if (count == 0) count = 1;
  if (meta_addrs.empty()) {
    cb(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no meta"), 0, 0);
    return;
  }
  struct State {
    std::vector<std::string> addrs;
    size_t seed_idx{0};
    uint32_t count{1};
    int timeout_ms{3000};
    std::function<void(grpc::Status, uint64_t, uint32_t)> cb;
    std::shared_ptr<std::function<void()>> try_seed;
  };
  auto st = std::make_shared<State>();
  st->addrs = meta_addrs;
  st->count = count;
  st->timeout_ms = timeout_ms;
  st->cb = std::move(cb);
  st->try_seed = std::make_shared<std::function<void()>>();
  *st->try_seed = [st]() {
    if (st->seed_idx >= st->addrs.size()) {
      st->cb(grpc::Status(grpc::StatusCode::UNAVAILABLE, "tso: all meta failed"), 0, 0);
      return;
    }
    const std::string addr = st->addrs[st->seed_idx];
    auto stub = cached_meta_stub(addr);
    auto ctx = std::make_shared<grpc::ClientContext>();
    attach_deadline_and_token(ctx.get(), st->timeout_ms);
    auto req = std::make_shared<::meta::v1::GetTsRequest>();
    req->set_count(st->count);
    auto resp = std::make_shared<::meta::v1::GetTsResponse>();
    auto next = st->try_seed;
    stub->async()->GetTs(
        ctx.get(), req.get(), resp.get(),
        [st, addr, stub, ctx, req, resp, next](grpc::Status gst) {
          if (!gst.ok() || resp->ts() == 0) {
            if (!gst.ok()) drop_cached_channel(addr);
            ++st->seed_idx;
            (*next)();
            return;
          }
          const uint32_t got = resp->count() == 0 ? 1 : resp->count();
          st->cb(grpc::Status::OK, resp->ts(), got);
        });
  };
  (*st->try_seed)();
}

}  // namespace openkv::compute
