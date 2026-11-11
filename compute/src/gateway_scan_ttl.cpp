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

// @file gateway_scan_ttl.cpp
// Scan 跨 Region 扇出；TTL / Expire / Persist。

#include "openkv/compute/gateway_server.hpp"

#include <chrono>
#include <limits>
#include <thread>

namespace openkv::compute {
namespace {

constexpr uint32_t kScanDefaultLimit = 256;
constexpr uint32_t kScanHardLimit = 4096;

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

// empty = unbounded（对齐 Go nil []byte）：end 的 empty=+inf，start 的 empty=-inf。
std::string min_end(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  if (b.empty()) return a;
  return a <= b ? a : b;
}

std::string max_start(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  if (b.empty()) return a;
  return a >= b ? a : b;
}

void resolve_scan_range(const kv::v1::ScanRequest& req, std::string* start, std::string* end) {
  *start = req.start_key();
  *end = req.end_key();
  if (!req.prefix().empty()) {
    *start = req.prefix();
    *end = prefix_upper_bound(req.prefix());
  }
}

}  // namespace

TxnScanResult GatewayServer::scan_one_region(uint32_t region_id, std::string_view start,
                                             std::string_view end, std::string_view after,
                                             uint64_t start_ts, uint32_t limit, bool keys_only,
                                             bool linearizable) {
  if (linearizable) {
    TxnScanResult part;
    with_leader(region_id, [&](StoreClient& c) {
      part = c.txn_scan_opt(region_id, start, end, after, start_ts, limit, keys_only, true);
    });
    return part;
  }
  std::exception_ptr last;
  for (auto* st : try_order(region_id)) {
    try {
      note_to_store(st->addr());
      return st->txn_scan_opt(region_id, start, end, after, start_ts, limit, keys_only, false);
    } catch (...) {
      last = std::current_exception();
    }
  }
  if (last) std::rethrow_exception(last);
  throw std::runtime_error("scan: no storage");
}

grpc::Status GatewayServer::ScanBody(const kv::v1::ScanRequest* req,
                                 kv::v1::ScanResponse* resp) {
  uint32_t limit = req->limit();
  if (limit == 0) limit = kScanDefaultLimit;
  if (limit > kScanHardLimit) limit = kScanHardLimit;

  std::string start;
  std::string end;
  resolve_scan_range(*req, &start, &end);
  std::string after = req->start_after();
  const bool linearizable = req->linearizable();

  uint64_t start_ts = 0;
  if (linearizable) {
    try {
      start_ts = alloc_ts(1);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
  } else {
    start_ts = std::numeric_limits<uint64_t>::max();
  }

  std::string loc = start;
  if (!after.empty()) loc = after;

  while (static_cast<uint32_t>(resp->pairs_size()) < limit) {
    const uint32_t remain = limit - static_cast<uint32_t>(resp->pairs_size());
    const uint32_t region_id = region_of(loc);
    auto snap = routes_.snapshot();
    if (snap.regions.empty()) {
      refresh_routes();
      snap = routes_.snapshot();
      if (snap.regions.empty()) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "scan: no route table");
      }
    }
    auto it = snap.regions.find(region_id);
    if (it == snap.regions.end()) {
      refresh_routes();
      snap = routes_.snapshot();
      it = snap.regions.find(region_id);
      if (it == snap.regions.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "scan: no region");
      }
    }
    const auto& reg = it->second;
    const std::string seg_start = max_start(start, reg.range.start_key);
    const std::string seg_end = min_end(end, reg.range.end_key);

    if (!seg_end.empty() && !seg_start.empty() && seg_start >= seg_end) {
      if (reg.range.end_key.empty()) break;
      loc = reg.range.end_key;
      if (!end.empty() && loc >= end) break;
      continue;
    }
    if (!after.empty() && !reg.range.end_key.empty() && after >= reg.range.end_key) {
      loc = reg.range.end_key;
      if (!end.empty() && loc >= end) break;
      continue;
    }

    TxnScanResult part;
    try {
      part = scan_one_region(region_id, seg_start, seg_end, after, start_ts, remain, req->keys_only(),
                             linearizable);
    } catch (const std::exception& e) {
      if (contains(e.what(), "splitting") || contains(e.what(), "no_region")) {
        refresh_routes();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }
      return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    for (const auto& p : part.pairs) {
      auto* sp = resp->add_pairs();
      sp->set_key(p.key);
      sp->set_value(p.value);
      sp->set_revision(p.revision);
    }
    if (part.truncated) {
      resp->set_truncated(true);
      resp->set_next_key(part.next_key);
      if (resp->next_key().empty() && !part.pairs.empty()) {
        resp->set_next_key(part.pairs.back().key);
      }
      return grpc::Status::OK;
    }
    if (reg.range.end_key.empty()) break;
    loc = reg.range.end_key;
    if (!end.empty() && loc >= end) break;
  }
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ttl_stale(const std::string& key, kv::v1::TTLResponse* resp) {
  const uint32_t region = region_of(key);
  std::exception_ptr last;
  for (auto* st : try_order(region)) {
    try {
      note_to_store(st->addr());
      const int64_t ttl = st->txn_ttl_opt(region, key, std::numeric_limits<uint64_t>::max(), false);
      resp->set_ttl_ms(ttl);
      return grpc::Status::OK;
    } catch (...) {
      last = std::current_exception();
    }
  }
  if (last) {
    try {
      std::rethrow_exception(last);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
  }
  return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage");
}

grpc::Status GatewayServer::TTLBody(const kv::v1::TTLRequest* req,
                                kv::v1::TTLResponse* resp) {
  if (!req->linearizable()) return ttl_stale(req->key(), resp);
  for (int attempt = 0; attempt < 4; ++attempt) {
    uint64_t ts = 0;
    try {
      ts = alloc_ts(1);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
    const uint32_t region = region_of(req->key());
    try {
      int64_t ttl = 0;
      with_leader(region, [&](StoreClient& c) {
        ttl = c.txn_ttl_opt(region, req->key(), ts, true);
      });
      resp->set_ttl_ms(ttl);
      return grpc::Status::OK;
    } catch (const LockedError& le) {
      try {
        resolve_blocking_lock(req->key(), le);
      } catch (const std::exception& re) {
        if (contains(re.what(), "locked")) {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, re.what());
      }
      continue;
    } catch (const std::exception& e) {
      if (contains(e.what(), "splitting")) {
        refresh_routes();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }
      return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
  }
  return grpc::Status(grpc::StatusCode::ABORTED, "ttl: locked or splitting");
}

grpc::Status GatewayServer::get_for_rewrite(const std::string& key, std::string* value, bool* found,
                                            uint64_t* rev) {
  kv::v1::GetRequest gr;
  gr.set_key(key);
  gr.set_linearizable(true);
  kv::v1::GetResponse out;
  auto st = GetBody( &gr, &out);
  if (!st.ok()) return st;
  *value = out.value();
  *found = out.found();
  *rev = out.revision();
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ExpireBody(const kv::v1::ExpireRequest* req,
                                   kv::v1::ExpireResponse* resp) {
  std::string val;
  bool found = false;
  uint64_t rev = 0;
  auto st = get_for_rewrite(req->key(), &val, &found, &rev);
  if (!st.ok()) return st;
  if (!found) {
    resp->set_ok(false);
    resp->set_found(false);
    return grpc::Status::OK;
  }
  int64_t ttl = req->ttl_ms();
  if (ttl < 0) ttl = 0;
  kv::v1::PutRequest pr;
  pr.set_key(req->key());
  pr.set_value(val);
  pr.set_ttl_ms(ttl);
  kv::v1::PutResponse pout;
  st = PutBody( &pr, &pout);
  if (!st.ok()) return st;
  if (!pout.ok()) {
    resp->set_ok(false);
    resp->set_found(true);
    return grpc::Status::OK;
  }
  resp->set_ok(true);
  resp->set_found(true);
  resp->set_revision(pout.revision());
  return grpc::Status::OK;
}

grpc::Status GatewayServer::PersistBody(const kv::v1::PersistRequest* req,
                                    kv::v1::PersistResponse* resp) {
  kv::v1::ExpireRequest er;
  er.set_key(req->key());
  er.set_ttl_ms(0);
  kv::v1::ExpireResponse ex;
  auto st = ExpireBody(&er, &ex);
  if (!st.ok()) return st;
  resp->set_ok(ex.ok());
  resp->set_found(ex.found());
  resp->set_revision(ex.revision());
  return grpc::Status::OK;
}

}  // namespace openkv::compute
