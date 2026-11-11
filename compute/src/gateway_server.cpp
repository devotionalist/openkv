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

// @file gateway_server.cpp
// Gateway 核心：路由 / Leader / Get·Put·Delete·Batch / 后台 watch_route。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/codec.hpp"
#include "openkv/compute/exec_line.hpp"
#include "openkv/compute/meta_client.hpp"
#include "openkv/logging.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <future>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <vector>

namespace openkv::compute {
namespace {

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

/// 撞锁 → ResolveLock 后短睡并重试。返回 true = 调用方 continue。
bool retry_after_locked(const std::string& err,
                        const std::function<void(const LockedError&)>& resolve) {
  if (!contains(err, "locked")) return false;
  try {
    resolve(parse_locked_error(err));
  } catch (...) {
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  return true;
}

/// splitting / epoch_not_match → 刷路由后短睡并重试。返回 true = 调用方 continue。
bool retry_after_route(const std::string& err, bool check_epoch,
                       const std::function<void()>& refresh) {
  if (!contains(err, "splitting") && !(check_epoch && contains(err, "epoch_not_match"))) {
    return false;
  }
  refresh();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return true;
}

}  // namespace

bool store_skip_err(const std::string& msg) {
  return contains(msg, "no_region") || contains(msg, "connection refused") ||
         contains(msg, "connection reset") || contains(msg, "transport: Error while dialing") ||
         contains(msg, "Unavailable") || contains(msg, "failed to connect");
}

uint64_t parse_epoch_expected(const std::string& msg) {
  constexpr std::string_view p = "expected=";
  const auto i = msg.find(p);
  if (i == std::string::npos) return 0;
  size_t j = i + p.size();
  uint64_t out = 0;
  bool any = false;
  while (j < msg.size() && msg[j] >= '0' && msg[j] <= '9') {
    any = true;
    out = out * 10 + static_cast<uint64_t>(msg[j] - '0');
    ++j;
  }
  return any ? out : 0;
}

uint64_t parse_cas_current_rev(const std::string& msg) {
  constexpr std::string_view p = "current_revision=";
  const auto i = msg.find(p);
  if (i == std::string::npos) return 0;
  size_t j = i + p.size();
  uint64_t out = 0;
  while (j < msg.size() && msg[j] >= '0' && msg[j] <= '9') {
    out = out * 10 + static_cast<uint64_t>(msg[j] - '0');
    ++j;
  }
  return out;
}

std::string prefix_upper_bound(std::string_view prefix) {
  if (prefix.empty()) return {};
  std::string out(prefix);
  for (int i = static_cast<int>(out.size()) - 1; i >= 0; --i) {
    auto& b = reinterpret_cast<unsigned char&>(out[static_cast<size_t>(i)]);
    if (b < 0xff) {
      ++b;
      out.resize(static_cast<size_t>(i) + 1);
      return out;
    }
  }
  return {};
}

GatewayServer::GatewayServer(std::vector<std::string> storage_addrs,
                             std::vector<std::string> meta_addrs)
    : meta_addrs_(std::move(meta_addrs)),
      ts_(meta_addrs_),
      active_ts_(std::chrono::seconds(60)) {
  size_t pool_n = 32;
  if (const char* e = std::getenv("SKV_GATEWAY_WORKERS")) {
    const auto v = std::strtoul(e, nullptr, 10);
    if (v > 0) pool_n = v;
  }
  pool_ = std::make_unique<openkv::WorkPool>(pool_n);

  RoutingTable boot;
  boot.version = 0;
  boot.shard_count = 1;
  RegionRoute rr;
  rr.region_id = 0;
  rr.epoch = 0;
  boot.regions.emplace(0, std::move(rr));
  routes_.update(std::move(boot));

  for (auto& a : storage_addrs) {
    if (a.empty()) continue;
    auto c = std::make_shared<StoreClient>(a);
    stores_.push_back(c);
    by_addr_[a] = c;
  }
}

GatewayServer::~GatewayServer() {
  stop_background();
  if (pool_) pool_->stop();
}

void GatewayServer::start_background() {
  if (bg_thread_.joinable()) return;
  bg_stop_ = false;
  bg_thread_ = std::thread([this] { route_watch_loop(); });
}

void GatewayServer::stop_background() {
  bg_stop_ = true;
  if (bg_thread_.joinable()) bg_thread_.join();
}

void GatewayServer::note_client_rpc() { client_in_.fetch_add(1, std::memory_order_relaxed); }
void GatewayServer::note_to_store() { to_store_.fetch_add(1, std::memory_order_relaxed); }
void GatewayServer::note_to_meta() { to_meta_.fetch_add(1, std::memory_order_relaxed); }

std::string GatewayServer::stats_json() const {
  const uint64_t client = client_in_.load(std::memory_order_relaxed);
  const uint64_t to_store = to_store_.load(std::memory_order_relaxed);
  const uint64_t to_meta = to_meta_.load(std::memory_order_relaxed);
  std::ostringstream oss;
  oss << "{\"requests\":" << (client + to_store + to_meta) << ",\"client\":" << client
      << ",\"gw\":" << client << ",\"to_store\":" << to_store << ",\"to_meta\":" << to_meta
      << ",\"to_store_peers\":{},\"to_meta_peers\":{}}";
  return oss.str();
}

uint64_t GatewayServer::alloc_ts(uint32_t n) {
  note_to_meta();
  return ts_.alloc(n);
}

uint32_t GatewayServer::region_of(std::string_view key) const { return routes_.shard_of(key); }

uint64_t GatewayServer::epoch_of(uint32_t region) const { return routes_.epoch(region); }

StoreClient* GatewayServer::client(const std::string& addr) {
  auto it = by_addr_.find(addr);
  if (it == by_addr_.end()) return nullptr;
  return it->second.get();
}

std::vector<StoreClient*> GatewayServer::try_order(uint32_t region) {
  std::string hint;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = leader_hint_.find(region);
    if (it != leader_hint_.end()) hint = it->second;
  }
  std::vector<StoreClient*> out;
  out.reserve(stores_.size());
  std::unordered_set<std::string> seen;
  auto add = [&](const std::string& addr) {
    if (addr.empty() || !seen.insert(addr).second) return;
    if (auto* c = client(addr)) out.push_back(c);
  };
  add(hint);
  for (const auto& addr : routes_.replica_addrs(region)) add(addr);
  for (const auto& c : stores_) add(c->addr());
  return out;
}

void GatewayServer::remember_leader(uint32_t region, const std::string& addr) {
  if (addr.empty()) return;
  std::lock_guard<std::mutex> lock(mu_);
  leader_hint_[region] = addr;
}

void GatewayServer::clear_leader_hint(uint32_t region) {
  std::lock_guard<std::mutex> lock(mu_);
  leader_hint_.erase(region);
}

void GatewayServer::refresh_routes() {
  try {
    note_to_meta();
    auto t = fetch_route(meta_addrs_);
    routes_.update(t);
    sync_epochs_async(t);
  } catch (const std::exception& e) {
    SKV_WARN << "refresh_routes: " << e.what();
  }
}

void GatewayServer::sync_epochs_async(const RoutingTable& t) {
  struct Job {
    std::string addr;
    std::string line;
  };
  std::vector<Job> jobs;
  for (const auto& [id, reg] : t.regions) {
    if (reg.epoch == 0) continue;
    std::ostringstream oss;
    oss << "ADMIN_SET_EPOCH " << id << ' ' << reg.epoch;
    const std::string line = oss.str();
    std::unordered_set<std::string> seen;
    for (const auto& rep : reg.replicas) {
      if (rep.address.empty() || !seen.insert(rep.address).second) continue;
      jobs.push_back({rep.address, line});
    }
  }
  if (jobs.empty()) return;
  std::thread([this, jobs = std::move(jobs)]() {
    for (const auto& j : jobs) {
      if (auto* c = client(j.addr)) {
        try {
          note_to_store();
          (void)c->call_raw(j.line);
        } catch (...) {
        }
      }
    }
  }).detach();
}

void GatewayServer::route_watch_loop() {
  uint64_t since = 0;
  while (!bg_stop_.load(std::memory_order_relaxed)) {
    RoutingTable t;
    bool ok = false;
    try {
      note_to_meta();
      t = watch_route(meta_addrs_, since, 4000);
      ok = true;
    } catch (...) {
      try {
        note_to_meta();
        t = fetch_route(meta_addrs_);
        ok = true;
      } catch (const std::exception& e) {
        SKV_WARN << "route watch/fetch: " << e.what();
      }
    }
    if (ok) {
      if (t.version != since) {
        routes_.update(t);
        sync_epochs_async(t);
        SKV_INFO << "route refreshed ver=" << t.version << " regions=" << t.regions.size();
      }
      since = t.version;
    }
    try {
      note_to_meta();
      report_min_start_ts(meta_addrs_, active_ts_.min());
    } catch (const std::exception& e) {
      SKV_WARN << "report min start_ts: " << e.what();
    }
  }
}

void GatewayServer::with_leader(uint32_t region, const LeaderFn& fn) {
  std::exception_ptr last;
  for (int attempt = 0; attempt < 5; ++attempt) {
    if (attempt > 0) {
      clear_leader_hint(region);
      refresh_routes();
      std::this_thread::sleep_for(std::chrono::milliseconds(40 * attempt));
    }
    std::unordered_set<std::string> tried;
    bool saw_not_leader = false;
    for (auto* st : try_order(region)) {
      const std::string addr = st->addr();
      if (!tried.insert(addr).second) continue;
      try {
        note_to_store();
        fn(*st);
        remember_leader(region, addr);
        return;
      } catch (const NotLeaderError& nl) {
        saw_not_leader = true;
        last = std::current_exception();
        if (!nl.redirect().empty()) {
          remember_leader(region, nl.redirect());
          if (auto* c = client(nl.redirect()); c && tried.insert(c->addr()).second) {
            try {
              note_to_store();
              fn(*c);
              return;
            } catch (const NotLeaderError&) {
              saw_not_leader = true;
              last = std::current_exception();
            } catch (const std::exception& e2) {
              if (store_skip_err(e2.what())) {
                if (!last) last = std::current_exception();
              } else {
                throw;
              }
            }
          }
        }
        continue;
      } catch (const std::exception& e) {
        if (store_skip_err(e.what())) {
          if (!last) last = std::current_exception();
          continue;
        }
        throw;
      }
    }
    if (!saw_not_leader) break;
  }
  if (last) std::rethrow_exception(last);
  throw std::runtime_error("no storage");
}

void GatewayServer::with_leader_epoch(uint32_t region, const LeaderEpochFn& fn) {
  std::exception_ptr last;
  uint64_t epoch = epoch_of(region);
  for (int attempt = 0; attempt < 3; ++attempt) {
    const uint64_t use = epoch;
    try {
      with_leader(region, [&](StoreClient& c) { fn(c, use); });
      return;
    } catch (const std::exception& e) {
      last = std::current_exception();
      const std::string msg = e.what();
      if (const uint64_t exp = parse_epoch_expected(msg); exp > 0) {
        routes_.bump_epoch(region, exp);
        epoch = exp;
        continue;
      }
      if (contains(msg, "splitting") || contains(msg, "epoch_not_match")) {
        refresh_routes();
        epoch = epoch_of(region);
        continue;
      }
      throw;
    }
  }
  if (last) std::rethrow_exception(last);
}

grpc::Status GatewayServer::get_stale(const std::string& key, kv::v1::GetResponse* resp) {
  const uint32_t region = region_of(key);
  std::exception_ptr last;
  for (auto* st : try_order(region)) {
    try {
      note_to_store();
      auto r = st->txn_get_opt(region, key, ~uint64_t{0}, false);
      resp->set_value(r.value);
      resp->set_found(r.found);
      resp->set_revision(r.revision);
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

grpc::Status GatewayServer::GetBody(const kv::v1::GetRequest* req,
                                kv::v1::GetResponse* resp) {
  if (!req->linearizable()) return get_stale(req->key(), resp);
  for (int attempt = 0; attempt < 4; ++attempt) {
    uint64_t ts = 0;
    try {
      ts = alloc_ts(1);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
    kv::v1::TxnGetRequest tg_req;
    tg_req.set_key(req->key());
    tg_req.set_start_ts(ts);
    kv::v1::TxnGetResponse tg;
    auto st = TxnGetBody( &tg_req, &tg);
    // TxnGet already counted; avoid double-count on internal path — acceptable for stats.
    if (!st.ok()) {
      if (contains(st.error_message(), "splitting")) {
        refresh_routes();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }
      return st;
    }
    if (tg.locked()) {
      refresh_routes();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }
    resp->set_value(tg.value());
    resp->set_found(tg.found());
    resp->set_revision(tg.revision());
    return grpc::Status::OK;
  }
  return grpc::Status(grpc::StatusCode::ABORTED, "get: splitting or locked");
}

grpc::Status GatewayServer::PutBody(const kv::v1::PutRequest* req,
                                kv::v1::PutResponse* resp) {
  std::string last_err;
  for (int attempt = 0; attempt < 4; ++attempt) {
    uint64_t start = 0;
    try {
      start = alloc_ts(2);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
    const uint64_t commit_ts = start + 1;
    active_ts_.add(start);
    int64_t expire_at = 0;
    if (req->ttl_ms() > 0) {
      expire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count() +
                  req->ttl_ms();
    }
    // TiKV 1PC：单 key 一次 Propose（data+write，无残留 lock）
    try {
      uint64_t rev = 0;
      with_leader_epoch(region_of(req->key()), [&](StoreClient& c, uint64_t epoch) {
        rev = c.one_pc(region_of(req->key()), req->key(), start, commit_ts, req->key(),
                       /*op put*/ 0, req->value(), epoch, req->cas(), req->expected_revision(),
                       expire_at);
      });
      active_ts_.remove(start);
      resp->set_ok(true);
      resp->set_revision(rev);
      return grpc::Status::OK;
    } catch (const LockedError& le) {
      active_ts_.remove(start);
      last_err = le.what();
      try {
        resolve_blocking_lock(req->key(), le);
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      continue;
    } catch (const std::exception& e) {
      active_ts_.remove(start);
      last_err = e.what();
      if (contains(last_err, "cas_failed")) {
        resp->set_ok(false);
        resp->set_cas_failed(true);
        resp->set_current_revision(parse_cas_current_rev(last_err));
        return grpc::Status::OK;
      }
      if (retry_after_route(last_err, /*check_epoch=*/true, [this] { refresh_routes(); })) {
        continue;
      }
      if (contains(last_err, "write_conflict") && !req->cas()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      }
      return grpc::Status(grpc::StatusCode::ABORTED, "put: " + last_err);
    }
  }
  if (last_err.empty()) last_err = "retries_exhausted";
  return grpc::Status(grpc::StatusCode::ABORTED, "put: " + last_err);
}

grpc::Status GatewayServer::DeleteBody(const kv::v1::DeleteRequest* req,
                                   kv::v1::DeleteResponse* resp) {
  std::string last_err;
  for (int attempt = 0; attempt < 4; ++attempt) {
    uint64_t start = 0;
    try {
      start = alloc_ts(2);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
    const uint64_t commit_ts = start + 1;
    active_ts_.add(start);
    try {
      uint64_t rev = 0;
      with_leader_epoch(region_of(req->key()), [&](StoreClient& c, uint64_t epoch) {
        rev = c.one_pc(region_of(req->key()), req->key(), start, commit_ts, req->key(),
                       /*op delete*/ 1, {}, epoch, req->cas(), req->expected_revision());
      });
      active_ts_.remove(start);
      resp->set_deleted(true);
      resp->set_revision(rev);
      return grpc::Status::OK;
    } catch (const LockedError& le) {
      active_ts_.remove(start);
      last_err = le.what();
      try {
        resolve_blocking_lock(req->key(), le);
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      continue;
    } catch (const std::exception& e) {
      active_ts_.remove(start);
      last_err = e.what();
      if (contains(last_err, "cas_failed")) {
        resp->set_deleted(false);
        resp->set_cas_failed(true);
        resp->set_current_revision(parse_cas_current_rev(last_err));
        return grpc::Status::OK;
      }
      if (retry_after_route(last_err, /*check_epoch=*/false, [this] { refresh_routes(); })) {
        continue;
      }
      return grpc::Status(grpc::StatusCode::ABORTED, "delete: " + last_err);
    }
  }
  if (last_err.empty()) last_err = "retries_exhausted";
  return grpc::Status(grpc::StatusCode::ABORTED, "delete: " + last_err);
}

grpc::Status GatewayServer::BatchGetBody(const kv::v1::BatchGetRequest* req,
                                     kv::v1::BatchGetResponse* resp) {
  for (const auto& k : req->keys()) {
    kv::v1::GetRequest gr;
    gr.set_key(k);
    gr.set_linearizable(true);
    kv::v1::GetResponse out;
    // nested Get would double-count; call logic via temporary
    auto st = GetBody( &gr, &out);
    if (!st.ok()) return st;
    *resp->add_results() = out;
  }
  return grpc::Status::OK;
}

grpc::Status GatewayServer::BatchPutBody(const kv::v1::BatchPutRequest* req,
                                     kv::v1::BatchPutResponse* resp) {
  const int n = req->items_size();
  if (n == 0) return grpc::Status::OK;

  // 有限并行（≤16），results 与请求顺序对齐。
  constexpr int kMaxParallel = 16;
  std::vector<kv::v1::PutResponse> outs(static_cast<size_t>(n));
  std::vector<grpc::Status> statuses(static_cast<size_t>(n), grpc::Status::OK);
  std::mutex gate_mu;
  std::condition_variable gate_cv;
  int running = 0;
  auto release_slot = [&] {
    {
      std::lock_guard<std::mutex> lock(gate_mu);
      --running;
    }
    gate_cv.notify_one();
  };

  std::vector<std::future<void>> futs;
  futs.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    {
      std::unique_lock<std::mutex> lock(gate_mu);
      gate_cv.wait(lock, [&] { return running < kMaxParallel; });
      ++running;
    }
    futs.push_back(std::async(std::launch::async, [this, req, i, &outs, &statuses, release_slot] {
      kv::v1::PutRequest pr;
      pr.set_key(req->items(i).key());
      pr.set_value(req->items(i).value());
      statuses[static_cast<size_t>(i)] = PutBody( &pr, &outs[static_cast<size_t>(i)]);
      release_slot();
    }));
  }
  for (auto& f : futs) f.get();
  for (int i = 0; i < n; ++i) {
    if (!statuses[static_cast<size_t>(i)].ok()) return statuses[static_cast<size_t>(i)];
    *resp->add_results() = outs[static_cast<size_t>(i)];
  }
  return grpc::Status::OK;
}

grpc::ServerUnaryReactor* GatewayServer::defer_unary(grpc::CallbackServerContext* ctx,
                                                     std::function<grpc::Status()> fn) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  pool_->post([reactor, fn = std::move(fn)]() mutable { reactor->Finish(fn()); });
  return reactor;
}

namespace {

std::string build_onepc_line(uint32_t shard, std::string_view key, uint64_t start_ts,
                             uint64_t commit_ts, std::string_view primary, uint8_t op,
                             std::string_view value, uint64_t epoch, bool cas,
                             uint64_t expected_rev, int64_t expire_at_ms) {
  std::string val_hex = openkv::to_hex(value);
  if (val_hex.empty()) val_hex = "-";
  std::ostringstream oss;
  oss << "ONEPC " << shard << ' ' << openkv::to_hex(key) << ' ' << start_ts << ' ' << commit_ts
      << ' ' << openkv::to_hex(primary) << ' ' << static_cast<unsigned>(op) << ' ' << val_hex;
  if (cas) {
    oss << ' ' << epoch << " 1 " << expected_rev;
    if (expire_at_ms != 0) oss << ' ' << expire_at_ms;
  } else if (expire_at_ms != 0) {
    oss << ' ' << epoch << ' ' << expire_at_ms;
  } else if (epoch != 0) {
    oss << ' ' << epoch;
  }
  return oss.str();
}

std::string build_txn_get_line(uint32_t shard, std::string_view key, uint64_t start_ts) {
  std::ostringstream oss;
  oss << "TXN_GET " << shard << ' ' << openkv::to_hex(key) << ' ' << start_ts << " 1";
  return oss.str();
}

bool parse_ok_onepc(std::string_view body, uint64_t* rev) {
  std::istringstream iss{std::string(body)};
  std::string a, b;
  uint64_t r = 0;
  if (!(iss >> a >> b >> r)) return false;
  if (a != "OK" || b != "ONEPC") return false;
  if (rev) *rev = r;
  return true;
}

bool parse_ok_txn_get(std::string_view body, kv::v1::GetResponse* resp) {
  std::istringstream iss{std::string(body)};
  std::string a, b, hex;
  uint64_t rev = 0;
  if (!(iss >> a >> b >> hex)) return false;
  if (a != "OK" || b != "TXN_GET") return false;
  iss >> rev;
  resp->set_revision(rev);
  if (hex == "-") {
    resp->set_found(false);
    return true;
  }
  std::string val;
  if (!openkv::from_hex(hex, &val)) return false;
  resp->set_value(std::move(val));
  resp->set_found(true);
  return true;
}

}  // namespace

void GatewayServer::with_start_commit_ts(grpc::ServerUnaryReactor* reactor,
                                         std::function<void(uint64_t, uint64_t)> cont) {
  // 与 PutBody 一致：一次 alloc_ts(2) 走本地 TSO 批缓存，禁止串行两次 Meta GetTs。
  uint64_t start = 0;
  try {
    start = alloc_ts(2);
  } catch (const std::exception& e) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what()));
    return;
  }
  cont(start, start + 1);
}

bool GatewayServer::retry_async_write(std::string_view body, const std::string& key,
                                      uint32_t region, bool retry_write_conflict,
                                      const std::function<void()>& retry) {
  const bool not_leader = contains(body, "not_leader");
  const bool locked = contains(body, "locked");
  const bool splitting = contains(body, "splitting");
  const bool epoch_mismatch = contains(body, "epoch_not_match");
  const bool write_conflict = retry_write_conflict && contains(body, "write_conflict");
  if (!not_leader && !locked && !splitting && !epoch_mismatch && !write_conflict) return false;
  if (not_leader) {
    const auto redir = parse_redirect(body);
    if (!redir.empty()) remember_leader(region, redir);
    clear_leader_hint(region);
  }
  if (splitting || epoch_mismatch) refresh_routes();
  if (locked) {
    try {
      resolve_blocking_lock(key, parse_locked_error(std::string(body)));
    } catch (...) {
    }
  }
  retry();
  return true;
}

void GatewayServer::put_async(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp,
                              grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "put: retries_exhausted"));
    return;
  }
  with_start_commit_ts(reactor, [this, req, resp, reactor, attempt](uint64_t start,
                                                                    uint64_t commit_ts) {
    active_ts_.add(start);
    int64_t expire_at = 0;
    if (req->ttl_ms() > 0) {
      expire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count() +
                  req->ttl_ms();
    }
    const uint32_t region = region_of(req->key());
    const uint64_t epoch = epoch_of(region);
    auto order = try_order(region);
    if (order.empty()) {
      active_ts_.remove(start);
      reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
      return;
    }
    const std::string addr = order.front()->addr();
    const std::string line =
        build_onepc_line(region, req->key(), start, commit_ts, req->key(), /*op put*/ 0,
                         req->value(), epoch, req->cas(), req->expected_revision(), expire_at);
    note_to_store();
    store_exec_line_async(
        addr, line, 15000,
        [this, req, resp, reactor, attempt, start, region, addr](grpc::Status sst,
                                                                std::string body) {
          active_ts_.remove(start);
          if (!sst.ok()) {
            reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
            return;
          }
          if (contains(body, "cas_failed")) {
            resp->set_ok(false);
            resp->set_cas_failed(true);
            resp->set_current_revision(parse_cas_current_rev(body));
            reactor->Finish(grpc::Status::OK);
            return;
          }
          uint64_t rev = 0;
          if (parse_ok_onepc(body, &rev)) {
            remember_leader(region, addr);
            resp->set_ok(true);
            resp->set_revision(rev);
            reactor->Finish(grpc::Status::OK);
            return;
          }
          if (retry_async_write(body, req->key(), region, /*retry_write_conflict=*/true,
                                [this, req, resp, reactor, attempt] {
                                  put_async(req, resp, reactor, attempt + 1);
                                })) {
            return;
          }
          reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "put: " + body));
        });
  });
}

void GatewayServer::delete_async(const kv::v1::DeleteRequest* req, kv::v1::DeleteResponse* resp,
                                 grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "delete: retries_exhausted"));
    return;
  }
  with_start_commit_ts(reactor, [this, req, resp, reactor, attempt](uint64_t start,
                                                                    uint64_t commit_ts) {
    active_ts_.add(start);
    const uint32_t region = region_of(req->key());
    const uint64_t epoch = epoch_of(region);
    auto order = try_order(region);
    if (order.empty()) {
      active_ts_.remove(start);
      reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
      return;
    }
    const std::string addr = order.front()->addr();
    const std::string line =
        build_onepc_line(region, req->key(), start, commit_ts, req->key(), /*op delete*/ 1, {},
                         epoch, req->cas(), req->expected_revision(), 0);
    note_to_store();
    store_exec_line_async(
        addr, line, 15000,
        [this, req, resp, reactor, attempt, start, region, addr](grpc::Status sst,
                                                                std::string body) {
          active_ts_.remove(start);
          if (!sst.ok()) {
            reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
            return;
          }
          if (contains(body, "cas_failed")) {
            resp->set_deleted(false);
            resp->set_cas_failed(true);
            resp->set_current_revision(parse_cas_current_rev(body));
            reactor->Finish(grpc::Status::OK);
            return;
          }
          uint64_t rev = 0;
          if (parse_ok_onepc(body, &rev)) {
            remember_leader(region, addr);
            resp->set_deleted(true);
            resp->set_revision(rev);
            reactor->Finish(grpc::Status::OK);
            return;
          }
          if (retry_async_write(body, req->key(), region, /*retry_write_conflict=*/false,
                                [this, req, resp, reactor, attempt] {
                                  delete_async(req, resp, reactor, attempt + 1);
                                })) {
            return;
          }
          reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "delete: " + body));
        });
  });
}

void GatewayServer::get_async(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                              grpc::ServerUnaryReactor* reactor, int attempt) {
  if (!req->linearizable()) {
    pool_->post([this, req, resp, reactor] { reactor->Finish(get_stale(req->key(), resp)); });
    return;
  }
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "get: splitting or locked"));
    return;
  }
  uint64_t ts = 0;
  try {
    ts = alloc_ts(1);
  } catch (const std::exception& e) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what()));
    return;
  }
  const uint32_t region = region_of(req->key());
  auto order = try_order(region);
  if (order.empty()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
    return;
  }
  const std::string addr = order.front()->addr();
  note_to_store();
  store_exec_line_async(
      addr, build_txn_get_line(region, req->key(), ts), 15000,
      [this, req, resp, reactor, attempt, region, addr](grpc::Status sst, std::string body) {
        if (!sst.ok()) {
          reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
          return;
        }
        if (contains(body, "locked") || contains(body, "splitting") ||
            contains(body, "not_leader")) {
          if (contains(body, "not_leader")) {
            const auto redir = parse_redirect(body);
            if (!redir.empty()) remember_leader(region, redir);
          }
          if (contains(body, "splitting")) refresh_routes();
          get_async(req, resp, reactor, attempt + 1);
          return;
        }
        if (parse_ok_txn_get(body, resp)) {
          remember_leader(region, addr);
          reactor->Finish(grpc::Status::OK);
          return;
        }
        reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "get: " + body));
      });
}

grpc::ServerUnaryReactor* GatewayServer::Get(grpc::CallbackServerContext* ctx,
                                             const kv::v1::GetRequest* req,
                                             kv::v1::GetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  get_async(req, resp, reactor, 0);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::Put(grpc::CallbackServerContext* ctx,
                                             const kv::v1::PutRequest* req,
                                             kv::v1::PutResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  put_async(req, resp, reactor, 0);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::Delete(grpc::CallbackServerContext* ctx,
                                                const kv::v1::DeleteRequest* req,
                                                kv::v1::DeleteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  delete_async(req, resp, reactor, 0);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::BatchGet(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::BatchGetRequest* req,
                                                  kv::v1::BatchGetResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return BatchGetBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::BatchPut(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::BatchPutRequest* req,
                                                  kv::v1::BatchPutResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return BatchPutBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::Scan(grpc::CallbackServerContext* ctx,
                                              const kv::v1::ScanRequest* req,
                                              kv::v1::ScanResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return ScanBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TTL(grpc::CallbackServerContext* ctx,
                                             const kv::v1::TTLRequest* req,
                                             kv::v1::TTLResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TTLBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::Expire(grpc::CallbackServerContext* ctx,
                                                const kv::v1::ExpireRequest* req,
                                                kv::v1::ExpireResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return ExpireBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::Persist(grpc::CallbackServerContext* ctx,
                                                 const kv::v1::PersistRequest* req,
                                                 kv::v1::PersistResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return PersistBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnBegin(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::TxnBeginRequest* req,
                                                  kv::v1::TxnBeginResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnBeginBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnGet(grpc::CallbackServerContext* ctx,
                                                const kv::v1::TxnGetRequest* req,
                                                kv::v1::TxnGetResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnGetBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnPrewrite(grpc::CallbackServerContext* ctx,
                                                     const kv::v1::TxnPrewriteRequest* req,
                                                     kv::v1::TxnPrewriteResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnPrewriteBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnCommit(grpc::CallbackServerContext* ctx,
                                                   const kv::v1::TxnCommitRequest* req,
                                                   kv::v1::TxnCommitResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnCommitBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnRollback(grpc::CallbackServerContext* ctx,
                                                     const kv::v1::TxnRollbackRequest* req,
                                                     kv::v1::TxnRollbackResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnRollbackBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnPessimisticLock(
    grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticLockRequest* req,
    kv::v1::TxnPessimisticLockResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnPessimisticLockBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnPessimisticRollback(
    grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticRollbackRequest* req,
    kv::v1::TxnPessimisticRollbackResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnPessimisticRollbackBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnResolveLock(grpc::CallbackServerContext* ctx,
                                                        const kv::v1::TxnResolveLockRequest* req,
                                                        kv::v1::TxnResolveLockResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnResolveLockBody(req, resp); });
}

grpc::ServerUnaryReactor* GatewayServer::TxnCheckStatus(grpc::CallbackServerContext* ctx,
                                                        const kv::v1::TxnCheckStatusRequest* req,
                                                        kv::v1::TxnCheckStatusResponse* resp) {
  return defer_unary(ctx, [this, req, resp] { return TxnCheckStatusBody(req, resp); });
}

#define GW_DS_WRAP(Name)                                                                          \
  grpc::ServerUnaryReactor* GatewayServer::Name(grpc::CallbackServerContext* ctx,                  \
                                                const kv::v1::Name##Request* req,                  \
                                                kv::v1::Name##Response* resp) {                   \
    return defer_unary(ctx, [this, req, resp] { return Name##Body(req, resp); });                 \
  }

GW_DS_WRAP(HSet)
GW_DS_WRAP(HGet)
GW_DS_WRAP(HDel)
GW_DS_WRAP(HExists)
GW_DS_WRAP(HLen)
GW_DS_WRAP(HGetAll)
GW_DS_WRAP(HKeys)
GW_DS_WRAP(HMSet)
GW_DS_WRAP(HMGet)
GW_DS_WRAP(SAdd)
GW_DS_WRAP(SRem)
GW_DS_WRAP(SIsMember)
GW_DS_WRAP(SCard)
GW_DS_WRAP(SMembers)
GW_DS_WRAP(LPush)
GW_DS_WRAP(RPush)
GW_DS_WRAP(LPop)
GW_DS_WRAP(RPop)
GW_DS_WRAP(LLen)
GW_DS_WRAP(LRange)
GW_DS_WRAP(LIndex)
GW_DS_WRAP(ZAdd)
GW_DS_WRAP(ZRem)
GW_DS_WRAP(ZScore)
GW_DS_WRAP(ZCard)
GW_DS_WRAP(ZRange)
#undef GW_DS_WRAP

}  // namespace openkv::compute

