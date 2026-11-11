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
void GatewayServer::note_to_store(const std::string& peer_addr) {
  note_to_store();
  if (peer_addr.empty()) return;
  std::lock_guard<std::mutex> lock(peer_mu_);
  to_store_peers_[peer_addr] += 1;
}
void GatewayServer::note_to_meta() { to_meta_.fetch_add(1, std::memory_order_relaxed); }
void GatewayServer::note_to_meta(const std::string& peer_addr) {
  note_to_meta();
  if (peer_addr.empty()) return;
  std::lock_guard<std::mutex> lock(peer_mu_);
  to_meta_peers_[peer_addr] += 1;
}

namespace {
std::string encode_peer_json(const std::unordered_map<std::string, uint64_t>& m) {
  std::ostringstream oss;
  oss << '{';
  bool first = true;
  for (const auto& [k, v] : m) {
    if (!first) oss << ',';
    first = false;
    oss << '"' << k << "\":" << v;
  }
  oss << '}';
  return oss.str();
}
}  // namespace

std::string GatewayServer::stats_json() const {
  const uint64_t client = client_in_.load(std::memory_order_relaxed);
  const uint64_t to_store = to_store_.load(std::memory_order_relaxed);
  const uint64_t to_meta = to_meta_.load(std::memory_order_relaxed);
  std::unordered_map<std::string, uint64_t> store_peers;
  std::unordered_map<std::string, uint64_t> meta_peers;
  {
    std::lock_guard<std::mutex> lock(peer_mu_);
    store_peers = to_store_peers_;
    meta_peers = to_meta_peers_;
  }
  std::ostringstream oss;
  oss << "{\"requests\":" << (client + to_store + to_meta) << ",\"client\":" << client
      << ",\"gw\":" << client << ",\"to_store\":" << to_store << ",\"to_meta\":" << to_meta
      << ",\"to_store_peers\":" << encode_peer_json(store_peers)
      << ",\"to_meta_peers\":" << encode_peer_json(meta_peers) << '}';
  return oss.str();
}

uint64_t GatewayServer::alloc_ts(uint32_t n) {
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
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
    note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
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
          note_to_store(j.addr);
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
    const std::string meta_peer = meta_addrs_.empty() ? std::string{} : meta_addrs_.front();
    try {
      note_to_meta(meta_peer);
      t = watch_route(meta_addrs_, since, 4000);
      ok = true;
    } catch (...) {
      try {
        note_to_meta(meta_peer);
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
      note_to_meta(meta_peer);
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
        note_to_store(addr);
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
              note_to_store(c->addr());
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
      note_to_store(st->addr());
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
    // 1PC：单 key 一次 Propose（data+write，无残留 lock）
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

void GatewayServer::with_start_commit_ts(std::function<void(grpc::Status)> on_err,
                                         std::function<void(uint64_t, uint64_t)> cont) {
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(2, [on_err = std::move(on_err), cont = std::move(cont)](grpc::Status st,
                                                                             uint64_t start) {
    if (!st.ok()) {
      if (on_err) on_err(st);
      return;
    }
    cont(start, start + 1);
  });
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
  put_async_cb(req, resp, attempt, [reactor](grpc::Status st) { reactor->Finish(st); });
}

void GatewayServer::put_async_cb(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp,
                                 int attempt, std::function<void(grpc::Status)> done) {
  if (attempt >= 4) {
    done(grpc::Status(grpc::StatusCode::ABORTED, "put: retries_exhausted"));
    return;
  }
  with_start_commit_ts(
      done, [this, req, resp, attempt, done](uint64_t start, uint64_t commit_ts) {
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
          done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
          return;
        }
        const std::string addr = order.front()->addr();
        ProposeOnePcArgs args;
        args.shard_id = region;
        args.key = req->key();
        args.value = req->value();
        args.start_ts = start;
        args.commit_ts = commit_ts;
        args.primary = req->key();
        args.mvcc_op = 0;
        args.epoch = epoch;
        args.cas = req->cas();
        args.expected_revision = req->expected_revision();
        args.expire_at_ms = expire_at;
        note_to_store(addr);
        store_propose_onepc_async(
            addr, std::move(args), 15000,
            [this, req, resp, attempt, start, region, addr, done](grpc::Status sst,
                                                                  ProposeOnePcResult out) {
              active_ts_.remove(start);
              if (!sst.ok()) {
                done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
                return;
              }
              if (out.cas_failed || contains(out.error, "cas_failed")) {
                resp->set_ok(false);
                resp->set_cas_failed(true);
                resp->set_current_revision(out.current_revision != 0
                                               ? out.current_revision
                                               : parse_cas_current_rev(out.error));
                done(grpc::Status::OK);
                return;
              }
              if (out.ok) {
                remember_leader(region, addr);
                resp->set_ok(true);
                resp->set_revision(out.revision);
                done(grpc::Status::OK);
                return;
              }
              std::string body = out.error;
              if (!out.redirect_to.empty() && body.find("redirect=") == std::string::npos) {
                body += " redirect=" + out.redirect_to;
              }
              if (retry_async_write(body, req->key(), region, /*retry_write_conflict=*/true,
                                    [this, req, resp, attempt, done] {
                                      put_async_cb(req, resp, attempt + 1, done);
                                    })) {
                return;
              }
              done(grpc::Status(grpc::StatusCode::ABORTED, "put: " + body));
            });
      });
}

void GatewayServer::delete_async(const kv::v1::DeleteRequest* req, kv::v1::DeleteResponse* resp,
                                 grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "delete: retries_exhausted"));
    return;
  }
  with_start_commit_ts(
      [reactor](grpc::Status st) { reactor->Finish(st); },
      [this, req, resp, reactor, attempt](uint64_t start, uint64_t commit_ts) {
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
        ProposeOnePcArgs args;
        args.shard_id = region;
        args.key = req->key();
        args.start_ts = start;
        args.commit_ts = commit_ts;
        args.primary = req->key();
        args.mvcc_op = 1;
        args.epoch = epoch;
        args.cas = req->cas();
        args.expected_revision = req->expected_revision();
        note_to_store(addr);
        store_propose_onepc_async(
            addr, std::move(args), 15000,
            [this, req, resp, reactor, attempt, start, region, addr](grpc::Status sst,
                                                                    ProposeOnePcResult out) {
              active_ts_.remove(start);
              if (!sst.ok()) {
                reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
                return;
              }
              if (out.cas_failed || contains(out.error, "cas_failed")) {
                resp->set_deleted(false);
                resp->set_cas_failed(true);
                resp->set_current_revision(out.current_revision != 0
                                               ? out.current_revision
                                               : parse_cas_current_rev(out.error));
                reactor->Finish(grpc::Status::OK);
                return;
              }
              if (out.ok) {
                remember_leader(region, addr);
                resp->set_deleted(true);
                resp->set_revision(out.revision);
                reactor->Finish(grpc::Status::OK);
                return;
              }
              std::string body = out.error;
              if (!out.redirect_to.empty() && body.find("redirect=") == std::string::npos) {
                body += " redirect=" + out.redirect_to;
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
  get_async_cb(req, resp, attempt, [reactor](grpc::Status st) { reactor->Finish(st); });
}

void GatewayServer::get_async_cb(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                                 int attempt, std::function<void(grpc::Status)> done) {
  if (!req->linearizable()) {
    pool_->post([this, req, resp, done] { done(get_stale(req->key(), resp)); });
    return;
  }
  if (attempt >= 4) {
    done(grpc::Status(grpc::StatusCode::ABORTED, "get: splitting or locked"));
    return;
  }
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(1, [this, req, resp, attempt, done](grpc::Status tst, uint64_t ts) {
    if (!tst.ok()) {
      done(tst);
      return;
    }
    const uint32_t region = region_of(req->key());
    auto order = try_order(region);
    if (order.empty()) {
      done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
      return;
    }
    const std::string addr = order.front()->addr();
    note_to_store(addr);
    store_get_async(
        addr, region, req->key(), /*linearizable=*/true, ts, 15000,
        [this, req, resp, attempt, region, addr, done](grpc::Status sst, StoreGetResult out) {
          if (!sst.ok()) {
            done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
            return;
          }
          if (out.locked || !out.redirect_to.empty() || contains(out.error, "splitting") ||
              contains(out.error, "locked")) {
            if (!out.redirect_to.empty()) {
              if (out.redirect_to == "key_not_in_region" || out.redirect_to == "no_region") {
                refresh_routes();
              } else {
                remember_leader(region, out.redirect_to);
                clear_leader_hint(region);
              }
            }
            if (contains(out.error, "splitting")) refresh_routes();
            get_async_cb(req, resp, attempt + 1, done);
            return;
          }
          if (!out.error.empty() && !out.found) {
            done(grpc::Status(grpc::StatusCode::ABORTED, "get: " + out.error));
            return;
          }
          remember_leader(region, addr);
          resp->set_found(out.found);
          if (out.found) resp->set_value(std::move(out.value));
          resp->set_revision(out.revision);
          done(grpc::Status::OK);
        });
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

namespace {

std::string propose_err_body(const ProposePlainResult& out) {
  std::string body = out.error;
  if (!out.redirect_to.empty() && body.find("redirect=") == std::string::npos) {
    body += " redirect=" + out.redirect_to;
  }
  return body;
}

bool propose_cas_failed(const ProposePlainResult& out) {
  return out.cas_failed || contains(out.error, "cas_failed");
}

uint64_t propose_cas_current_rev(const ProposePlainResult& out) {
  return out.current_revision != 0 ? out.current_revision : parse_cas_current_rev(out.error);
}

}  // namespace

void GatewayServer::raw_put_async(const kv::v1::RawPutRequest* req, kv::v1::RawPutResponse* resp,
                                  grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "raw_put: retries_exhausted"));
    return;
  }
  const uint32_t region = region_of(req->key());
  auto order = try_order(region);
  if (order.empty()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
    return;
  }
  const std::string addr = order.front()->addr();
  ProposePlainArgs args;
  args.shard_id = region;
  args.is_delete = false;
  args.key = req->key();
  args.value = req->value();
  args.ttl_ms = req->ttl_ms();
  args.cas = req->cas();
  args.expected_revision = req->expected_revision();
  note_to_store(addr);
  store_propose_plain_async(
      addr, std::move(args), 15000,
      [this, req, resp, reactor, attempt, region, addr](grpc::Status sst, ProposePlainResult out) {
        if (!sst.ok()) {
          reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
          return;
        }
        if (propose_cas_failed(out)) {
          resp->set_ok(false);
          resp->set_cas_failed(true);
          resp->set_current_revision(propose_cas_current_rev(out));
          reactor->Finish(grpc::Status::OK);
          return;
        }
        if (out.ok) {
          remember_leader(region, addr);
          resp->set_ok(true);
          resp->set_revision(out.revision);
          reactor->Finish(grpc::Status::OK);
          return;
        }
        const std::string body = propose_err_body(out);
        if (retry_async_write(body, req->key(), region, /*retry_write_conflict=*/false,
                              [this, req, resp, reactor, attempt] {
                                raw_put_async(req, resp, reactor, attempt + 1);
                              })) {
          return;
        }
        reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "raw_put: " + body));
      });
}

void GatewayServer::raw_delete_async(const kv::v1::RawDeleteRequest* req,
                                     kv::v1::RawDeleteResponse* resp,
                                     grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "raw_delete: retries_exhausted"));
    return;
  }
  const uint32_t region = region_of(req->key());
  auto order = try_order(region);
  if (order.empty()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
    return;
  }
  const std::string addr = order.front()->addr();
  ProposePlainArgs args;
  args.shard_id = region;
  args.is_delete = true;
  args.key = req->key();
  args.cas = req->cas();
  args.expected_revision = req->expected_revision();
  note_to_store(addr);
  store_propose_plain_async(
      addr, std::move(args), 15000,
      [this, req, resp, reactor, attempt, region, addr](grpc::Status sst, ProposePlainResult out) {
        if (!sst.ok()) {
          reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
          return;
        }
        if (propose_cas_failed(out)) {
          resp->set_deleted(false);
          resp->set_cas_failed(true);
          resp->set_current_revision(propose_cas_current_rev(out));
          reactor->Finish(grpc::Status::OK);
          return;
        }
        if (out.ok) {
          remember_leader(region, addr);
          resp->set_deleted(true);
          resp->set_revision(out.revision);
          reactor->Finish(grpc::Status::OK);
          return;
        }
        const std::string body = propose_err_body(out);
        if (retry_async_write(body, req->key(), region, /*retry_write_conflict=*/false,
                              [this, req, resp, reactor, attempt] {
                                raw_delete_async(req, resp, reactor, attempt + 1);
                              })) {
          return;
        }
        reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "raw_delete: " + body));
      });
}

void GatewayServer::raw_get_async(const kv::v1::RawGetRequest* req, kv::v1::RawGetResponse* resp,
                                  grpc::ServerUnaryReactor* reactor, int attempt) {
  if (attempt >= 4) {
    reactor->Finish(grpc::Status(grpc::StatusCode::ABORTED, "raw_get: retries_exhausted"));
    return;
  }
  const uint32_t region = region_of(req->key());
  auto order = try_order(region);
  if (order.empty()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
    return;
  }
  // 非线性：可打任意副本；线性：leader 优先（try_order 已排）。
  const std::string addr = order.front()->addr();
  note_to_store(addr);
  store_get_async(
      addr, region, req->key(), req->linearizable(), /*start_ts=*/0, 15000,
      [this, req, resp, reactor, attempt, region, addr](grpc::Status sst, StoreGetResult out) {
        if (!sst.ok()) {
          reactor->Finish(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
          return;
        }
        if (!out.redirect_to.empty()) {
          if (out.redirect_to == "key_not_in_region" || out.redirect_to == "no_region") {
            refresh_routes();
          } else {
            remember_leader(region, out.redirect_to);
            clear_leader_hint(region);
          }
          raw_get_async(req, resp, reactor, attempt + 1);
          return;
        }
        remember_leader(region, addr);
        resp->set_found(out.found);
        if (out.found) resp->set_value(std::move(out.value));
        resp->set_revision(out.revision);
        reactor->Finish(grpc::Status::OK);
      });
}

grpc::ServerUnaryReactor* GatewayServer::RawGet(grpc::CallbackServerContext* ctx,
                                                const kv::v1::RawGetRequest* req,
                                                kv::v1::RawGetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_get_async(req, resp, reactor, 0);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::RawPut(grpc::CallbackServerContext* ctx,
                                                const kv::v1::RawPutRequest* req,
                                                kv::v1::RawPutResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_put_async(req, resp, reactor, 0);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::RawDelete(grpc::CallbackServerContext* ctx,
                                                   const kv::v1::RawDeleteRequest* req,
                                                   kv::v1::RawDeleteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_delete_async(req, resp, reactor, 0);
  return reactor;
}

void GatewayServer::batch_put_async(const kv::v1::BatchPutRequest* req,
                                    kv::v1::BatchPutResponse* resp,
                                    grpc::ServerUnaryReactor* reactor) {
  const int n = req->items_size();
  if (n == 0) {
    reactor->Finish(grpc::Status::OK);
    return;
  }
  constexpr int kMaxParallel = 16;
  struct State {
    std::mutex mu;
    int inflight{0};
    int next{0};
    int done{0};
    bool finished{false};
    std::vector<std::unique_ptr<kv::v1::PutRequest>> reqs;
    std::vector<kv::v1::PutResponse> outs;
    std::vector<grpc::Status> statuses;
    grpc::ServerUnaryReactor* reactor{nullptr};
    kv::v1::BatchPutResponse* resp{nullptr};
    const kv::v1::BatchPutRequest* batch{nullptr};
    GatewayServer* self{nullptr};
  };
  auto st = std::make_shared<State>();
  st->reactor = reactor;
  st->resp = resp;
  st->batch = req;
  st->self = this;
  st->outs.resize(static_cast<size_t>(n));
  st->statuses.assign(static_cast<size_t>(n), grpc::Status::OK);
  st->reqs.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    st->reqs[static_cast<size_t>(i)] = std::make_unique<kv::v1::PutRequest>();
    st->reqs[static_cast<size_t>(i)]->set_key(req->items(i).key());
    st->reqs[static_cast<size_t>(i)]->set_value(req->items(i).value());
  }

  auto finish_if_done = [st]() {
    if (st->finished) return;
    if (st->done < static_cast<int>(st->outs.size())) return;
    st->finished = true;
    for (size_t i = 0; i < st->statuses.size(); ++i) {
      if (!st->statuses[i].ok()) {
        st->reactor->Finish(st->statuses[i]);
        return;
      }
      *st->resp->add_results() = st->outs[i];
    }
    st->reactor->Finish(grpc::Status::OK);
  };

  auto pump = std::make_shared<std::function<void()>>();
  *pump = [st, finish_if_done, pump]() {
    for (;;) {
      int i = -1;
      {
        std::lock_guard<std::mutex> lock(st->mu);
        if (st->finished) return;
        if (st->next >= static_cast<int>(st->outs.size()) || st->inflight >= kMaxParallel) {
          return;
        }
        i = st->next++;
        ++st->inflight;
      }
      auto* pr = st->reqs[static_cast<size_t>(i)].get();
      auto* out = &st->outs[static_cast<size_t>(i)];
      st->self->put_async_cb(pr, out, 0, [st, i, finish_if_done, pump](grpc::Status s) {
        {
          std::lock_guard<std::mutex> lock(st->mu);
          st->statuses[static_cast<size_t>(i)] = s;
          --st->inflight;
          ++st->done;
        }
        finish_if_done();
        (*pump)();
      });
    }
  };
  (*pump)();
}

void GatewayServer::batch_get_async(const kv::v1::BatchGetRequest* req,
                                    kv::v1::BatchGetResponse* resp,
                                    grpc::ServerUnaryReactor* reactor) {
  const int n = req->keys_size();
  if (n == 0) {
    reactor->Finish(grpc::Status::OK);
    return;
  }
  constexpr int kMaxParallel = 16;
  struct State {
    std::mutex mu;
    int inflight{0};
    int next{0};
    int done{0};
    bool finished{false};
    std::vector<std::unique_ptr<kv::v1::GetRequest>> reqs;
    std::vector<kv::v1::GetResponse> outs;
    std::vector<grpc::Status> statuses;
    grpc::ServerUnaryReactor* reactor{nullptr};
    kv::v1::BatchGetResponse* resp{nullptr};
    GatewayServer* self{nullptr};
  };
  auto st = std::make_shared<State>();
  st->reactor = reactor;
  st->resp = resp;
  st->self = this;
  st->outs.resize(static_cast<size_t>(n));
  st->statuses.assign(static_cast<size_t>(n), grpc::Status::OK);
  st->reqs.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    st->reqs[static_cast<size_t>(i)] = std::make_unique<kv::v1::GetRequest>();
    st->reqs[static_cast<size_t>(i)]->set_key(req->keys(i));
    st->reqs[static_cast<size_t>(i)]->set_linearizable(req->linearizable());
  }

  auto finish_if_done = [st]() {
    if (st->finished) return;
    if (st->done < static_cast<int>(st->outs.size())) return;
    st->finished = true;
    for (size_t i = 0; i < st->statuses.size(); ++i) {
      if (!st->statuses[i].ok()) {
        st->reactor->Finish(st->statuses[i]);
        return;
      }
      *st->resp->add_results() = st->outs[i];
    }
    st->reactor->Finish(grpc::Status::OK);
  };

  auto pump = std::make_shared<std::function<void()>>();
  *pump = [st, finish_if_done, pump]() {
    for (;;) {
      int i = -1;
      {
        std::lock_guard<std::mutex> lock(st->mu);
        if (st->finished) return;
        if (st->next >= static_cast<int>(st->outs.size()) || st->inflight >= kMaxParallel) {
          return;
        }
        i = st->next++;
        ++st->inflight;
      }
      auto* gr = st->reqs[static_cast<size_t>(i)].get();
      auto* out = &st->outs[static_cast<size_t>(i)];
      st->self->get_async_cb(gr, out, 0, [st, i, finish_if_done, pump](grpc::Status s) {
        {
          std::lock_guard<std::mutex> lock(st->mu);
          st->statuses[static_cast<size_t>(i)] = s;
          --st->inflight;
          ++st->done;
        }
        finish_if_done();
        (*pump)();
      });
    }
  };
  (*pump)();
}

grpc::ServerUnaryReactor* GatewayServer::BatchGet(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::BatchGetRequest* req,
                                                  kv::v1::BatchGetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  batch_get_async(req, resp, reactor);
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::BatchPut(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::BatchPutRequest* req,
                                                  kv::v1::BatchPutResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  batch_put_async(req, resp, reactor);
  return reactor;
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

