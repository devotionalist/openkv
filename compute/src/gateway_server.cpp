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
// Gateway 核心实现。路径矩阵见 docs/flows/29-compute-code-map.md。
//
// 本文件：Lifecycle、Routing、异步热路径、RPC 入口。
// 分文件：gateway_txn / gateway_ds / gateway_watch / gateway_scan_ttl。
//
// gRPC 入口一律 StoreService / AdminService / MetaService 类型化 async。
// 错误分类靠子串（locked/not_leader/splitting/…）。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/codec.hpp"
#include "openkv/compute/meta_client.hpp"
#include "openkv/logging.hpp"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <vector>

namespace openkv::compute {
namespace {

/// 子串包含判断，用于错误分类（locked / not_leader / splitting 等）。
bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

}  // namespace

/// 判断 Store 错误是否可跳过并换副本重试（连通性 / no_region）。
bool store_skip_err(const std::string& msg) {
  return contains(msg, "no_region") || contains(msg, "connection refused") ||
         contains(msg, "connection reset") || contains(msg, "transport: Error while dialing") ||
         contains(msg, "Unavailable") || contains(msg, "failed to connect");
}

/// 从错误串解析 `expected=` 后的 epoch；解析失败返回 0。
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

/// 从 cas_failed 错误串解析 `current_revision=`。
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

/// 计算 prefix 扫描上界（末字节 +1）；全 0xff 前缀返回空串。
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

/// 构造：建 WorkPool、空路由表、记录 storage_addrs（出站走 store_rpc）。
GatewayServer::GatewayServer(std::vector<std::string> storage_addrs,
                             std::vector<std::string> meta_addrs)
    : meta_addrs_(std::move(meta_addrs)),
      storage_addrs_(std::move(storage_addrs)),
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
}

/// 停后台路由线程并关闭 WorkPool。
GatewayServer::~GatewayServer() {
  stop_background();
  if (pool_) pool_->stop();
}

/// 启动路由 watch 后台线程（幂等）。
void GatewayServer::start_background() {
  if (bg_thread_.joinable()) return;
  bg_stop_ = false;
  bg_thread_ = std::thread([this] { route_watch_loop(); });
}

/// 置停标志并 join 后台线程。
void GatewayServer::stop_background() {
  bg_stop_ = true;
  if (bg_thread_.joinable()) bg_thread_.join();
}

/// 客户端入站 RPC 计数 +1。
void GatewayServer::note_client_rpc() { client_in_.fetch_add(1, std::memory_order_relaxed); }
/// Store 出站计数 +1。
void GatewayServer::note_to_store() { to_store_.fetch_add(1, std::memory_order_relaxed); }
/// Store 出站计数 + 按 peer 分桶。
void GatewayServer::note_to_store(const std::string& peer_addr) {
  note_to_store();
  if (peer_addr.empty()) return;
  std::lock_guard<std::mutex> lock(peer_mu_);
  to_store_peers_[peer_addr] += 1;
}
/// Meta 出站计数 +1。
void GatewayServer::note_to_meta() { to_meta_.fetch_add(1, std::memory_order_relaxed); }
/// Meta 出站计数 + 按 peer 分桶。
void GatewayServer::note_to_meta(const std::string& peer_addr) {
  note_to_meta();
  if (peer_addr.empty()) return;
  std::lock_guard<std::mutex> lock(peer_mu_);
  to_meta_peers_[peer_addr] += 1;
}

namespace {
/// 将 peer→count 映射编码为紧凑 JSON object。
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

/// 汇总 client/to_store/to_meta 及分 peer 计数为 JSON。
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

/// 按路由表计算 key 所属 region。
uint32_t GatewayServer::region_of(std::string_view key) const { return routes_.shard_of(key); }

/// 读取 region 当前 epoch。
uint64_t GatewayServer::epoch_of(uint32_t region) const { return routes_.epoch(region); }

/// 生成尝试顺序：leader hint → 路由副本 → boot storage_addrs（去重）。
std::vector<std::string> GatewayServer::try_order(uint32_t region) {
  std::string hint;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = leader_hint_.find(region);
    if (it != leader_hint_.end()) hint = it->second;
  }
  std::vector<std::string> out;
  out.reserve(storage_addrs_.size() + 4);
  std::unordered_set<std::string> seen;
  auto add = [&](const std::string& addr) {
    if (addr.empty() || !seen.insert(addr).second) return;
    out.push_back(addr);
  };
  add(hint);
  for (const auto& addr : routes_.replica_addrs(region)) add(addr);
  for (const auto& addr : storage_addrs_) add(addr);
  return out;
}

/// CQ 安全：leader_hint 优先，否则 try_order 首地址。
std::optional<std::string> GatewayServer::pick_leader_addr(uint32_t region) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = leader_hint_.find(region);
    if (it != leader_hint_.end() && !it->second.empty()) return it->second;
  }
  auto order = try_order(region);
  if (order.empty()) return std::nullopt;
  return order.front();
}

/// CQ 安全 Store 调用：not_leader / redirect / splitting / epoch 回调内重试（≤4）。
void GatewayServer::with_store_async(uint32_t region, int attempt, StoreAsyncRpcFn make_call,
                                     StoreAsyncDoneFn on_done) {
  if (!make_call || !on_done) return;
  if (attempt >= 4) {
    on_done(grpc::Status(grpc::StatusCode::ABORTED, "store: retries_exhausted"), false,
            "retries_exhausted", "", "");
    return;
  }
  auto addr_opt = pick_leader_addr(region);
  if (!addr_opt) {
    on_done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"), false, "no storage", "",
            "");
    return;
  }
  const std::string addr = *addr_opt;
  note_to_store(addr);
  make_call(addr, [this, region, attempt, make_call, on_done, addr](grpc::Status sst, bool ok,
                                                                    std::string error,
                                                                    std::string redirect_to) {
    if (!sst.ok()) {
      on_done(sst, false, sst.error_message(), "", addr);
      return;
    }
    const bool not_leader = contains(error, "not_leader") || !redirect_to.empty();
    const bool splitting = contains(error, "splitting");
    const bool epoch_mismatch = contains(error, "epoch_not_match");
    if (!ok && (not_leader || splitting || epoch_mismatch)) {
      if (!redirect_to.empty()) {
        if (redirect_to == "key_not_in_region" || redirect_to == "no_region") {
          refresh_routes();
        } else {
          remember_leader(region, redirect_to);
        }
      }
      if (not_leader) clear_leader_hint(region);
      if (splitting || epoch_mismatch) {
        if (const uint64_t exp = parse_epoch_expected(error); exp > 0) {
          routes_.bump_epoch(region, exp);
        }
        refresh_routes();
      }
      with_store_async(region, attempt + 1, make_call, on_done);
      return;
    }
    if (ok) remember_leader(region, addr);
    on_done(sst, ok, std::move(error), std::move(redirect_to), addr);
  });
}

/// 缓存 region 的 leader 地址 hint。
void GatewayServer::remember_leader(uint32_t region, const std::string& addr) {
  if (addr.empty()) return;
  std::lock_guard<std::mutex> lock(mu_);
  leader_hint_[region] = addr;
}

/// 清除 region 的 leader hint（换副本重试前常用）。
void GatewayServer::clear_leader_hint(uint32_t region) {
  std::lock_guard<std::mutex> lock(mu_);
  leader_hint_.erase(region);
}

/// 从 Meta 拉取路由并 update；随后 best-effort sync_epochs_async。
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

// Best-effort：把 Meta 路由里的 epoch 推到各 Store（AdminService.SetEpoch）。
// detach 无 join——stop_background 不等待。
void GatewayServer::sync_epochs_async(const RoutingTable& t) {
  struct Job {
    std::string addr;
    uint32_t region_id{0};
    uint64_t epoch{0};
  };
  std::vector<Job> jobs;
  for (const auto& [id, reg] : t.regions) {
    if (reg.epoch == 0) continue;
    std::unordered_set<std::string> seen;
    for (const auto& rep : reg.replicas) {
      if (rep.address.empty() || !seen.insert(rep.address).second) continue;
      jobs.push_back({rep.address, id, reg.epoch});
    }
  }
  if (jobs.empty()) return;
  std::thread([this, jobs = std::move(jobs)]() {
    for (const auto& j : jobs) {
      try {
        note_to_store(j.addr);
        store_admin_set_epoch_async(j.addr, j.region_id, j.epoch, 5000,
                                    [](grpc::Status, std::string) {});
      } catch (...) {
      }
    }
  }).detach();
}

/// 后台循环：watch/fetch 路由、同步 epoch、上报 min start_ts。
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

/// 异步申请 start/commit_ts（alloc_async(2)），失败走 on_err，成功回调 cont。
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

// Async 热路径重试入口：用回调续跑而非 sleep+continue。
// locked 时走 resolve_blocking_lock_async，完成后再 retry。
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
    resolve_blocking_lock_async(key, parse_locked_error(std::string(body)),
                                [retry](grpc::Status) { retry(); });
    return true;
  }
  retry();
  return true;
}

/// Put 异步入口：把 reactor->Finish 包成 done，转 put_async_cb。
void GatewayServer::put_async(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp,
                              grpc::ServerUnaryReactor* reactor, int attempt) {
  put_async_cb(req, resp, attempt, [reactor](grpc::Status st) { reactor->Finish(st); });
}

// 事务平面 Put：alloc_async(2) → ONEPC Propose；attempt 封顶 4（含 not_leader/locked/conflict）。
// Put 异步路径：全程回调，不占用 WorkPool。
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
        const std::string addr = order.front();
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

/// 事务平面 Delete 异步：alloc_async → ONEPC(op=delete)；可重试错误经 retry_async_write 续跑。
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
        const std::string addr = order.front();
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

/// Get 异步入口：reactor->Finish 转 get_async_cb。
void GatewayServer::get_async(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                              grpc::ServerUnaryReactor* reactor, int attempt) {
  get_async_cb(req, resp, attempt, [reactor](grpc::Status st) { reactor->Finish(st); });
}

/// 事务平面 Get 异步：stale → store_get_async(linearizable=false)；线性 alloc_async + Get。
void GatewayServer::get_async_cb(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                                 int attempt, std::function<void(grpc::Status)> done) {
  if (attempt >= 4) {
    done(grpc::Status(grpc::StatusCode::ABORTED, "get: splitting or locked"));
    return;
  }
  const bool linearizable = req->linearizable();
  auto run_get = [this, req, resp, attempt, done, linearizable](uint64_t ts) {
    const uint32_t region = region_of(req->key());
    auto addr_opt = pick_leader_addr(region);
    if (!addr_opt) {
      done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
      return;
    }
    const std::string addr = *addr_opt;
    note_to_store(addr);
    store_get_async(
        addr, region, req->key(), linearizable, ts, 15000,
        [this, req, resp, attempt, region, addr, done](grpc::Status sst, StoreGetResult out) {
          if (!sst.ok()) {
            done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
            return;
          }
          if (out.locked || !out.redirect_to.empty() || contains(out.error, "splitting") ||
              contains(out.error, "locked") || contains(out.error, "not_leader")) {
            if (!out.redirect_to.empty()) {
              if (out.redirect_to == "key_not_in_region" || out.redirect_to == "no_region") {
                refresh_routes();
              } else {
                remember_leader(region, out.redirect_to);
                clear_leader_hint(region);
              }
            }
            if (contains(out.error, "splitting") || contains(out.error, "epoch_not_match")) {
              refresh_routes();
            }
            if (out.locked || contains(out.error, "locked")) {
              LockedError le(out.lock_primary, out.lock_ts,
                             out.error.empty() ? "locked" : out.error);
              resolve_blocking_lock_async(req->key(), le,
                                         [this, req, resp, attempt, done](grpc::Status) {
                                           get_async_cb(req, resp, attempt + 1, done);
                                         });
              return;
            }
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
  };
  if (!linearizable) {
    run_get(std::numeric_limits<uint64_t>::max());
    return;
  }
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(1, [run_get = std::move(run_get), done](grpc::Status tst, uint64_t ts) {
    if (!tst.ok()) {
      done(tst);
      return;
    }
    run_get(ts);
  });
}

/// gRPC Get 入口：记计数后启动 get_async。
grpc::ServerUnaryReactor* GatewayServer::Get(grpc::CallbackServerContext* ctx,
                                             const kv::v1::GetRequest* req,
                                             kv::v1::GetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  get_async(req, resp, reactor, 0);
  return reactor;
}

/// gRPC Put 入口：记计数后启动 put_async。
grpc::ServerUnaryReactor* GatewayServer::Put(grpc::CallbackServerContext* ctx,
                                             const kv::v1::PutRequest* req,
                                             kv::v1::PutResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  put_async(req, resp, reactor, 0);
  return reactor;
}

/// gRPC Delete 入口：记计数后启动 delete_async。
grpc::ServerUnaryReactor* GatewayServer::Delete(grpc::CallbackServerContext* ctx,
                                                const kv::v1::DeleteRequest* req,
                                                kv::v1::DeleteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  delete_async(req, resp, reactor, 0);
  return reactor;
}

namespace {

/// 拼 ProposePlain 错误体，必要时附加 redirect=。
std::string propose_err_body(const ProposePlainResult& out) {
  std::string body = out.error;
  if (!out.redirect_to.empty() && body.find("redirect=") == std::string::npos) {
    body += " redirect=" + out.redirect_to;
  }
  return body;
}

/// 判断 Plain Propose 是否 CAS 失败（字段或错误串）。
bool propose_cas_failed(const ProposePlainResult& out) {
  return out.cas_failed || contains(out.error, "cas_failed");
}

/// 取 CAS 当前 revision（优先结果字段，否则解析错误串）。
uint64_t propose_cas_current_rev(const ProposePlainResult& out) {
  return out.current_revision != 0 ? out.current_revision : parse_cas_current_rev(out.error);
}

}  // namespace

/// Raw 平面 Put 异步：store_propose_plain_async，无时间戳；可重试错误回调续跑（≤4）。
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
  const std::string addr = order.front();
  ProposePlainArgs args;
  args.shard_id = region;
  args.is_delete = false;
  args.key = req->key();
  args.value = req->value();
  args.ttl_ms = req->ttl_ms();
  args.epoch = epoch_of(region);
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

/// Raw 平面 Delete 异步：Plain Propose is_delete；流程同 raw_put_async。
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
  const std::string addr = order.front();
  ProposePlainArgs args;
  args.shard_id = region;
  args.is_delete = true;
  args.key = req->key();
  args.epoch = epoch_of(region);
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

/// Raw 平面 Get 异步：store_get_async(start_ts=0)；redirect/no_region 刷路由后重试。
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
  const std::string addr = order.front();
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

/// gRPC RawGet 入口。
grpc::ServerUnaryReactor* GatewayServer::RawGet(grpc::CallbackServerContext* ctx,
                                                const kv::v1::RawGetRequest* req,
                                                kv::v1::RawGetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_get_async(req, resp, reactor, 0);
  return reactor;
}

/// gRPC RawPut 入口。
grpc::ServerUnaryReactor* GatewayServer::RawPut(grpc::CallbackServerContext* ctx,
                                                const kv::v1::RawPutRequest* req,
                                                kv::v1::RawPutResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_put_async(req, resp, reactor, 0);
  return reactor;
}

/// gRPC RawDelete 入口。
grpc::ServerUnaryReactor* GatewayServer::RawDelete(grpc::CallbackServerContext* ctx,
                                                   const kv::v1::RawDeleteRequest* req,
                                                   kv::v1::RawDeleteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  raw_delete_async(req, resp, reactor, 0);
  return reactor;
}

/// 异步批量 Put：共享 State + pump，≤16 并行 put_async_cb，全部完成后按序 Finish。
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
    grpc::Status fail = grpc::Status::OK;
    {
      std::lock_guard<std::mutex> lock(st->mu);
      if (st->finished) return;
      if (st->done < static_cast<int>(st->outs.size())) return;
      st->finished = true;
      for (size_t i = 0; i < st->statuses.size(); ++i) {
        if (!st->statuses[i].ok()) {
          fail = st->statuses[i];
          break;
        }
        *st->resp->add_results() = st->outs[i];
      }
    }
    st->reactor->Finish(fail.ok() ? grpc::Status::OK : fail);
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

/// 异步批量 Get：同 batch_put_async 的 pump 模型，子请求走 get_async_cb。
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
    grpc::Status fail = grpc::Status::OK;
    {
      std::lock_guard<std::mutex> lock(st->mu);
      if (st->finished) return;
      if (st->done < static_cast<int>(st->outs.size())) return;
      st->finished = true;
      for (size_t i = 0; i < st->statuses.size(); ++i) {
        if (!st->statuses[i].ok()) {
          fail = st->statuses[i];
          break;
        }
        *st->resp->add_results() = st->outs[i];
      }
    }
    st->reactor->Finish(fail.ok() ? grpc::Status::OK : fail);
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

/// gRPC BatchGet 入口 → batch_get_async。
grpc::ServerUnaryReactor* GatewayServer::BatchGet(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::BatchGetRequest* req,
                                                  kv::v1::BatchGetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  batch_get_async(req, resp, reactor);
  return reactor;
}

/// gRPC BatchPut 入口 → batch_put_async。
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
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  scan_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TTL(grpc::CallbackServerContext* ctx,
                                             const kv::v1::TTLRequest* req,
                                             kv::v1::TTLResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  ttl_async(req, resp, 0, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::Expire(grpc::CallbackServerContext* ctx,
                                                const kv::v1::ExpireRequest* req,
                                                kv::v1::ExpireResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  expire_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::Persist(grpc::CallbackServerContext* ctx,
                                                 const kv::v1::PersistRequest* req,
                                                 kv::v1::PersistResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  persist_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnBegin(grpc::CallbackServerContext* ctx,
                                                  const kv::v1::TxnBeginRequest* req,
                                                  kv::v1::TxnBeginResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_begin_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnGet(grpc::CallbackServerContext* ctx,
                                                const kv::v1::TxnGetRequest* req,
                                                kv::v1::TxnGetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_get_async(req, resp, 0, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnPrewrite(grpc::CallbackServerContext* ctx,
                                                     const kv::v1::TxnPrewriteRequest* req,
                                                     kv::v1::TxnPrewriteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_prewrite_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnCommit(grpc::CallbackServerContext* ctx,
                                                   const kv::v1::TxnCommitRequest* req,
                                                   kv::v1::TxnCommitResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_commit_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnRollback(grpc::CallbackServerContext* ctx,
                                                     const kv::v1::TxnRollbackRequest* req,
                                                     kv::v1::TxnRollbackResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_rollback_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnPessimisticLock(
    grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticLockRequest* req,
    kv::v1::TxnPessimisticLockResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_pessimistic_lock_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnPessimisticRollback(
    grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticRollbackRequest* req,
    kv::v1::TxnPessimisticRollbackResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_pessimistic_rollback_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnResolveLock(grpc::CallbackServerContext* ctx,
                                                        const kv::v1::TxnResolveLockRequest* req,
                                                        kv::v1::TxnResolveLockResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_resolve_lock_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

grpc::ServerUnaryReactor* GatewayServer::TxnCheckStatus(grpc::CallbackServerContext* ctx,
                                                        const kv::v1::TxnCheckStatusRequest* req,
                                                        kv::v1::TxnCheckStatusResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  note_client_rpc();
  txn_check_status_async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });
  return reactor;
}

// 展开 DS RPC 入口：各 Name → Name##Async。
#define GW_DS_WRAP(Name)                                                                          \
  grpc::ServerUnaryReactor* GatewayServer::Name(grpc::CallbackServerContext* ctx,                  \
                                                const kv::v1::Name##Request* req,                  \
                                                kv::v1::Name##Response* resp) {                   \
    auto* reactor = ctx->DefaultReactor();                                                        \
    note_client_rpc();                                                                            \
    Name##Async(req, resp, [reactor](grpc::Status st) { reactor->Finish(st); });                  \
    return reactor;                                                                               \
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

