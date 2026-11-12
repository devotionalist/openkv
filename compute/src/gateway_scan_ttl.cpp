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
// Scan / TTL / Expire / Persist：StoreService async + get/put_async RMW。

#include "openkv/compute/gateway_server.hpp"

#include <limits>
#include <memory>
#include <utility>

namespace openkv::compute {
namespace {

constexpr uint32_t kScanDefaultLimit = 256;
constexpr uint32_t kScanHardLimit = 4096;

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

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

void GatewayServer::scan_async(const kv::v1::ScanRequest* req, kv::v1::ScanResponse* resp,
                               std::function<void(grpc::Status)> done) {
  uint32_t limit = req->limit();
  if (limit == 0) limit = kScanDefaultLimit;
  if (limit > kScanHardLimit) limit = kScanHardLimit;

  std::string start;
  std::string end;
  resolve_scan_range(*req, &start, &end);
  std::string after = req->start_after();
  const bool linearizable = req->linearizable();

  struct State {
    GatewayServer* self;
    const kv::v1::ScanRequest* req;
    kv::v1::ScanResponse* resp;
    std::function<void(grpc::Status)> done;
    uint32_t limit{0};
    std::string start;
    std::string end;
    std::string after;
    std::string loc;
    bool linearizable{true};
    uint64_t start_ts{0};
    std::shared_ptr<std::function<void()>> step;
  };
  auto st = std::make_shared<State>();
  st->self = this;
  st->req = req;
  st->resp = resp;
  st->done = std::move(done);
  st->limit = limit;
  st->start = std::move(start);
  st->end = std::move(end);
  st->after = after;
  st->loc = after.empty() ? st->start : after;
  st->linearizable = linearizable;
  st->step = std::make_shared<std::function<void()>>();

  auto begin_scan = [st]() {
    *st->step = [st]() {
      if (static_cast<uint32_t>(st->resp->pairs_size()) >= st->limit) {
        st->done(grpc::Status::OK);
        return;
      }
      const uint32_t remain =
          st->limit - static_cast<uint32_t>(st->resp->pairs_size());
      const uint32_t region_id = st->self->region_of(st->loc);
      auto snap = st->self->routes_.snapshot();
      if (snap.regions.empty()) {
        st->self->refresh_routes();
        snap = st->self->routes_.snapshot();
        if (snap.regions.empty()) {
          st->done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "scan: no route table"));
          return;
        }
      }
      auto it = snap.regions.find(region_id);
      if (it == snap.regions.end()) {
        st->self->refresh_routes();
        snap = st->self->routes_.snapshot();
        it = snap.regions.find(region_id);
        if (it == snap.regions.end()) {
          st->done(grpc::Status(grpc::StatusCode::NOT_FOUND, "scan: no region"));
          return;
        }
      }
      const auto& reg = it->second;
      const std::string seg_start = max_start(st->start, reg.range.start_key);
      const std::string seg_end = min_end(st->end, reg.range.end_key);
      if (!seg_end.empty() && !seg_start.empty() && seg_start >= seg_end) {
        if (reg.range.end_key.empty()) {
          st->done(grpc::Status::OK);
          return;
        }
        st->loc = reg.range.end_key;
        if (!st->end.empty() && st->loc >= st->end) {
          st->done(grpc::Status::OK);
          return;
        }
        (*st->step)();
        return;
      }
      if (!st->after.empty() && !reg.range.end_key.empty() && st->after >= reg.range.end_key) {
        st->loc = reg.range.end_key;
        if (!st->end.empty() && st->loc >= st->end) {
          st->done(grpc::Status::OK);
          return;
        }
        (*st->step)();
        return;
      }

      StoreScanArgs args;
      args.shard_id = region_id;
      args.start_key = seg_start;
      args.end_key = seg_end;
      args.start_after = st->after;
      args.start_ts = st->start_ts;
      args.limit = remain;
      args.keys_only = st->req->keys_only();
      args.linearizable = st->linearizable;
      const std::string reg_end = reg.range.end_key;
      auto part = std::make_shared<StoreScanResult>();
      st->self->with_store_async(
          region_id, 0,
          [args = std::move(args), part](
              const std::string& addr,
              std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
            store_scan_async(addr, std::move(args), 15000,
                             [on_rpc = std::move(on_rpc), part](grpc::Status sst,
                                                                StoreScanResult out) {
                               if (!sst.ok()) {
                                 on_rpc(sst, false, sst.error_message(), "");
                                 return;
                               }
                               if (!out.redirect_to.empty() || contains(out.error, "not_leader") ||
                                   contains(out.error, "splitting") ||
                                   contains(out.error, "no_region")) {
                                 on_rpc(grpc::Status::OK, false,
                                        out.error.empty() ? "not_leader" : out.error,
                                        out.redirect_to);
                                 return;
                               }
                               *part = std::move(out);
                               on_rpc(grpc::Status::OK, true, part->error, "");
                             });
          },
          [st, part, reg_end](grpc::Status sst, bool ok, std::string err, std::string, std::string) {
            if (!sst.ok()) {
              st->done(sst);
              return;
            }
            if (!ok) {
              if (contains(err, "splitting") || contains(err, "no_region")) {
                st->self->refresh_routes();
                (*st->step)();
                return;
              }
              st->done(grpc::Status(grpc::StatusCode::INTERNAL, err));
              return;
            }
            if (!part->error.empty()) {
              st->done(grpc::Status(grpc::StatusCode::INTERNAL, part->error));
              return;
            }
            for (const auto& p : part->pairs) {
              auto* sp = st->resp->add_pairs();
              sp->set_key(p.key);
              sp->set_value(p.value);
              sp->set_revision(p.revision);
            }
            if (part->truncated) {
              st->resp->set_truncated(true);
              st->resp->set_next_key(part->next_key);
              if (st->resp->next_key().empty() && !part->pairs.empty()) {
                st->resp->set_next_key(part->pairs.back().key);
              }
              st->done(grpc::Status::OK);
              return;
            }
            if (reg_end.empty()) {
              st->done(grpc::Status::OK);
              return;
            }
            st->loc = reg_end;
            if (!st->end.empty() && st->loc >= st->end) {
              st->done(grpc::Status::OK);
              return;
            }
            (*st->step)();
          });
    };
    (*st->step)();
  };

  if (linearizable) {
    note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
    ts_.alloc_async(1, [st, begin_scan](grpc::Status tst, uint64_t ts) {
      if (!tst.ok()) {
        st->done(tst);
        return;
      }
      st->start_ts = ts;
      begin_scan();
    });
  } else {
    st->start_ts = std::numeric_limits<uint64_t>::max();
    begin_scan();
  }
}

void GatewayServer::ttl_async(const kv::v1::TTLRequest* req, kv::v1::TTLResponse* resp, int attempt,
                              std::function<void(grpc::Status)> done) {
  if (attempt >= 4) {
    done(grpc::Status(grpc::StatusCode::ABORTED, "ttl: locked or splitting"));
    return;
  }
  const bool linearizable = req->linearizable();
  auto run = [this, req, resp, attempt, done, linearizable](uint64_t ts) {
    const uint32_t region = region_of(req->key());
    auto addr_opt = pick_leader_addr(region);
    if (!addr_opt) {
      done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
      return;
    }
    note_to_store(*addr_opt);
    store_ttl_async(
        *addr_opt, region, req->key(), ts, linearizable, 15000,
        [this, req, resp, attempt, region, done](grpc::Status sst, StoreTtlResult out) {
          if (!sst.ok()) {
            done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
            return;
          }
          if (out.locked || contains(out.error, "locked")) {
            LockedError le(out.lock_primary, out.lock_ts, out.error.empty() ? "locked" : out.error);
            resolve_blocking_lock_async(req->key(), le, [this, req, resp, attempt, done](grpc::Status) {
              ttl_async(req, resp, attempt + 1, done);
            });
            return;
          }
          if (!out.redirect_to.empty() || contains(out.error, "not_leader") ||
              contains(out.error, "splitting")) {
            if (!out.redirect_to.empty() && out.redirect_to != "no_region" &&
                out.redirect_to != "key_not_in_region") {
              remember_leader(region, out.redirect_to);
            }
            refresh_routes();
            clear_leader_hint(region);
            ttl_async(req, resp, attempt + 1, done);
            return;
          }
          if (!out.error.empty()) {
            done(grpc::Status(grpc::StatusCode::INTERNAL, out.error));
            return;
          }
          resp->set_ttl_ms(out.ttl_ms);
          done(grpc::Status::OK);
        });
  };
  if (!linearizable) {
    run(std::numeric_limits<uint64_t>::max());
    return;
  }
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(1, [run = std::move(run), done](grpc::Status tst, uint64_t ts) {
    if (!tst.ok()) {
      done(tst);
      return;
    }
    run(ts);
  });
}

void GatewayServer::expire_async(const kv::v1::ExpireRequest* req, kv::v1::ExpireResponse* resp,
                                 std::function<void(grpc::Status)> done) {
  // GetRequest 必须 heap 持有：get_async_cb 在 alloc/store 回调后才读 key。
  auto gr = std::make_shared<kv::v1::GetRequest>();
  gr->set_key(req->key());
  gr->set_linearizable(true);
  auto get_resp = std::make_shared<kv::v1::GetResponse>();
  get_async_cb(gr.get(), get_resp.get(), 0,
               [this, req, resp, gr, get_resp, done = std::move(done)](grpc::Status st) {
                 if (!st.ok()) {
                   done(st);
                   return;
                 }
                 if (!get_resp->found()) {
                   resp->set_ok(false);
                   resp->set_found(false);
                   done(grpc::Status::OK);
                   return;
                 }
                 int64_t ttl = req->ttl_ms();
                 if (ttl < 0) ttl = 0;
                 auto put_req = std::make_shared<kv::v1::PutRequest>();
                 put_req->set_key(req->key());
                 put_req->set_value(get_resp->value());
                 put_req->set_ttl_ms(ttl);
                 auto put_resp = std::make_shared<kv::v1::PutResponse>();
                 put_async_cb(put_req.get(), put_resp.get(), 0,
                              [resp, put_req, put_resp, done](grpc::Status pst) {
                                if (!pst.ok()) {
                                  done(pst);
                                  return;
                                }
                                if (!put_resp->ok()) {
                                  resp->set_ok(false);
                                  resp->set_found(true);
                                  done(grpc::Status::OK);
                                  return;
                                }
                                resp->set_ok(true);
                                resp->set_found(true);
                                resp->set_revision(put_resp->revision());
                                done(grpc::Status::OK);
                              });
               });
}

void GatewayServer::persist_async(const kv::v1::PersistRequest* req, kv::v1::PersistResponse* resp,
                                  std::function<void(grpc::Status)> done) {
  auto er = std::make_shared<kv::v1::ExpireRequest>();
  er->set_key(req->key());
  er->set_ttl_ms(0);
  auto ex = std::make_shared<kv::v1::ExpireResponse>();
  expire_async(er.get(), ex.get(), [resp, er, ex, done = std::move(done)](grpc::Status st) {
    if (!st.ok()) {
      done(st);
      return;
    }
    resp->set_ok(ex->ok());
    resp->set_found(ex->found());
    resp->set_revision(ex->revision());
    done(grpc::Status::OK);
  });
}

}  // namespace openkv::compute
