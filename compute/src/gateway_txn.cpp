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

// @file gateway_txn.cpp
// Percolator Txn* 异步路径：StoreService Propose/Get/CheckTxnStatus。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/logging.hpp"

#include <memory>
#include <string>
#include <utility>

namespace openkv::compute {
namespace {

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

ProposeTxnArgs make_txn_args(uint32_t region, ProposeOp op, std::string key, uint64_t start_ts,
                             uint64_t epoch, std::string primary = {}) {
  ProposeTxnArgs a;
  a.shard_id = region;
  a.op = op;
  a.key = std::move(key);
  a.start_ts = start_ts;
  a.epoch = epoch;
  a.primary = std::move(primary);
  return a;
}

}  // namespace

void GatewayServer::resolve_blocking_lock_async(const std::string& key, const LockedError& le,
                                                std::function<void(grpc::Status)> cont) {
  if (!cont) return;
  if (le.primary().empty() || le.lock_ts() == 0) {
    cont(grpc::Status(grpc::StatusCode::ABORTED, "locked"));
    return;
  }
  const uint32_t primary_region = region_of(le.primary());
  with_store_async(
      primary_region, 0,
      [primary_region, primary = le.primary(), lock_ts = le.lock_ts()](
          const std::string& addr,
          std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) {
        store_check_txn_status_async(
            addr, primary_region, primary, lock_ts, 15000,
            [on_rpc = std::move(on_rpc)](grpc::Status sst, StoreCheckTxnResult out) {
              if (!sst.ok()) {
                on_rpc(sst, false, sst.error_message(), "");
                return;
              }
              if (!out.redirect_to.empty() || contains(out.error, "not_leader")) {
                on_rpc(grpc::Status::OK, false, out.error.empty() ? "not_leader" : out.error,
                       out.redirect_to);
                return;
              }
              on_rpc(grpc::Status::OK, true, out.status + "\n" + std::to_string(out.commit_ts),
                     "");
            });
      },
      [this, key, le, cont = std::move(cont)](grpc::Status sst, bool ok, std::string body,
                                              std::string, std::string) {
        if (!sst.ok()) {
          cont(sst);
          return;
        }
        if (!ok) {
          cont(grpc::Status(grpc::StatusCode::ABORTED, body.empty() ? "locked" : body));
          return;
        }
        const auto nl = body.find('\n');
        std::string status = nl == std::string::npos ? body : body.substr(0, nl);
        uint64_t commit_ts = 0;
        if (nl != std::string::npos) {
          try {
            commit_ts = std::stoull(body.substr(nl + 1));
          } catch (...) {
          }
        }
        auto resolve_key = [this](const std::string& k, uint64_t lock_ts, uint64_t cts,
                                  std::function<void(grpc::Status)> next) {
          const uint32_t region = region_of(k);
          const uint64_t epoch = epoch_of(region);
          with_store_async(
              region, 0,
              [region, k, lock_ts, cts, epoch](
                  const std::string& addr,
                  std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) {
                ProposeTxnArgs args =
                    make_txn_args(region, ProposeOp::kResolveLock, k, lock_ts, epoch);
                args.commit_ts = cts;
                store_resolve_lock_async(addr, std::move(args), 15000,
                                         [on_rpc = std::move(on_rpc)](grpc::Status sst,
                                                                      ProposeResult out) {
                                           if (!sst.ok()) {
                                             on_rpc(sst, false, sst.error_message(), "");
                                             return;
                                           }
                                           on_rpc(grpc::Status::OK, out.ok, out.error,
                                                  out.redirect_to);
                                         });
              },
              [next = std::move(next)](grpc::Status st, bool ok2, std::string err, std::string,
                                       std::string) {
                if (!st.ok()) {
                  next(st);
                  return;
                }
                if (!ok2) {
                  next(grpc::Status(grpc::StatusCode::ABORTED, err.empty() ? "resolve_failed" : err));
                  return;
                }
                next(grpc::Status::OK);
              });
        };
        if (status == "committed") {
          resolve_key(key, le.lock_ts(), commit_ts, cont);
          return;
        }
        if (status == "rolledback" || status == "expired") {
          auto after_primary = [resolve_key, key, le, cont](grpc::Status) {
            resolve_key(key, le.lock_ts(), 0, cont);
          };
          if (status == "expired") {
            resolve_key(le.primary(), le.lock_ts(), 0, after_primary);
          } else {
            after_primary(grpc::Status::OK);
          }
          return;
        }
        cont(grpc::Status(grpc::StatusCode::ABORTED, "locked"));
      });
}

void GatewayServer::async_resolve_secondaries(uint64_t start_ts, uint64_t commit_ts,
                                              std::vector<std::string> keys) {
  if (keys.empty()) return;
  for (const auto& key : keys) {
    const uint32_t region = region_of(key);
    const uint64_t epoch = epoch_of(region);
    ProposeTxnArgs args = make_txn_args(region, ProposeOp::kResolveLock, key, start_ts, epoch);
    args.commit_ts = commit_ts;
    with_store_async(
        region, 0,
        [args](const std::string& addr,
               std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
          store_resolve_lock_async(addr, std::move(args), 15000,
                                   [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                                     if (!sst.ok()) {
                                       on_rpc(sst, false, sst.error_message(), "");
                                       return;
                                     }
                                     on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                                   });
        },
        [key](grpc::Status st, bool ok, std::string err, std::string, std::string) {
          if (!st.ok() || !ok) {
            SKV_WARN << "async resolve secondary key=" << key
                     << " err=" << (st.ok() ? err : st.error_message());
          }
        });
  }
}

void GatewayServer::txn_begin_async(const kv::v1::TxnBeginRequest* req, kv::v1::TxnBeginResponse* resp,
                                    std::function<void(grpc::Status)> done) {
  uint32_t n = req->count();
  if (n == 0) n = 1;
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(n, [this, resp, done = std::move(done)](grpc::Status st, uint64_t start) {
    if (!st.ok()) {
      done(st);
      return;
    }
    active_ts_.add(start);
    resp->set_start_ts(start);
    done(grpc::Status::OK);
  });
}

void GatewayServer::txn_get_async(const kv::v1::TxnGetRequest* req, kv::v1::TxnGetResponse* resp,
                                  int attempt, std::function<void(grpc::Status)> done) {
  if (attempt >= 3) {
    resp->set_locked(true);
    done(grpc::Status::OK);
    return;
  }
  const uint32_t region = region_of(req->key());
  auto addr_opt = pick_leader_addr(region);
  if (!addr_opt) {
    done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
    return;
  }
  const std::string addr = *addr_opt;
  note_to_store(addr);
  store_get_async(
      addr, region, req->key(), /*linearizable=*/true, req->start_ts(), 15000,
      [this, req, resp, attempt, region, addr, done](grpc::Status sst, StoreGetResult out) {
        if (!sst.ok()) {
          done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
          return;
        }
        if (out.locked || contains(out.error, "locked")) {
          LockedError le(out.lock_primary, out.lock_ts, out.error.empty() ? "locked" : out.error);
          resolve_blocking_lock_async(req->key(), le, [this, req, resp, attempt, done](grpc::Status rst) {
            if (!rst.ok() && contains(rst.error_message(), "locked")) {
              resp->set_locked(true);
              done(grpc::Status::OK);
              return;
            }
            txn_get_async(req, resp, attempt + 1, done);
          });
          return;
        }
        if (!out.redirect_to.empty() || contains(out.error, "not_leader") ||
            contains(out.error, "splitting")) {
          if (!out.redirect_to.empty() && out.redirect_to != "key_not_in_region" &&
              out.redirect_to != "no_region") {
            remember_leader(region, out.redirect_to);
          } else {
            refresh_routes();
          }
          clear_leader_hint(region);
          txn_get_async(req, resp, attempt + 1, done);
          return;
        }
        if (!out.error.empty() && !out.found) {
          done(grpc::Status(grpc::StatusCode::INTERNAL, out.error));
          return;
        }
        remember_leader(region, addr);
        resp->set_value(std::move(out.value));
        resp->set_found(out.found);
        resp->set_revision(out.revision);
        resp->set_locked(false);
        done(grpc::Status::OK);
      });
}

void GatewayServer::txn_prewrite_async(const kv::v1::TxnPrewriteRequest* req,
                                       kv::v1::TxnPrewriteResponse* resp,
                                       std::function<void(grpc::Status)> done) {
  if (req->mutations_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    done(grpc::Status::OK);
    return;
  }
  std::string primary = req->primary_key();
  if (primary.empty()) primary = req->mutations(0).key();
  struct State {
    GatewayServer* self;
    const kv::v1::TxnPrewriteRequest* req;
    kv::v1::TxnPrewriteResponse* resp;
    std::string primary;
    int idx{0};
    std::function<void(grpc::Status)> done;
    std::shared_ptr<std::function<void()>> step;
  };
  auto st = std::make_shared<State>();
  st->self = this;
  st->req = req;
  st->resp = resp;
  st->primary = std::move(primary);
  st->done = std::move(done);
  st->step = std::make_shared<std::function<void()>>();
  *st->step = [st]() {
    if (st->idx >= st->req->mutations_size()) {
      st->resp->set_ok(true);
      st->done(grpc::Status::OK);
      return;
    }
    const auto& m = st->req->mutations(st->idx);
    const uint32_t region = st->self->region_of(m.key());
    const uint64_t epoch = st->self->epoch_of(region);
    ProposeTxnArgs args =
        make_txn_args(region, ProposeOp::kPrewrite, m.key(), st->req->start_ts(), epoch, st->primary);
    args.mvcc_op = m.is_delete() ? 1u : 0u;
    if (!m.is_delete()) args.value = m.value();
    args.cas = st->req->cas();
    args.expected_revision = st->req->expected_revision();
    args.expire_at_ms = st->req->expire_at_ms();
    st->self->with_store_async(
        region, 0,
        [args = std::move(args)](
            const std::string& addr,
            std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
          store_prewrite_async(addr, std::move(args), 15000,
                               [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                                 if (!sst.ok()) {
                                   on_rpc(sst, false, sst.error_message(), "");
                                   return;
                                 }
                                 if (out.cas_failed) {
                                   on_rpc(grpc::Status::OK, false,
                                          "cas_failed current_revision=" +
                                              std::to_string(out.current_revision),
                                          out.redirect_to);
                                   return;
                                 }
                                 on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                               });
        },
        [st](grpc::Status sst, bool ok, std::string err, std::string, std::string) {
          if (!sst.ok()) {
            st->done(sst);
            return;
          }
          if (!ok) {
            st->resp->set_ok(false);
            st->resp->set_error(err);
            if (contains(err, "cas_failed")) {
              st->resp->set_cas_failed(true);
              st->resp->set_current_revision(parse_cas_current_rev(err));
            }
            st->done(grpc::Status::OK);
            return;
          }
          ++st->idx;
          (*st->step)();
        });
  };
  (*st->step)();
}

void GatewayServer::txn_commit_async(const kv::v1::TxnCommitRequest* req,
                                     kv::v1::TxnCommitResponse* resp,
                                     std::function<void(grpc::Status)> done) {
  if (req->keys_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    done(grpc::Status::OK);
    return;
  }
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(1, [this, req, resp, done = std::move(done)](grpc::Status tst, uint64_t commit_ts) {
    if (!tst.ok()) {
      done(tst);
      return;
    }
    resp->set_commit_ts(commit_ts);
    std::string pri = req->primary_key();
    if (pri.empty()) pri = req->keys(0);
    if (commit_ts <= req->start_ts()) {
      resp->set_ok(false);
      resp->set_error("commit_ts<=start_ts");
      done(grpc::Status::OK);
      return;
    }
    const uint32_t pr = region_of(pri);
    const uint64_t epoch = epoch_of(pr);
    ProposeTxnArgs args =
        make_txn_args(pr, ProposeOp::kCommit, pri, req->start_ts(), epoch, pri);
    args.commit_ts = commit_ts;
    with_store_async(
        pr, 0,
        [args = std::move(args)](
            const std::string& addr,
            std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
          store_commit_async(addr, std::move(args), 15000,
                             [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                               if (!sst.ok()) {
                                 on_rpc(sst, false, sst.error_message(), "");
                                 return;
                               }
                               on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                             });
        },
        [this, req, resp, done, pri, commit_ts](grpc::Status sst, bool ok, std::string err,
                                                std::string, std::string) {
          if (!sst.ok()) {
            done(sst);
            return;
          }
          if (!ok) {
            resp->set_ok(false);
            resp->set_error(err);
            done(grpc::Status::OK);
            return;
          }
          active_ts_.remove(req->start_ts());
          std::vector<std::string> secondaries;
          secondaries.reserve(static_cast<size_t>(req->keys_size()));
          for (const auto& k : req->keys()) {
            if (k != pri) secondaries.push_back(k);
          }
          async_resolve_secondaries(req->start_ts(), commit_ts, secondaries);
          // best-effort async commit secondaries
          for (const auto& key : secondaries) {
            const uint32_t region = region_of(key);
            const uint64_t ep = epoch_of(region);
            ProposeTxnArgs a =
                make_txn_args(region, ProposeOp::kCommit, key, req->start_ts(), ep, pri);
            a.commit_ts = commit_ts;
            with_store_async(
                region, 0,
                [a = std::move(a)](
                    const std::string& addr,
                    std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
                  store_commit_async(addr, std::move(a), 15000,
                                     [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                                       if (!sst.ok()) {
                                         on_rpc(sst, false, sst.error_message(), "");
                                         return;
                                       }
                                       on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                                     });
                },
                [](grpc::Status, bool, std::string, std::string, std::string) {});
          }
          resp->set_ok(true);
          done(grpc::Status::OK);
        });
  });
}

void GatewayServer::txn_rollback_async(const kv::v1::TxnRollbackRequest* req,
                                       kv::v1::TxnRollbackResponse* resp,
                                       std::function<void(grpc::Status)> done) {
  struct State {
    GatewayServer* self;
    const kv::v1::TxnRollbackRequest* req;
    kv::v1::TxnRollbackResponse* resp;
    int idx{0};
    std::function<void(grpc::Status)> done;
    std::shared_ptr<std::function<void()>> step;
  };
  auto st = std::make_shared<State>();
  st->self = this;
  st->req = req;
  st->resp = resp;
  st->done = std::move(done);
  st->step = std::make_shared<std::function<void()>>();
  *st->step = [st]() {
    if (st->idx >= st->req->keys_size()) {
      st->self->active_ts_.remove(st->req->start_ts());
      st->resp->set_ok(true);
      st->done(grpc::Status::OK);
      return;
    }
    const std::string& key = st->req->keys(st->idx);
    const uint32_t region = st->self->region_of(key);
    const uint64_t epoch = st->self->epoch_of(region);
    ProposeTxnArgs args =
        make_txn_args(region, ProposeOp::kRollback, key, st->req->start_ts(), epoch);
    st->self->with_store_async(
        region, 0,
        [args = std::move(args)](
            const std::string& addr,
            std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
          store_rollback_async(addr, std::move(args), 15000,
                               [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                                 if (!sst.ok()) {
                                   on_rpc(sst, false, sst.error_message(), "");
                                   return;
                                 }
                                 on_rpc(grpc::Status::OK, true, out.error, out.redirect_to);
                               });
        },
        [st](grpc::Status, bool, std::string, std::string, std::string) {
          ++st->idx;
          (*st->step)();
        });
  };
  (*st->step)();
}

void GatewayServer::txn_pessimistic_lock_async(const kv::v1::TxnPessimisticLockRequest* req,
                                               kv::v1::TxnPessimisticLockResponse* resp,
                                               std::function<void(grpc::Status)> done) {
  if (req->keys_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    done(grpc::Status::OK);
    return;
  }
  std::string primary = req->primary_key();
  if (primary.empty()) primary = req->keys(0);
  auto continue_with = [this, req, resp, primary, done = std::move(done)](uint64_t for_update) mutable {
    uint64_t ttl = req->ttl_ms();
    if (ttl == 0) ttl = 5000;
    struct State {
      GatewayServer* self;
      const kv::v1::TxnPessimisticLockRequest* req;
      kv::v1::TxnPessimisticLockResponse* resp;
      std::string primary;
      uint64_t for_update{0};
      uint64_t ttl{0};
      int idx{0};
      std::function<void(grpc::Status)> done;
      std::shared_ptr<std::function<void()>> step;
    };
    auto st = std::make_shared<State>();
    st->self = this;
    st->req = req;
    st->resp = resp;
    st->primary = std::move(primary);
    st->for_update = for_update;
    st->ttl = ttl;
    st->done = std::move(done);
    st->step = std::make_shared<std::function<void()>>();
    *st->step = [st]() {
      if (st->idx >= st->req->keys_size()) {
        st->resp->set_ok(true);
        st->done(grpc::Status::OK);
        return;
      }
      const std::string& key = st->req->keys(st->idx);
      const uint32_t region = st->self->region_of(key);
      const uint64_t epoch = st->self->epoch_of(region);
      ProposeTxnArgs args = make_txn_args(region, ProposeOp::kPessimisticLock, key,
                                          st->req->start_ts(), epoch, st->primary);
      args.for_update_ts = st->for_update;
      args.lock_ttl_ms = st->ttl;
      st->self->with_store_async(
          region, 0,
          [args = std::move(args)](
              const std::string& addr,
              std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
            store_pessimistic_lock_async(
                addr, std::move(args), 15000,
                [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                  if (!sst.ok()) {
                    on_rpc(sst, false, sst.error_message(), "");
                    return;
                  }
                  on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                });
          },
          [st](grpc::Status sst, bool ok, std::string err, std::string, std::string) {
            if (!sst.ok()) {
              st->done(sst);
              return;
            }
            if (!ok) {
              st->resp->set_ok(false);
              st->resp->set_error(err);
              st->done(grpc::Status::OK);
              return;
            }
            ++st->idx;
            (*st->step)();
          });
    };
    (*st->step)();
  };
  if (req->for_update_ts() != 0) {
    continue_with(req->for_update_ts());
    return;
  }
  note_to_meta(meta_addrs_.empty() ? std::string{} : meta_addrs_.front());
  ts_.alloc_async(1, [continue_with = std::move(continue_with), done](grpc::Status st, uint64_t ts) mutable {
    if (!st.ok()) {
      done(st);
      return;
    }
    continue_with(ts);
  });
}

void GatewayServer::txn_pessimistic_rollback_async(const kv::v1::TxnPessimisticRollbackRequest* req,
                                                   kv::v1::TxnPessimisticRollbackResponse* resp,
                                                   std::function<void(grpc::Status)> done) {
  struct State {
    GatewayServer* self;
    const kv::v1::TxnPessimisticRollbackRequest* req;
    kv::v1::TxnPessimisticRollbackResponse* resp;
    int idx{0};
    std::function<void(grpc::Status)> done;
    std::shared_ptr<std::function<void()>> step;
  };
  auto st = std::make_shared<State>();
  st->self = this;
  st->req = req;
  st->resp = resp;
  st->done = std::move(done);
  st->step = std::make_shared<std::function<void()>>();
  *st->step = [st]() {
    if (st->idx >= st->req->keys_size()) {
      st->resp->set_ok(true);
      st->done(grpc::Status::OK);
      return;
    }
    const std::string& key = st->req->keys(st->idx);
    const uint32_t region = st->self->region_of(key);
    const uint64_t epoch = st->self->epoch_of(region);
    ProposeTxnArgs args =
        make_txn_args(region, ProposeOp::kPessimisticRollback, key, st->req->start_ts(), epoch);
    st->self->with_store_async(
        region, 0,
        [args = std::move(args)](
            const std::string& addr,
            std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
          store_pessimistic_rollback_async(
              addr, std::move(args), 15000,
              [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                if (!sst.ok()) {
                  on_rpc(sst, false, sst.error_message(), "");
                  return;
                }
                on_rpc(grpc::Status::OK, true, out.error, out.redirect_to);
              });
        },
        [st](grpc::Status, bool, std::string, std::string, std::string) {
          ++st->idx;
          (*st->step)();
        });
  };
  (*st->step)();
}

void GatewayServer::txn_resolve_lock_async(const kv::v1::TxnResolveLockRequest* req,
                                           kv::v1::TxnResolveLockResponse* resp,
                                           std::function<void(grpc::Status)> done) {
  const uint32_t region = region_of(req->key());
  const uint64_t epoch = epoch_of(region);
  ProposeTxnArgs args =
      make_txn_args(region, ProposeOp::kResolveLock, req->key(), req->start_ts(), epoch);
  args.commit_ts = req->commit_ts();
  with_store_async(
      region, 0,
      [args = std::move(args)](
          const std::string& addr,
          std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) mutable {
        store_resolve_lock_async(addr, std::move(args), 15000,
                                 [on_rpc = std::move(on_rpc)](grpc::Status sst, ProposeResult out) {
                                   if (!sst.ok()) {
                                     on_rpc(sst, false, sst.error_message(), "");
                                     return;
                                   }
                                   on_rpc(grpc::Status::OK, out.ok, out.error, out.redirect_to);
                                 });
      },
      [resp, done = std::move(done)](grpc::Status sst, bool ok, std::string err, std::string,
                                     std::string) {
        if (!sst.ok()) {
          done(sst);
          return;
        }
        if (!ok) {
          resp->set_ok(false);
          resp->set_error(err);
          done(grpc::Status::OK);
          return;
        }
        resp->set_ok(true);
        done(grpc::Status::OK);
      });
}

void GatewayServer::txn_check_status_async(const kv::v1::TxnCheckStatusRequest* req,
                                           kv::v1::TxnCheckStatusResponse* resp,
                                           std::function<void(grpc::Status)> done) {
  const uint32_t region = region_of(req->primary_key());
  with_store_async(
      region, 0,
      [region, primary = req->primary_key(), lock_ts = req->lock_ts()](
          const std::string& addr,
          std::function<void(grpc::Status, bool, std::string, std::string)> on_rpc) {
        store_check_txn_status_async(
            addr, region, primary, lock_ts, 15000,
            [on_rpc = std::move(on_rpc)](grpc::Status sst, StoreCheckTxnResult out) {
              if (!sst.ok()) {
                on_rpc(sst, false, sst.error_message(), "");
                return;
              }
              if (!out.redirect_to.empty() || !out.error.empty()) {
                on_rpc(grpc::Status::OK, false, out.error.empty() ? "not_leader" : out.error,
                       out.redirect_to);
                return;
              }
              on_rpc(grpc::Status::OK, true, out.status + "\n" + std::to_string(out.commit_ts),
                     "");
            });
      },
      [resp, done = std::move(done)](grpc::Status sst, bool ok, std::string body, std::string,
                                     std::string) {
        if (!sst.ok()) {
          done(sst);
          return;
        }
        if (!ok) {
          done(grpc::Status(grpc::StatusCode::INTERNAL, body));
          return;
        }
        const auto nl = body.find('\n');
        resp->set_status(nl == std::string::npos ? body : body.substr(0, nl));
        if (nl != std::string::npos) {
          try {
            resp->set_commit_ts(std::stoull(body.substr(nl + 1)));
          } catch (...) {
          }
        }
        done(grpc::Status::OK);
      });
}

}  // namespace openkv::compute
