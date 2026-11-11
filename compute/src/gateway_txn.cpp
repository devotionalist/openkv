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
// Percolator Txn* RPCs + lock resolve / commitWithTs。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/logging.hpp"

#include <string>

namespace openkv::compute {
namespace {

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

}  // namespace

void GatewayServer::resolve_blocking_lock(const std::string& key, const LockedError& le) {
  if (le.primary().empty() || le.lock_ts() == 0) {
    throw std::runtime_error("locked");
  }
  const uint32_t primary_region = region_of(le.primary());
  std::string status;
  uint64_t commit_ts = 0;
  with_leader(primary_region, [&](StoreClient& c) {
    auto r = c.check_txn_status(primary_region, le.primary(), le.lock_ts());
    status = r.first;
    commit_ts = r.second;
  });
  if (status == "committed") {
    with_leader_epoch(region_of(key), [&](StoreClient& c, uint64_t epoch) {
      (void)c.resolve_lock(region_of(key), key, le.lock_ts(), commit_ts, epoch);
    });
    return;
  }
  if (status == "rolledback" || status == "expired") {
    if (status == "expired") {
      try {
        with_leader_epoch(primary_region, [&](StoreClient& c, uint64_t epoch) {
          (void)c.resolve_lock(primary_region, le.primary(), le.lock_ts(), 0, epoch);
        });
      } catch (...) {
      }
    }
    with_leader_epoch(region_of(key), [&](StoreClient& c, uint64_t epoch) {
      (void)c.resolve_lock(region_of(key), key, le.lock_ts(), 0, epoch);
    });
    return;
  }
  throw std::runtime_error("locked");
}

kv::v1::TxnCommitResponse GatewayServer::commit_with_ts(uint64_t start_ts, uint64_t commit_ts,
                                                        const std::string& primary,
                                                        const std::vector<std::string>& keys) {
  kv::v1::TxnCommitResponse out;
  out.set_commit_ts(commit_ts);
  if (keys.empty()) {
    out.set_ok(false);
    out.set_error("empty");
    return out;
  }
  std::string pri = primary;
  if (pri.empty()) pri = keys[0];
  if (commit_ts <= start_ts) {
    out.set_ok(false);
    out.set_error("commit_ts<=start_ts");
    return out;
  }
  const uint32_t pr = region_of(pri);
  try {
    with_leader_epoch(pr, [&](StoreClient& c, uint64_t epoch) {
      (void)c.commit_key(pr, pri, start_ts, commit_ts, epoch);
    });
  } catch (const std::exception& e) {
    out.set_ok(false);
    out.set_error(e.what());
    return out;
  }
  active_ts_.remove(start_ts);
  std::vector<std::string> secondaries;
  secondaries.reserve(keys.size());
  for (const auto& k : keys) {
    if (k == pri) continue;
    secondaries.push_back(k);
  }
  async_resolve_secondaries(start_ts, commit_ts, secondaries);
  for (const auto& key : secondaries) {
    const uint32_t region = region_of(key);
    try {
      with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
        (void)c.commit_key(region, key, start_ts, commit_ts, epoch);
      });
    } catch (...) {
    }
  }
  out.set_ok(true);
  return out;
}

void GatewayServer::async_resolve_secondaries(uint64_t start_ts, uint64_t commit_ts,
                                              std::vector<std::string> keys) {
  if (keys.empty()) return;
  std::thread([this, start_ts, commit_ts, keys = std::move(keys)]() {
    for (const auto& key : keys) {
      const uint32_t region = region_of(key);
      try {
        with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
          (void)c.resolve_lock(region, key, start_ts, commit_ts, epoch);
        });
      } catch (const std::exception& e) {
        SKV_WARN << "async resolve secondary key=" << key << " err=" << e.what();
      }
    }
  }).detach();
}

grpc::Status GatewayServer::TxnBeginBody(const kv::v1::TxnBeginRequest* req,
                                     kv::v1::TxnBeginResponse* resp) {
  uint32_t n = req->count();
  if (n == 0) n = 1;
  try {
    const uint64_t start = alloc_ts(n);
    active_ts_.add(start);
    resp->set_start_ts(start);
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
  }
}

grpc::Status GatewayServer::TxnGetBody(const kv::v1::TxnGetRequest* req,
                                   kv::v1::TxnGetResponse* resp) {
  for (int attempt = 0; attempt < 3; ++attempt) {
    const uint32_t region = region_of(req->key());
    try {
      with_leader(region, [&](StoreClient& c) {
        auto r = c.txn_get_opt(region, req->key(), req->start_ts(), true);
        resp->set_value(r.value);
        resp->set_found(r.found);
        resp->set_revision(r.revision);
        resp->set_locked(false);
      });
      return grpc::Status::OK;
    } catch (const LockedError& le) {
      try {
        resolve_blocking_lock(req->key(), le);
      } catch (const std::exception& re) {
        if (contains(re.what(), "locked")) {
          resp->set_locked(true);
          return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, re.what());
      }
      continue;
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
  }
  resp->set_locked(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnPrewriteBody(const kv::v1::TxnPrewriteRequest* req,
                                        kv::v1::TxnPrewriteResponse* resp) {
  if (req->mutations_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    return grpc::Status::OK;
  }
  std::string primary = req->primary_key();
  if (primary.empty()) primary = req->mutations(0).key();
  for (const auto& m : req->mutations()) {
    const uint32_t region = region_of(m.key());
    uint8_t op = 0;
    std::string_view val = m.value();
    if (m.is_delete()) {
      op = 1;
      val = {};
    }
    try {
      with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
        (void)c.prewrite_cas(region, m.key(), req->start_ts(), primary, op, val, epoch, req->cas(),
                             req->expected_revision(), req->expire_at_ms());
      });
    } catch (const std::exception& e) {
      const std::string msg = e.what();
      resp->set_ok(false);
      resp->set_error(msg);
      if (contains(msg, "cas_failed")) {
        resp->set_cas_failed(true);
        resp->set_current_revision(parse_cas_current_rev(msg));
      }
      return grpc::Status::OK;
    }
  }
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnCommitBody(const kv::v1::TxnCommitRequest* req,
                                      kv::v1::TxnCommitResponse* resp) {
  if (req->keys_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    return grpc::Status::OK;
  }
  uint64_t commit_ts = 0;
  try {
    commit_ts = alloc_ts(1);
  } catch (const std::exception& e) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
  }
  std::vector<std::string> keys;
  keys.reserve(static_cast<size_t>(req->keys_size()));
  for (const auto& k : req->keys()) keys.push_back(k);
  *resp = commit_with_ts(req->start_ts(), commit_ts, req->primary_key(), keys);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnRollbackBody(const kv::v1::TxnRollbackRequest* req,
                                        kv::v1::TxnRollbackResponse* resp) {
  for (const auto& key : req->keys()) {
    const uint32_t region = region_of(key);
    try {
      with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
        (void)c.rollback_key(region, key, req->start_ts(), epoch);
      });
    } catch (...) {
    }
  }
  active_ts_.remove(req->start_ts());
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnPessimisticLockBody(const kv::v1::TxnPessimisticLockRequest* req,
                                               kv::v1::TxnPessimisticLockResponse* resp) {
  if (req->keys_size() == 0) {
    resp->set_ok(false);
    resp->set_error("empty");
    return grpc::Status::OK;
  }
  std::string primary = req->primary_key();
  if (primary.empty()) primary = req->keys(0);
  uint64_t for_update = req->for_update_ts();
  if (for_update == 0) {
    try {
      for_update = alloc_ts(1);
    } catch (const std::exception& e) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
    }
  }
  uint64_t ttl = req->ttl_ms();
  if (ttl == 0) ttl = 5000;
  for (const auto& key : req->keys()) {
    const uint32_t region = region_of(key);
    try {
      with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
        (void)c.pessimistic_lock(region, key, req->start_ts(), for_update, primary, ttl, epoch);
      });
    } catch (const std::exception& e) {
      resp->set_ok(false);
      resp->set_error(e.what());
      return grpc::Status::OK;
    }
  }
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnPessimisticRollbackBody(const kv::v1::TxnPessimisticRollbackRequest* req,
    kv::v1::TxnPessimisticRollbackResponse* resp) {
  for (const auto& key : req->keys()) {
    const uint32_t region = region_of(key);
    try {
      with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
        (void)c.pessimistic_rollback(region, key, req->start_ts(), epoch);
      });
    } catch (...) {
    }
  }
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnResolveLockBody(const kv::v1::TxnResolveLockRequest* req,
                                           kv::v1::TxnResolveLockResponse* resp) {
  const uint32_t region = region_of(req->key());
  try {
    with_leader_epoch(region, [&](StoreClient& c, uint64_t epoch) {
      (void)c.resolve_lock(region, req->key(), req->start_ts(), req->commit_ts(), epoch);
    });
  } catch (const std::exception& e) {
    resp->set_ok(false);
    resp->set_error(e.what());
    return grpc::Status::OK;
  }
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::TxnCheckStatusBody(const kv::v1::TxnCheckStatusRequest* req,
                                           kv::v1::TxnCheckStatusResponse* resp) {
  const uint32_t region = region_of(req->primary_key());
  try {
    with_leader(region, [&](StoreClient& c) {
      auto r = c.check_txn_status(region, req->primary_key(), req->lock_ts());
      resp->set_status(r.first);
      resp->set_commit_ts(r.second);
    });
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

}  // namespace openkv::compute
