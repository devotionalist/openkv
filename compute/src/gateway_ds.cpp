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

// @file gateway_ds.cpp
// DSService：Hash/Set/List/ZSet 经 MetaKey + ds_encode；出站 get/put_async。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/compute/ds_encode.hpp"

#include <memory>
#include <utility>

namespace openkv::compute {
namespace {

grpc::Status ds_err(const std::exception& e) {
  if (dynamic_cast<const ds::WrongTypeError*>(&e)) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, e.what());
  }
  if (dynamic_cast<const ds::CorruptError*>(&e)) {
    return grpc::Status(grpc::StatusCode::DATA_LOSS, e.what());
  }
  return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
}

grpc::Status require_key(const std::string& key) {
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "key required");
  }
  return grpc::Status::OK;
}

}  // namespace

void GatewayServer::ds_load_async(const std::string& mk,
                                  std::function<void(grpc::Status, std::string, bool)> done) {
  auto gr = std::make_shared<kv::v1::GetRequest>();
  gr->set_key(mk);
  gr->set_linearizable(true);
  auto out = std::make_shared<kv::v1::GetResponse>();
  get_async_cb(gr.get(), out.get(), 0,
               [gr, out, done = std::move(done)](grpc::Status st) {
                 if (!st.ok()) {
                   done(st, {}, false);
                   return;
                 }
                 done(grpc::Status::OK, out->found() ? out->value() : std::string{}, out->found());
               });
}

void GatewayServer::ds_store_async(const std::string& mk, const std::string& blob,
                                   std::function<void(grpc::Status)> done, int attempt) {
  if (blob.empty()) {
    if (attempt >= 4) {
      done(grpc::Status(grpc::StatusCode::ABORTED, "ds_store delete: retries exceeded"));
      return;
    }
    auto dr = std::make_shared<kv::v1::DeleteRequest>();
    dr->set_key(mk);
    auto out = std::make_shared<kv::v1::DeleteResponse>();
    with_start_commit_ts(
        done, [this, dr, out, done, attempt](uint64_t start, uint64_t commit_ts) {
          active_ts_.add(start);
          const uint32_t region = region_of(dr->key());
          const uint64_t epoch = epoch_of(region);
          auto addr_opt = pick_leader_addr(region);
          if (!addr_opt) {
            active_ts_.remove(start);
            done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "no storage"));
            return;
          }
          ProposeOnePcArgs args;
          args.shard_id = region;
          args.key = dr->key();
          args.start_ts = start;
          args.commit_ts = commit_ts;
          args.primary = dr->key();
          args.mvcc_op = 1;
          args.epoch = epoch;
          note_to_store(*addr_opt);
          store_propose_onepc_async(
              *addr_opt, std::move(args), 15000,
              [this, dr, out, start, region, addr = *addr_opt, done, attempt](
                  grpc::Status sst, ProposeOnePcResult res) {
                active_ts_.remove(start);
                if (!sst.ok()) {
                  done(grpc::Status(grpc::StatusCode::UNAVAILABLE, sst.error_message()));
                  return;
                }
                if (res.ok) {
                  remember_leader(region, addr);
                  out->set_deleted(true);
                  out->set_revision(res.revision);
                  done(grpc::Status::OK);
                  return;
                }
                std::string body = res.error;
                if (!res.redirect_to.empty() && body.find("redirect=") == std::string::npos) {
                  body += " redirect=" + res.redirect_to;
                }
                if (retry_async_write(body, dr->key(), region, false,
                                      [this, dr, out, done, attempt] {
                                        ds_store_async(dr->key(), "", done, attempt + 1);
                                      })) {
                  return;
                }
                done(grpc::Status(grpc::StatusCode::ABORTED, "ds_store delete: " + body));
              });
        });
    return;
  }
  auto pr = std::make_shared<kv::v1::PutRequest>();
  pr->set_key(mk);
  pr->set_value(blob);
  auto out = std::make_shared<kv::v1::PutResponse>();
  put_async_cb(pr.get(), out.get(), 0, [pr, out, done = std::move(done)](grpc::Status st) {
    done(st);
  });
}

void GatewayServer::list_push_async(const std::string& key, std::vector<std::string> values,
                                    bool left, std::function<void(grpc::Status, int64_t)> done) {
  if (auto st = require_key(key); !st.ok()) {
    done(st, 0);
    return;
  }
  const std::string mk = ds::meta_key(key);
  ds_load_async(mk, [this, mk, values = std::move(values), left, done = std::move(done)](
                        grpc::Status st, std::string blob, bool) mutable {
    if (!st.ok()) {
      done(st, 0);
      return;
    }
    ds::List l;
    try {
      l = ds::decode_list(blob);
    } catch (const std::exception& e) {
      done(ds_err(e), 0);
      return;
    }
    if (left) {
      for (const auto& v : values) l.insert(l.begin(), v);
    } else {
      for (const auto& v : values) l.push_back(v);
    }
    const int64_t length = static_cast<int64_t>(l.size());
    ds_store_async(mk, ds::encode_list(l), [done = std::move(done), length](grpc::Status sst) {
      done(sst, length);
    });
  });
}

void GatewayServer::list_pop_async(
    const std::string& key, bool left,
    std::function<void(grpc::Status, std::string, bool)> done) {
  if (auto st = require_key(key); !st.ok()) {
    done(st, {}, false);
    return;
  }
  const std::string mk = ds::meta_key(key);
  ds_load_async(mk, [this, mk, left, done = std::move(done)](grpc::Status st, std::string blob,
                                                             bool found) mutable {
    if (!st.ok()) {
      done(st, {}, false);
      return;
    }
    if (!found) {
      done(grpc::Status::OK, {}, false);
      return;
    }
    ds::List l;
    try {
      l = ds::decode_list(blob);
    } catch (const std::exception& e) {
      done(ds_err(e), {}, false);
      return;
    }
    if (l.empty()) {
      done(grpc::Status::OK, {}, false);
      return;
    }
    std::string value;
    if (left) {
      value = l.front();
      l.erase(l.begin());
    } else {
      value = l.back();
      l.pop_back();
    }
    std::string store;
    if (!l.empty()) store = ds::encode_list(l);
    ds_store_async(mk, store, [done = std::move(done), value = std::move(value)](grpc::Status sst) {
      done(sst, value, true);
    });
  });
}

void GatewayServer::HSetAsync(const kv::v1::HSetRequest* req, kv::v1::HSetResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  if (req->field().empty()) {
    done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "field required"));
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool) {
    if (!st.ok()) {
      done(st);
      return;
    }
    ds::Hash h;
    try {
      h = ds::decode_hash(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    const bool existed = h.count(req->field()) > 0;
    h[req->field()] = req->value();
    ds_store_async(mk, ds::encode_hash(h), [resp, existed, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_ok(true);
      resp->set_added(!existed);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::HGetAsync(const kv::v1::HGetRequest* req, kv::v1::HGetResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
    if (!st.ok()) {
      done(st);
      return;
    }
    if (!found) {
      done(grpc::Status::OK);
      return;
    }
    ds::Hash h;
    try {
      h = ds::decode_hash(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    auto it = h.find(req->field());
    if (it != h.end()) {
      resp->set_value(it->second);
      resp->set_found(true);
    }
    done(grpc::Status::OK);
  });
}

void GatewayServer::HDelAsync(const kv::v1::HDelRequest* req, kv::v1::HDelResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool found) {
    if (!st.ok()) {
      done(st);
      return;
    }
    if (!found) {
      done(grpc::Status::OK);
      return;
    }
    ds::Hash h;
    try {
      h = ds::decode_hash(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    int64_t n = 0;
    for (const auto& f : req->fields()) {
      if (h.erase(f)) ++n;
    }
    std::string store;
    if (!h.empty()) store = ds::encode_hash(h);
    ds_store_async(mk, store, [resp, n, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_deleted(n);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::HExistsAsync(const kv::v1::HExistsRequest* req, kv::v1::HExistsResponse* resp,
                                 std::function<void(grpc::Status)> done) {
  auto gr = std::make_shared<kv::v1::HGetRequest>();
  gr->set_key(req->key());
  gr->set_field(req->field());
  auto out = std::make_shared<kv::v1::HGetResponse>();
  HGetAsync(gr.get(), out.get(), [resp, gr, out, done = std::move(done)](grpc::Status st) {
    if (!st.ok()) {
      done(st);
      return;
    }
    resp->set_exists(out->found());
    done(grpc::Status::OK);
  });
}

void GatewayServer::HLenAsync(const kv::v1::HLenRequest* req, kv::v1::HLenResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    resp->set_length(static_cast<int64_t>(ds::decode_hash(blob).size()));
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::HGetAllAsync(const kv::v1::HGetAllRequest* req, kv::v1::HGetAllResponse* resp,
                                 std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    for (const auto& [f, v] : ds::decode_hash(blob)) {
                      resp->add_fields(f);
                      resp->add_values(v);
                    }
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::HKeysAsync(const kv::v1::HKeysRequest* req, kv::v1::HKeysResponse* resp,
                               std::function<void(grpc::Status)> done) {
  auto ar = std::make_shared<kv::v1::HGetAllRequest>();
  ar->set_key(req->key());
  auto all = std::make_shared<kv::v1::HGetAllResponse>();
  HGetAllAsync(ar.get(), all.get(), [resp, ar, all, done = std::move(done)](grpc::Status st) {
    if (!st.ok()) {
      done(st);
      return;
    }
    *resp->mutable_fields() = all->fields();
    done(grpc::Status::OK);
  });
}

void GatewayServer::HMSetAsync(const kv::v1::HMSetRequest* req, kv::v1::HMSetResponse* resp,
                               std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  if (req->fields_size() != req->values_size()) {
    done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "fields/values length mismatch"));
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool) {
    if (!st.ok()) {
      done(st);
      return;
    }
    ds::Hash h;
    try {
      h = ds::decode_hash(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    for (int i = 0; i < req->fields_size(); ++i) h[req->fields(i)] = req->values(i);
    ds_store_async(mk, ds::encode_hash(h), [resp, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_ok(true);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::HMGetAsync(const kv::v1::HMGetRequest* req, kv::v1::HMGetResponse* resp,
                               std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  ds::Hash h;
                  if (found) {
                    try {
                      h = ds::decode_hash(blob);
                    } catch (const std::exception& e) {
                      done(ds_err(e));
                      return;
                    }
                  }
                  for (const auto& f : req->fields()) {
                    auto it = h.find(f);
                    if (it != h.end()) {
                      resp->add_founds(true);
                      resp->add_values(it->second);
                    } else {
                      resp->add_founds(false);
                      resp->add_values("");
                    }
                  }
                  done(grpc::Status::OK);
                });
}

void GatewayServer::SAddAsync(const kv::v1::SAddRequest* req, kv::v1::SAddResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool) {
    if (!st.ok()) {
      done(st);
      return;
    }
    ds::Set set;
    try {
      set = ds::decode_set(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    int64_t added = 0;
    for (const auto& m : req->members()) {
      if (set.insert(m).second) ++added;
    }
    ds_store_async(mk, ds::encode_set(set), [resp, added, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_added(added);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::SRemAsync(const kv::v1::SRemRequest* req, kv::v1::SRemResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool found) {
    if (!st.ok()) {
      done(st);
      return;
    }
    if (!found) {
      done(grpc::Status::OK);
      return;
    }
    ds::Set set;
    try {
      set = ds::decode_set(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    int64_t n = 0;
    for (const auto& m : req->members()) {
      if (set.erase(m)) ++n;
    }
    std::string store;
    if (!set.empty()) store = ds::encode_set(set);
    ds_store_async(mk, store, [resp, n, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_removed(n);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::SIsMemberAsync(const kv::v1::SIsMemberRequest* req,
                                   kv::v1::SIsMemberResponse* resp,
                                   std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    resp->set_is_member(ds::decode_set(blob).count(req->member()) > 0);
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::SCardAsync(const kv::v1::SCardRequest* req, kv::v1::SCardResponse* resp,
                               std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    resp->set_card(static_cast<int64_t>(ds::decode_set(blob).size()));
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::SMembersAsync(const kv::v1::SMembersRequest* req, kv::v1::SMembersResponse* resp,
                                  std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    for (const auto& m : ds::decode_set(blob)) resp->add_members(m);
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::LPushAsync(const kv::v1::LPushRequest* req, kv::v1::LPushResponse* resp,
                               std::function<void(grpc::Status)> done) {
  std::vector<std::string> values(req->values().begin(), req->values().end());
  list_push_async(req->key(), std::move(values), true,
                  [resp, done = std::move(done)](grpc::Status st, int64_t length) {
                    if (!st.ok()) {
                      done(st);
                      return;
                    }
                    resp->set_length(length);
                    done(grpc::Status::OK);
                  });
}

void GatewayServer::RPushAsync(const kv::v1::RPushRequest* req, kv::v1::RPushResponse* resp,
                               std::function<void(grpc::Status)> done) {
  std::vector<std::string> values(req->values().begin(), req->values().end());
  list_push_async(req->key(), std::move(values), false,
                  [resp, done = std::move(done)](grpc::Status st, int64_t length) {
                    if (!st.ok()) {
                      done(st);
                      return;
                    }
                    resp->set_length(length);
                    done(grpc::Status::OK);
                  });
}

void GatewayServer::LPopAsync(const kv::v1::LPopRequest* req, kv::v1::LPopResponse* resp,
                              std::function<void(grpc::Status)> done) {
  list_pop_async(req->key(), true,
                 [resp, done = std::move(done)](grpc::Status st, std::string value, bool found) {
                   if (!st.ok()) {
                     done(st);
                     return;
                   }
                   if (found) {
                     resp->set_value(std::move(value));
                     resp->set_found(true);
                   }
                   done(grpc::Status::OK);
                 });
}

void GatewayServer::RPopAsync(const kv::v1::RPopRequest* req, kv::v1::RPopResponse* resp,
                              std::function<void(grpc::Status)> done) {
  list_pop_async(req->key(), false,
                 [resp, done = std::move(done)](grpc::Status st, std::string value, bool found) {
                   if (!st.ok()) {
                     done(st);
                     return;
                   }
                   if (found) {
                     resp->set_value(std::move(value));
                     resp->set_found(true);
                   }
                   done(grpc::Status::OK);
                 });
}

void GatewayServer::LLenAsync(const kv::v1::LLenRequest* req, kv::v1::LLenResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    resp->set_length(static_cast<int64_t>(ds::decode_list(blob).size()));
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::LRangeAsync(const kv::v1::LRangeRequest* req, kv::v1::LRangeResponse* resp,
                                std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    auto l = ds::decode_list(blob);
                    auto rng =
                        ds::normalize_range(static_cast<int64_t>(l.size()), req->start(), req->stop());
                    if (rng) {
                      for (size_t i = rng->first; i < rng->second; ++i) resp->add_values(l[i]);
                    }
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::LIndexAsync(const kv::v1::LIndexRequest* req, kv::v1::LIndexResponse* resp,
                                std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    auto l = ds::decode_list(blob);
                    int64_t idx = req->index();
                    const int64_t n = static_cast<int64_t>(l.size());
                    if (idx < 0) idx = n + idx;
                    if (idx >= 0 && idx < n) {
                      resp->set_value(l[static_cast<size_t>(idx)]);
                      resp->set_found(true);
                    }
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::ZAddAsync(const kv::v1::ZAddRequest* req, kv::v1::ZAddResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool) {
    if (!st.ok()) {
      done(st);
      return;
    }
    ds::ZSet z;
    try {
      z = ds::decode_zset(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    int64_t added = 0;
    for (const auto& it : req->items()) {
      if (!z.count(it.member())) ++added;
      z[it.member()] = it.score();
    }
    ds_store_async(mk, ds::encode_zset(z), [resp, added, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_added(added);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::ZRemAsync(const kv::v1::ZRemRequest* req, kv::v1::ZRemResponse* resp,
                              std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  const std::string mk = ds::meta_key(req->key());
  ds_load_async(mk, [this, req, resp, mk, done = std::move(done)](grpc::Status st, std::string blob,
                                                                  bool found) {
    if (!st.ok()) {
      done(st);
      return;
    }
    if (!found) {
      done(grpc::Status::OK);
      return;
    }
    ds::ZSet z;
    try {
      z = ds::decode_zset(blob);
    } catch (const std::exception& e) {
      done(ds_err(e));
      return;
    }
    int64_t n = 0;
    for (const auto& m : req->members()) {
      if (z.erase(m)) ++n;
    }
    std::string store;
    if (!z.empty()) store = ds::encode_zset(z);
    ds_store_async(mk, store, [resp, n, done = std::move(done)](grpc::Status sst) {
      if (!sst.ok()) {
        done(sst);
        return;
      }
      resp->set_removed(n);
      done(grpc::Status::OK);
    });
  });
}

void GatewayServer::ZScoreAsync(const kv::v1::ZScoreRequest* req, kv::v1::ZScoreResponse* resp,
                                std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    auto z = ds::decode_zset(blob);
                    auto it = z.find(req->member());
                    if (it != z.end()) {
                      resp->set_score(it->second);
                      resp->set_found(true);
                    }
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::ZCardAsync(const kv::v1::ZCardRequest* req, kv::v1::ZCardResponse* resp,
                               std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    resp->set_card(static_cast<int64_t>(ds::decode_zset(blob).size()));
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

void GatewayServer::ZRangeAsync(const kv::v1::ZRangeRequest* req, kv::v1::ZRangeResponse* resp,
                                std::function<void(grpc::Status)> done) {
  if (auto st = require_key(req->key()); !st.ok()) {
    done(st);
    return;
  }
  ds_load_async(ds::meta_key(req->key()),
                [req, resp, done = std::move(done)](grpc::Status st, std::string blob, bool found) {
                  if (!st.ok()) {
                    done(st);
                    return;
                  }
                  if (!found) {
                    done(grpc::Status::OK);
                    return;
                  }
                  try {
                    auto ents = ds::z_sorted(ds::decode_zset(blob));
                    auto rng = ds::normalize_range(static_cast<int64_t>(ents.size()), req->start(),
                                                   req->stop());
                    if (rng) {
                      for (size_t i = rng->first; i < rng->second; ++i) {
                        resp->add_members(ents[i].member);
                        if (req->with_scores()) resp->add_scores(ents[i].score);
                      }
                    }
                    done(grpc::Status::OK);
                  } catch (const std::exception& e) {
                    done(ds_err(e));
                  }
                });
}

}  // namespace openkv::compute
