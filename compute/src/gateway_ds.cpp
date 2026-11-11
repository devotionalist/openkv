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
// DSService：Hash/Set/List/ZSet 经 Get/Put 读写 ds_encode blob。

#include "openkv/compute/gateway_server.hpp"

#include "openkv/compute/ds_encode.hpp"

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

grpc::Status GatewayServer::ds_load(const std::string& mk, std::string* blob, bool* found) {
  kv::v1::GetRequest gr;
  gr.set_key(mk);
  gr.set_linearizable(true);
  kv::v1::GetResponse out;
  auto st = GetBody( &gr, &out);
  if (!st.ok()) return st;
  *found = out.found();
  if (*found) *blob = out.value();
  else blob->clear();
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ds_store(const std::string& mk, const std::string& blob) {
  if (blob.empty()) {
    kv::v1::DeleteRequest dr;
    dr.set_key(mk);
    kv::v1::DeleteResponse out;
    return DeleteBody( &dr, &out);
  }
  kv::v1::PutRequest pr;
  pr.set_key(mk);
  pr.set_value(blob);
  kv::v1::PutResponse out;
  return PutBody( &pr, &out);
}

// ---- Hash ----

grpc::Status GatewayServer::HSetBody(const kv::v1::HSetRequest* req,
                                 kv::v1::HSetResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  if (req->field().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "field required");
  }
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  const bool existed = h.count(req->field()) > 0;
  h[req->field()] = req->value();
  if (auto st = ds_store(mk, ds::encode_hash(h)); !st.ok()) return st;
  resp->set_ok(true);
  resp->set_added(!existed);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HGetBody(const kv::v1::HGetRequest* req,
                                 kv::v1::HGetResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  auto it = h.find(req->field());
  if (it == h.end()) return grpc::Status::OK;
  resp->set_value(it->second);
  resp->set_found(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HDelBody(const kv::v1::HDelRequest* req,
                                 kv::v1::HDelResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t n = 0;
  for (const auto& f : req->fields()) {
    if (h.erase(f)) ++n;
  }
  std::string store;
  if (!h.empty()) store = ds::encode_hash(h);
  if (auto st = ds_store(mk, store); !st.ok()) return st;
  resp->set_deleted(n);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HExistsBody(const kv::v1::HExistsRequest* req,
                                    kv::v1::HExistsResponse* resp) {
  kv::v1::HGetRequest gr;
  gr.set_key(req->key());
  gr.set_field(req->field());
  kv::v1::HGetResponse out;
  auto st = HGetBody( &gr, &out);
  if (!st.ok()) return st;
  resp->set_exists(out.found());
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HLenBody(const kv::v1::HLenRequest* req,
                                 kv::v1::HLenResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  resp->set_length(static_cast<int64_t>(h.size()));
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HGetAllBody(const kv::v1::HGetAllRequest* req,
                                    kv::v1::HGetAllResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  for (const auto& [f, v] : h) {
    resp->add_fields(f);
    resp->add_values(v);
  }
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HKeysBody(const kv::v1::HKeysRequest* req,
                                  kv::v1::HKeysResponse* resp) {
  kv::v1::HGetAllRequest ar;
  ar.set_key(req->key());
  kv::v1::HGetAllResponse all;
  auto st = HGetAllBody(&ar, &all);
  if (!st.ok()) return st;
  *resp->mutable_fields() = all.fields();
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HMSetBody(const kv::v1::HMSetRequest* req,
                                  kv::v1::HMSetResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  if (req->fields_size() != req->values_size()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "fields/values length mismatch");
  }
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::Hash h;
  try {
    h = ds::decode_hash(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  for (int i = 0; i < req->fields_size(); ++i) {
    h[req->fields(i)] = req->values(i);
  }
  if (auto st = ds_store(mk, ds::encode_hash(h)); !st.ok()) return st;
  resp->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::HMGetBody(const kv::v1::HMGetRequest* req,
                                  kv::v1::HMGetResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::Hash h;
  if (found) {
    try {
      h = ds::decode_hash(blob);
    } catch (const std::exception& e) {
      return ds_err(e);
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
  return grpc::Status::OK;
}

// ---- Set ----

grpc::Status GatewayServer::SAddBody(const kv::v1::SAddRequest* req,
                                 kv::v1::SAddResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::Set set;
  try {
    set = ds::decode_set(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t added = 0;
  for (const auto& m : req->members()) {
    if (set.insert(m).second) ++added;
  }
  if (auto st = ds_store(mk, ds::encode_set(set)); !st.ok()) return st;
  resp->set_added(added);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::SRemBody(const kv::v1::SRemRequest* req,
                                 kv::v1::SRemResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Set set;
  try {
    set = ds::decode_set(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t n = 0;
  for (const auto& m : req->members()) {
    if (set.erase(m)) ++n;
  }
  std::string store;
  if (!set.empty()) store = ds::encode_set(set);
  if (auto st = ds_store(mk, store); !st.ok()) return st;
  resp->set_removed(n);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::SIsMemberBody(const kv::v1::SIsMemberRequest* req,
                                      kv::v1::SIsMemberResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Set set;
  try {
    set = ds::decode_set(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  resp->set_is_member(set.count(req->member()) > 0);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::SCardBody(const kv::v1::SCardRequest* req,
                                  kv::v1::SCardResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Set set;
  try {
    set = ds::decode_set(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  resp->set_card(static_cast<int64_t>(set.size()));
  return grpc::Status::OK;
}

grpc::Status GatewayServer::SMembersBody(const kv::v1::SMembersRequest* req,
                                     kv::v1::SMembersResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::Set set;
  try {
    set = ds::decode_set(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  for (const auto& m : set) resp->add_members(m);
  return grpc::Status::OK;
}

// ---- List ----

grpc::Status GatewayServer::list_push(const std::string& key, const std::vector<std::string>& values,
                                      bool left, int64_t* length) {
  if (auto st = require_key(key); !st.ok()) return st;
  const std::string mk = ds::meta_key(key);
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::List l;
  try {
    l = ds::decode_list(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  if (left) {
    for (const auto& v : values) {
      l.insert(l.begin(), v);
    }
  } else {
    for (const auto& v : values) l.push_back(v);
  }
  if (auto st = ds_store(mk, ds::encode_list(l)); !st.ok()) return st;
  *length = static_cast<int64_t>(l.size());
  return grpc::Status::OK;
}

grpc::Status GatewayServer::list_pop(const std::string& key, bool left, std::string* value,
                                     bool* found_out) {
  if (auto st = require_key(key); !st.ok()) return st;
  const std::string mk = ds::meta_key(key);
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) {
    *found_out = false;
    return grpc::Status::OK;
  }
  ds::List l;
  try {
    l = ds::decode_list(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  if (l.empty()) {
    *found_out = false;
    return grpc::Status::OK;
  }
  if (left) {
    *value = l.front();
    l.erase(l.begin());
  } else {
    *value = l.back();
    l.pop_back();
  }
  std::string store;
  if (!l.empty()) store = ds::encode_list(l);
  if (auto st = ds_store(mk, store); !st.ok()) return st;
  *found_out = true;
  return grpc::Status::OK;
}

grpc::Status GatewayServer::LPushBody(const kv::v1::LPushRequest* req,
                                  kv::v1::LPushResponse* resp) {
  std::vector<std::string> values(req->values().begin(), req->values().end());
  int64_t length = 0;
  auto st = list_push(req->key(), values, true, &length);
  if (!st.ok()) return st;
  resp->set_length(length);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::RPushBody(const kv::v1::RPushRequest* req,
                                  kv::v1::RPushResponse* resp) {
  std::vector<std::string> values(req->values().begin(), req->values().end());
  int64_t length = 0;
  auto st = list_push(req->key(), values, false, &length);
  if (!st.ok()) return st;
  resp->set_length(length);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::LPopBody(const kv::v1::LPopRequest* req,
                                 kv::v1::LPopResponse* resp) {
  std::string value;
  bool found = false;
  auto st = list_pop(req->key(), true, &value, &found);
  if (!st.ok()) return st;
  if (found) {
    resp->set_value(value);
    resp->set_found(true);
  }
  return grpc::Status::OK;
}

grpc::Status GatewayServer::RPopBody(const kv::v1::RPopRequest* req,
                                 kv::v1::RPopResponse* resp) {
  std::string value;
  bool found = false;
  auto st = list_pop(req->key(), false, &value, &found);
  if (!st.ok()) return st;
  if (found) {
    resp->set_value(value);
    resp->set_found(true);
  }
  return grpc::Status::OK;
}

grpc::Status GatewayServer::LLenBody(const kv::v1::LLenRequest* req,
                                 kv::v1::LLenResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::List l;
  try {
    l = ds::decode_list(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  resp->set_length(static_cast<int64_t>(l.size()));
  return grpc::Status::OK;
}

grpc::Status GatewayServer::LRangeBody(const kv::v1::LRangeRequest* req,
                                   kv::v1::LRangeResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::List l;
  try {
    l = ds::decode_list(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  auto rng = ds::normalize_range(static_cast<int64_t>(l.size()), req->start(), req->stop());
  if (!rng) return grpc::Status::OK;
  for (size_t i = rng->first; i < rng->second; ++i) resp->add_values(l[i]);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::LIndexBody(const kv::v1::LIndexRequest* req,
                                   kv::v1::LIndexResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::List l;
  try {
    l = ds::decode_list(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t idx = req->index();
  const int64_t n = static_cast<int64_t>(l.size());
  if (idx < 0) idx = n + idx;
  if (idx < 0 || idx >= n) return grpc::Status::OK;
  resp->set_value(l[static_cast<size_t>(idx)]);
  resp->set_found(true);
  return grpc::Status::OK;
}

// ---- ZSet ----

grpc::Status GatewayServer::ZAddBody(const kv::v1::ZAddRequest* req,
                                 kv::v1::ZAddResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  ds::ZSet z;
  try {
    z = ds::decode_zset(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t added = 0;
  for (const auto& it : req->items()) {
    if (!z.count(it.member())) ++added;
    z[it.member()] = it.score();
  }
  if (auto st = ds_store(mk, ds::encode_zset(z)); !st.ok()) return st;
  resp->set_added(added);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ZRemBody(const kv::v1::ZRemRequest* req,
                                 kv::v1::ZRemResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::ZSet z;
  try {
    z = ds::decode_zset(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  int64_t n = 0;
  for (const auto& m : req->members()) {
    if (z.erase(m)) ++n;
  }
  std::string store;
  if (!z.empty()) store = ds::encode_zset(z);
  if (auto st = ds_store(mk, store); !st.ok()) return st;
  resp->set_removed(n);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ZScoreBody(const kv::v1::ZScoreRequest* req,
                                   kv::v1::ZScoreResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::ZSet z;
  try {
    z = ds::decode_zset(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  auto it = z.find(req->member());
  if (it == z.end()) return grpc::Status::OK;
  resp->set_score(it->second);
  resp->set_found(true);
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ZCardBody(const kv::v1::ZCardRequest* req,
                                  kv::v1::ZCardResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::ZSet z;
  try {
    z = ds::decode_zset(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  resp->set_card(static_cast<int64_t>(z.size()));
  return grpc::Status::OK;
}

grpc::Status GatewayServer::ZRangeBody(const kv::v1::ZRangeRequest* req,
                                   kv::v1::ZRangeResponse* resp) {
  if (auto st = require_key(req->key()); !st.ok()) return st;
  const std::string mk = ds::meta_key(req->key());
  std::string blob;
  bool found = false;
  if (auto st = ds_load(mk, &blob, &found); !st.ok()) return st;
  if (!found) return grpc::Status::OK;
  ds::ZSet z;
  try {
    z = ds::decode_zset(blob);
  } catch (const std::exception& e) {
    return ds_err(e);
  }
  auto ents = ds::z_sorted(z);
  auto rng = ds::normalize_range(static_cast<int64_t>(ents.size()), req->start(), req->stop());
  if (!rng) return grpc::Status::OK;
  for (size_t i = rng->first; i < rng->second; ++i) {
    resp->add_members(ents[i].member);
    if (req->with_scores()) resp->add_scores(ents[i].score);
  }
  return grpc::Status::OK;
}

}  // namespace openkv::compute
