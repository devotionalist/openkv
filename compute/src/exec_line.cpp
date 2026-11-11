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

// @file exec_line.cpp
// Channel/Stub 缓存 + Meta/Storage ExecLine；Meta 跟随 not_leader redirect。

#include "openkv/compute/exec_line.hpp"

#include "openkv/admin_acl.hpp"
#include "openkv/grpc_auth.hpp"

#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

#include <chrono>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openkv::compute {
namespace {

std::mutex g_ch_mu;
std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> g_channels;
std::unordered_map<std::string, std::shared_ptr<::storage::v1::StoreService::Stub>> g_store_stubs;
std::unordered_map<std::string, std::shared_ptr<::storage::v1::AdminService::Stub>> g_admin_stubs;
std::unordered_map<std::string, std::shared_ptr<::meta::v1::MetaService::Stub>> g_meta_stubs;

void attach_deadline_and_token(grpc::ClientContext* ctx, int timeout_ms) {
  ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(ctx, admin_token_from_env());
}

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

ProposeOnePcResult propose_result_from(const ::storage::v1::ProposeResponse& resp) {
  ProposeOnePcResult out;
  out.ok = resp.ok();
  out.revision = resp.revision();
  out.error = resp.error();
  out.redirect_to = resp.redirect_to();
  out.cas_failed = resp.cas_failed();
  out.current_revision = resp.current_revision();
  return out;
}

StoreGetResult store_get_result_from(const ::storage::v1::StoreGetResponse& resp) {
  StoreGetResult out;
  out.found = resp.found();
  out.value = resp.value();
  out.revision = resp.revision();
  out.is_leader = resp.is_leader();
  out.redirect_to = resp.redirect_to();
  out.locked = resp.locked();
  out.error = resp.error();
  return out;
}

std::shared_ptr<::storage::v1::StoreService::Stub> cached_store_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_store_stubs.find(addr);
  if (it != g_store_stubs.end() && it->second) return it->second;
  auto ch_it = g_channels.find(addr);
  std::shared_ptr<grpc::Channel> ch;
  if (ch_it != g_channels.end() && ch_it->second) {
    ch = ch_it->second;
  } else {
    ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    g_channels[addr] = ch;
  }
  auto stub = std::shared_ptr<::storage::v1::StoreService::Stub>(
      ::storage::v1::StoreService::NewStub(ch));
  g_store_stubs[addr] = stub;
  return stub;
}

std::shared_ptr<::storage::v1::AdminService::Stub> cached_admin_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_admin_stubs.find(addr);
  if (it != g_admin_stubs.end() && it->second) return it->second;
  auto ch_it = g_channels.find(addr);
  std::shared_ptr<grpc::Channel> ch;
  if (ch_it != g_channels.end() && ch_it->second) {
    ch = ch_it->second;
  } else {
    ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    g_channels[addr] = ch;
  }
  auto stub = std::shared_ptr<::storage::v1::AdminService::Stub>(
      ::storage::v1::AdminService::NewStub(ch));
  g_admin_stubs[addr] = stub;
  return stub;
}

std::shared_ptr<::meta::v1::MetaService::Stub> cached_meta_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_meta_stubs.find(addr);
  if (it != g_meta_stubs.end() && it->second) return it->second;
  auto ch_it = g_channels.find(addr);
  std::shared_ptr<grpc::Channel> ch;
  if (ch_it != g_channels.end() && ch_it->second) {
    ch = ch_it->second;
  } else {
    ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    g_channels[addr] = ch;
  }
  auto stub =
      std::shared_ptr<::meta::v1::MetaService::Stub>(::meta::v1::MetaService::NewStub(ch));
  g_meta_stubs[addr] = stub;
  return stub;
}

}  // namespace

std::string parse_redirect(std::string_view resp) {
  constexpr std::string_view k = "redirect=";
  const auto i = resp.find(k);
  if (i == std::string_view::npos) return {};
  auto rest = resp.substr(i + k.size());
  size_t end = 0;
  while (end < rest.size() && rest[end] != ' ' && rest[end] != '\t' && rest[end] != '\r' &&
         rest[end] != '\n') {
    ++end;
  }
  return std::string(rest.substr(0, end));
}

std::shared_ptr<grpc::Channel> cached_channel(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_channels.find(addr);
  if (it != g_channels.end() && it->second) return it->second;
  auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  g_channels[addr] = ch;
  return ch;
}

void drop_cached_channel(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  g_channels.erase(addr);
  g_store_stubs.erase(addr);
  g_admin_stubs.erase(addr);
  g_meta_stubs.erase(addr);
}

std::string meta_exec_line_one(const std::string& addr, const std::string& line, int timeout_ms) {
  auto stub = cached_meta_stub(addr);
  ::meta::v1::ExecLineRequest er;
  er.set_line(line);
  ::meta::v1::ExecLineResponse eres;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->ExecLine(&ctx, er, &eres);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("meta ExecLine: ") + st.error_message());
  }
  return eres.body();
}

std::string meta_exec_line(const std::vector<std::string>& addrs, const std::string& line,
                           int timeout_ms) {
  std::string last_err = "no meta";
  int backoff_ms = 20;
  for (int attempt = 0; attempt < 3; ++attempt) {
    for (const auto& seed : addrs) {
      std::string cur = seed;
      for (int hop = 0; hop < 6; ++hop) {
        try {
          auto resp = meta_exec_line_one(cur, line, timeout_ms);
          if (resp.find("not_leader") != std::string::npos) {
            auto redir = parse_redirect(resp);
            if (!redir.empty()) {
              cur = std::move(redir);
              continue;
            }
            last_err = resp;
            break;
          }
          return resp;
        } catch (const std::exception& e) {
          last_err = e.what();
          break;
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    if (backoff_ms < 500) backoff_ms *= 2;
  }
  throw std::runtime_error(last_err);
}

void store_admin_ping(const std::string& addr, int timeout_ms) {
  auto stub = cached_admin_stub(addr);
  ::storage::v1::PingRequest req;
  ::storage::v1::PingResponse resp;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->Ping(&ctx, req, &resp);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("storage Ping: ") + st.error_message());
  }
  if (resp.message() != "PONG") {
    throw std::runtime_error("storage Ping: unexpected " + resp.message());
  }
}

std::string store_exec_line(const std::string& addr, const std::string& line, int timeout_ms) {
  auto stub = cached_admin_stub(addr);
  ::storage::v1::ExecLineRequest er;
  er.set_line(line);
  ::storage::v1::ExecLineResponse eres;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->ExecLine(&ctx, er, &eres);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("storage ExecLine: ") + st.error_message());
  }
  return eres.body();
}

void store_exec_line_async(const std::string& addr, const std::string& line, int timeout_ms,
                           std::function<void(grpc::Status, std::string)> cb) {
  if (!cb) return;
  auto stub = cached_admin_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ExecLineRequest>();
  req->set_line(line);
  auto resp = std::make_shared<::storage::v1::ExecLineResponse>();
  stub->async()->ExecLine(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, resp->body());
      });
}

void store_propose_onepc_async(const std::string& addr, ProposeOnePcArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposeOnePcResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ProposeRequest>();
  req->set_shard_id(args.shard_id);
  req->set_op(::storage::v1::ProposeRequest::ONEPC);
  req->set_key(std::move(args.key));
  req->set_value(std::move(args.value));
  req->set_start_ts(args.start_ts);
  req->set_commit_ts(args.commit_ts);
  req->set_primary(args.primary.empty() ? req->key() : args.primary);
  req->set_mvcc_op(args.mvcc_op);
  req->set_epoch(args.epoch);
  req->set_cas(args.cas);
  req->set_expected_revision(args.expected_revision);
  req->set_ttl_ms(args.expire_at_ms);
  auto resp = std::make_shared<::storage::v1::ProposeResponse>();
  stub->async()->Propose(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, propose_result_from(*resp));
      });
}

void store_propose_plain_async(const std::string& addr, ProposePlainArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposePlainResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ProposeRequest>();
  req->set_shard_id(args.shard_id);
  req->set_op(args.is_delete ? ::storage::v1::ProposeRequest::DELETE
                             : ::storage::v1::ProposeRequest::PUT);
  req->set_key(std::move(args.key));
  if (!args.is_delete) req->set_value(std::move(args.value));
  req->set_ttl_ms(args.ttl_ms);
  req->set_cas(args.cas);
  req->set_expected_revision(args.expected_revision);
  auto resp = std::make_shared<::storage::v1::ProposeResponse>();
  stub->async()->Propose(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, propose_result_from(*resp));
      });
}

void store_get_async(const std::string& addr, uint32_t shard_id, std::string key, bool linearizable,
                     uint64_t start_ts, int timeout_ms,
                     std::function<void(grpc::Status, StoreGetResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreGetRequest>();
  req->set_shard_id(shard_id);
  req->set_key(std::move(key));
  req->set_linearizable(linearizable);
  req->set_start_ts(start_ts);
  auto resp = std::make_shared<::storage::v1::StoreGetResponse>();
  stub->async()->Get(ctx.get(), req.get(), resp.get(),
                     [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
                       if (!st.ok()) drop_cached_channel(addr);
                       cb(st, store_get_result_from(*resp));
                     });
}

void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb) {
  if (!cb) return;
  auto stub = cached_meta_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::meta::v1::GetTsRequest>();
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
    size_t idx{0};
    uint32_t count{1};
    int timeout_ms{3000};
    std::string line;
    std::function<void(grpc::Status, uint64_t, uint32_t)> cb;
    std::shared_ptr<std::function<void()>> try_next;
  };
  auto st = std::make_shared<State>();
  st->addrs = meta_addrs;
  st->count = count;
  st->timeout_ms = timeout_ms;
  st->line = "GET_TS " + std::to_string(count);
  st->cb = std::move(cb);
  st->try_next = std::make_shared<std::function<void()>>();
  *st->try_next = [st]() {
    if (st->idx >= st->addrs.size()) {
      st->cb(grpc::Status(grpc::StatusCode::UNAVAILABLE, "tso: all meta failed"), 0, 0);
      return;
    }
    const std::string addr = st->addrs[st->idx++];
    auto stub = cached_meta_stub(addr);
    auto ctx = std::make_shared<grpc::ClientContext>();
    attach_deadline_and_token(ctx.get(), st->timeout_ms);
    auto req = std::make_shared<::meta::v1::ExecLineRequest>();
    req->set_line(st->line);
    auto resp = std::make_shared<::meta::v1::ExecLineResponse>();
    auto next = st->try_next;
    stub->async()->ExecLine(
        ctx.get(), req.get(), resp.get(),
        [st, addr, stub, ctx, req, resp, next](grpc::Status gst) {
          if (!gst.ok()) {
            drop_cached_channel(addr);
            (*next)();
            return;
          }
          const std::string body = resp->body();
          if (body.find("not_leader") != std::string::npos) {
            auto redir = parse_redirect(body);
            if (!redir.empty()) {
              st->addrs.insert(st->addrs.begin() + static_cast<std::ptrdiff_t>(st->idx), redir);
            }
            (*next)();
            return;
          }
          auto parts = split_ws(body);
          if (parts.size() >= 3 && parts[0] == "OK" && parts[1] == "TS") {
            try {
              uint64_t start = std::stoull(parts[2]);
              uint32_t got = st->count;
              if (parts.size() >= 4) {
                unsigned long n = std::stoul(parts[3]);
                if (n > 0) got = static_cast<uint32_t>(n);
              }
              st->cb(grpc::Status::OK, start, got);
              return;
            } catch (...) {
              (*next)();
              return;
            }
          }
          (*next)();
        });
  };
  (*st->try_next)();
}

}  // namespace openkv::compute
