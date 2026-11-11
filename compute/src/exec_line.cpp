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
// Channel 缓存 + Meta/Storage ExecLine；Meta 跟随 not_leader redirect。

#include "openkv/compute/exec_line.hpp"

#include "openkv/admin_acl.hpp"
#include "openkv/grpc_auth.hpp"

#include "meta/v1/meta.grpc.pb.h"
#include "meta/v1/meta.pb.h"
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

#include <chrono>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace openkv::compute {
namespace {

std::mutex g_ch_mu;
std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> g_channels;

void attach_deadline_and_token(grpc::ClientContext* ctx, int timeout_ms) {
  ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(ctx, admin_token_from_env());
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
}

std::string meta_exec_line_one(const std::string& addr, const std::string& line, int timeout_ms) {
  auto ch = cached_channel(addr);
  auto stub = ::meta::v1::MetaService::NewStub(ch);
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

std::string store_exec_line(const std::string& addr, const std::string& line, int timeout_ms) {
  auto ch = cached_channel(addr);
  auto stub = ::storage::v1::AdminService::NewStub(ch);
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
  // shared_ptr 保活：gRPC async 完成回调前 stub/ctx/req/resp 必须仍存活。
  auto stub = std::shared_ptr<::storage::v1::AdminService::Stub>(
      ::storage::v1::AdminService::NewStub(cached_channel(addr)));
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

void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb) {
  if (!cb) return;
  auto stub = std::shared_ptr<::meta::v1::MetaService::Stub>(
      ::meta::v1::MetaService::NewStub(cached_channel(addr)));
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

}  // namespace openkv::compute
