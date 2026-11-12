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

// @file grpc_channel.cpp
// Channel / Stub 进程级缓存。

#include "openkv/compute/grpc_channel.hpp"

#include "openkv/admin_acl.hpp"
#include "openkv/grpc_auth.hpp"

#include <chrono>
#include <mutex>
#include <unordered_map>

namespace openkv::compute {
namespace {

std::mutex g_ch_mu;
std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> g_channels;
std::unordered_map<std::string, std::shared_ptr<::storage::v1::StoreService::Stub>> g_store_stubs;
std::unordered_map<std::string, std::shared_ptr<::storage::v1::AdminService::Stub>> g_admin_stubs;
std::unordered_map<std::string, std::shared_ptr<::meta::v1::MetaService::Stub>> g_meta_stubs;

std::shared_ptr<grpc::Channel> channel_locked(const std::string& addr) {
  auto it = g_channels.find(addr);
  if (it != g_channels.end() && it->second) return it->second;
  auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  g_channels[addr] = ch;
  return ch;
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
  return channel_locked(addr);
}

void drop_cached_channel(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  g_channels.erase(addr);
  g_store_stubs.erase(addr);
  g_admin_stubs.erase(addr);
  g_meta_stubs.erase(addr);
}

void attach_deadline_and_token(grpc::ClientContext* ctx, int timeout_ms) {
  ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  grpc_attach_admin_token(ctx, admin_token_from_env());
}

std::shared_ptr<::storage::v1::StoreService::Stub> cached_store_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_store_stubs.find(addr);
  if (it != g_store_stubs.end() && it->second) return it->second;
  auto stub = std::shared_ptr<::storage::v1::StoreService::Stub>(
      ::storage::v1::StoreService::NewStub(channel_locked(addr)));
  g_store_stubs[addr] = stub;
  return stub;
}

std::shared_ptr<::storage::v1::AdminService::Stub> cached_admin_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_admin_stubs.find(addr);
  if (it != g_admin_stubs.end() && it->second) return it->second;
  auto stub = std::shared_ptr<::storage::v1::AdminService::Stub>(
      ::storage::v1::AdminService::NewStub(channel_locked(addr)));
  g_admin_stubs[addr] = stub;
  return stub;
}

std::shared_ptr<::meta::v1::MetaService::Stub> cached_meta_stub(const std::string& addr) {
  std::lock_guard<std::mutex> lock(g_ch_mu);
  auto it = g_meta_stubs.find(addr);
  if (it != g_meta_stubs.end() && it->second) return it->second;
  auto stub =
      std::shared_ptr<::meta::v1::MetaService::Stub>(::meta::v1::MetaService::NewStub(channel_locked(addr)));
  g_meta_stubs[addr] = stub;
  return stub;
}

}  // namespace openkv::compute
