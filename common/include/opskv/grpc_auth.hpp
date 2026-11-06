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

// @file grpc_auth.hpp
// gRPC metadata 鉴权：x-skv-admin-token ↔ SKV_ADMIN_TOKEN / admin_acl。

#pragma once

#include "opskv/admin_acl.hpp"

#include <grpcpp/grpcpp.h>

#include <string>
#include <string_view>

namespace opskv {

inline constexpr const char* kGrpcAdminTokenMeta = "x-skv-admin-token";

inline bool grpc_admin_allowed(const grpc::ServerContext& ctx, const std::string& configured) {
  if (!admin_acl_enabled(configured)) return true;
  const auto& md = ctx.client_metadata();
  auto it = md.find(kGrpcAdminTokenMeta);
  if (it == md.end()) return false;
  return std::string_view(it->second.data(), it->second.size()) == configured;
}

inline void grpc_attach_admin_token(grpc::ClientContext* ctx, const std::string& token) {
  if (!ctx || token.empty()) return;
  ctx->AddMetadata(kGrpcAdminTokenMeta, token);
}

}  // namespace opskv
