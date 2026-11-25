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

#include <grpcpp/grpcpp.h>

#include <string>
#include <string_view>

#include "openkv/admin_acl.hpp"

namespace openkv {

inline constexpr const char* kGrpcAdminTokenMeta = "x-skv-admin-token";  // metadata 键名

/**
 * 服务端：metadata 里的 token 是否与配置一致；ACL 关闭时恒 true。
 * @param ctx 入站 ServerContext（读 client_metadata）
 * @param configured 进程配置的 Admin token；空=关闭鉴权
 * @return 是否允许继续处理
 */
inline bool grpc_admin_allowed(const grpc::ServerContextBase& ctx, const std::string& configured) {
  if (!admin_acl_enabled(configured)) {
    return true;
  }
  const auto& md = ctx.client_metadata();
  auto it = md.find(kGrpcAdminTokenMeta);
  if (it == md.end()) {
    return false;
  }
  return std::string_view(it->second.data(), it->second.size()) == configured;
}

/**
 * 客户端：把 Admin token 挂到出站 metadata；token 空则跳过。
 * @param ctx 出站 ClientContext；可空
 * @param token Admin token
 */
inline void grpc_attach_admin_token(grpc::ClientContext* ctx, const std::string& token) {
  if (!ctx || token.empty()) {
    return;
  }
  ctx->AddMetadata(kGrpcAdminTokenMeta, token);
}

}  // namespace openkv
