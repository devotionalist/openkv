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

// @file admin_acl.hpp
// O1：Admin ACL — SKV_ADMIN_TOKEN；连接级 AUTH；空 token = 关闭鉴权（开发默认）。
// TLS：进程内明文 TCP；生产用 stunnel/envoy/nginx 终止 TLS（见 STORE/META_PRODUCTION）。

#pragma once

#include "opskv/protocol.hpp"

#include <sys/socket.h>

#include <cstdlib>
#include <string>
#include <string_view>

namespace opskv {

struct AdminSession {
  bool authenticated{false};
};

inline std::string admin_token_from_env() {
  if (const char* e = std::getenv(proto::env::kAdminToken)) {
    return std::string(e);
  }
  return {};
}

/// ACL 关闭时任意连接视为已认证。
inline bool admin_acl_enabled(const std::string& token) { return !token.empty(); }

inline bool admin_try_auth(AdminSession* sess, const std::string& configured_token,
                           std::string_view provided) {
  if (!sess) return false;
  if (!admin_acl_enabled(configured_token)) {
    sess->authenticated = true;
    return true;
  }
  if (provided == configured_token) {
    sess->authenticated = true;
    return true;
  }
  return false;
}

inline bool admin_allowed(const AdminSession& sess, const std::string& configured_token) {
  if (!admin_acl_enabled(configured_token)) return true;
  return sess.authenticated;
}

/// Storage：ADMIN_* 运维命令需鉴权。
inline bool storage_needs_admin(std::string_view op) {
  return op.rfind(proto::kAdminPrefix, 0) == 0;
}

/// Meta：变更拓扑 / 调度 / 水位的写命令需鉴权（读路径与 Raft/心跳除外）。
inline bool meta_needs_admin(std::string_view op) {
  for (const auto o : proto::kMetaAdminOps) {
    if (op == o) return true;
  }
  return false;
}

/// 已连接 fd：若配置了 token 则先 AUTH；失败返回错误信息，成功返回空串。
inline std::string admin_auth_handshake(int fd, const std::string& token) {
  if (!admin_acl_enabled(token)) return {};
  std::string line = "AUTH " + token + "\n";
  if (::send(fd, line.data(), line.size(), 0) < 0) return "AUTH send failed";
  std::string buf;
  char tmp[512];
  while (buf.find('\n') == std::string::npos) {
    ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
    if (n <= 0) return "AUTH recv failed";
    buf.append(tmp, static_cast<size_t>(n));
  }
  auto pos = buf.find('\n');
  std::string resp = buf.substr(0, pos);
  if (!resp.empty() && resp.back() == '\r') resp.pop_back();
  if (!proto::starts_with_ok(resp)) return resp.empty() ? "AUTH rejected" : resp;
  return {};
}

}  // namespace opskv
