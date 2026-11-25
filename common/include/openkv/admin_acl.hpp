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

#include <sys/socket.h>

#include <cstdlib>
#include <string>
#include <string_view>

#include "openkv/protocol.hpp"

namespace openkv {

/* 一条 TCP/行协议连接上的鉴权状态。 */
struct AdminSession {
  bool authenticated{false};  // 是否已 AUTH 成功（ACL 关闭时也可视为已认证）
};

/**
 * 从 SKV_ADMIN_TOKEN 读配置；未设返回空（关闭 ACL）。
 * @return 配置的 token 或空串
 */
inline std::string admin_token_from_env() {
  if (const char* e = std::getenv(proto::env::kAdminToken)) {
    return std::string(e);
  }
  return {};
}

/**
 * token 非空则开启 ACL。
 * @param token 进程配置的 Admin token
 * @return 是否启用鉴权
 */
inline bool admin_acl_enabled(const std::string& token) { return !token.empty(); }

/**
 * 尝试用 provided 完成 AUTH；ACL 关闭时直接标已认证。
 * @param sess 连接会话；不可空
 * @param configured_token 进程配置 token
 * @param provided 客户端提交的 token
 * @return 是否认证成功
 */
inline bool admin_try_auth(AdminSession* sess, const std::string& configured_token,
                           std::string_view provided) {
  if (!sess) {
    return false;
  }
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

/**
 * 当前会话是否允许执行需 Admin 的操作。
 * @param sess 连接会话
 * @param configured_token 进程配置 token
 * @return ACL 关闭或已认证则为 true
 */
inline bool admin_allowed(const AdminSession& sess, const std::string& configured_token) {
  if (!admin_acl_enabled(configured_token)) {
    return true;
  }
  return sess.authenticated;
}

/**
 * Storage：ADMIN_* 运维命令需鉴权。
 * @param op 操作名（行协议第一词）
 * @return 是否需要 Admin
 */
inline bool storage_needs_admin(std::string_view op) {
  return op.rfind(proto::kAdminPrefix, 0) == 0;
}

/**
 * Meta：变更拓扑 / 调度 / 水位的写命令需鉴权（读路径与 Raft/心跳除外）。
 * @param op 操作名（行协议第一词）
 * @return 是否需要 Admin
 */
inline bool meta_needs_admin(std::string_view op) {
  for (const auto o : proto::kMetaAdminOps) {
    if (op == o) {
      return true;
    }
  }
  return false;
}

/**
 * 已连接 fd：若配置了 token 则先 AUTH；失败返回错误信息，成功返回空串。
 * @param fd 已连接套接字
 * @param token Admin token；空则跳过
 * @return 空串=成功，否则错误描述
 */
inline std::string admin_auth_handshake(int fd, const std::string& token) {
  if (!admin_acl_enabled(token)) {
    return {};
  }
  std::string line = "AUTH " + token + "\n";
  if (::send(fd, line.data(), line.size(), 0) < 0) {
    return "AUTH send failed";
  }
  std::string buf;
  char tmp[512];
  while (buf.find('\n') == std::string::npos) {
    ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
    if (n <= 0) {
      return "AUTH recv failed";
    }
    buf.append(tmp, static_cast<size_t>(n));
  }
  auto pos = buf.find('\n');
  std::string resp = buf.substr(0, pos);
  if (!resp.empty() && resp.back() == '\r') {
    resp.pop_back();
  }
  if (!proto::starts_with_ok(resp)) {
    return resp.empty() ? "AUTH rejected" : resp;
  }
  return {};
}

}  // namespace openkv
