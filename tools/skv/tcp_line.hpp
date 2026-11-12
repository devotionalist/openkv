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

// @file tcp_line.hpp
// 运维 CLI：类型化 MetaService / AdminService（兼容历史行命令字符串）。
#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "openkv/admin_acl.hpp"
#include "openkv/admin_rpc.hpp"
#include "openkv/codec.hpp"
#include "openkv/meta_rpc.hpp"
#include "openkv/protocol.hpp"

#include <chrono>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace skv {

inline bool parse_host_port(std::string_view addr, std::string* host, uint16_t* port) {
  auto pos = addr.rfind(':');
  if (pos == std::string_view::npos) return false;
  *host = std::string(addr.substr(0, pos));
  *port = static_cast<uint16_t>(std::stoul(std::string(addr.substr(pos + 1))));
  return true;
}

inline bool tcp_reachable(std::string_view addr, int timeout_ms = 300) {
  std::string host;
  uint16_t port = 0;
  if (!parse_host_port(addr, &host, &port)) return false;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  timeval tv{.tv_sec = timeout_ms / 1000, .tv_usec = (timeout_ms % 1000) * 1000};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
    if (host == "localhost") {
      ::inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    } else {
      ::close(fd);
      return false;
    }
  }
  bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0;
  ::close(fd);
  return ok;
}

inline bool looks_meta_req(std::string_view req) {
  return req.rfind("REGISTER", 0) == 0 || req.rfind("HEARTBEAT", 0) == 0 ||
         req.rfind("GET_ROUTE", 0) == 0 || req.rfind("GET_TS", 0) == 0 ||
         req.rfind("GET_SAFE", 0) == 0 || req.rfind("LIST_", 0) == 0 ||
         req.rfind("SCALE_", 0) == 0 || req.rfind("SPLIT_REGION", 0) == 0 ||
         req.rfind("MERGE_REGIONS", 0) == 0 || req.rfind("PLACE_REGION", 0) == 0 ||
         req.rfind("DRAIN_", 0) == 0 || req.rfind("ACTIVATE_", 0) == 0 ||
         req.rfind("OFFLINE_", 0) == 0 || req.rfind("JOIN_", 0) == 0 ||
         req.rfind("KICK_", 0) == 0 || req.rfind("WHO_LEADER", 0) == 0 ||
         req.rfind("LOCATE", 0) == 0 || req.rfind("BALANCE", 0) == 0 ||
         req.rfind("SUGGEST_", 0) == 0 || req.rfind("SCHEDULE_", 0) == 0 ||
         req.rfind("CANCEL_", 0) == 0 || req.rfind("BUMP_", 0) == 0 ||
         req.rfind("ADVANCE_", 0) == 0 || req.rfind("REPORT_", 0) == 0 ||
         req.rfind("STORE_", 0) == 0 || req.rfind("FIND_MERGE", 0) == 0 ||
         req.rfind("REMOVE_", 0) == 0 || req.rfind("METRICS", 0) == 0 ||
         req.rfind("STATS", 0) == 0;
}

inline std::string tcp_line(std::string_view addr, std::string_view req, int timeout_ms = 3000) {
  std::string host;
  uint16_t port = 0;
  parse_host_port(addr, &host, &port);
  const bool meta = looks_meta_req(req) || (port >= 8500 && port < 8600);
  const auto token = openkv::admin_token_from_env();
  if (meta) {
    return openkv::meta_typed_call(addr, req, token, timeout_ms);
  }
  return openkv::admin_typed_call(addr, req, token, timeout_ms);
}

inline std::optional<std::string> redirect_of(std::string_view resp) {
  const std::string_view key = "redirect=";
  auto i = resp.find(key);
  if (i == std::string_view::npos) return std::nullopt;
  auto rest = resp.substr(i + key.size());
  size_t end = 0;
  while (end < rest.size() && rest[end] != ' ' && rest[end] != '\t' && rest[end] != '\r') ++end;
  if (end == 0) return std::nullopt;
  return std::string(rest.substr(0, end));
}

inline std::string to_hex(std::string_view s) { return ::openkv::to_hex(s); }
inline bool from_hex(std::string_view hex, std::string* out) {
  return ::openkv::from_hex(hex, out);
}

inline std::vector<std::string> split_ws(std::string_view s) {
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

}  // namespace skv
