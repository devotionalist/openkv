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

// @file meta_client.cpp
// Meta 行协议客户端：GET_TS / GET_ROUTE / WATCH_ROUTE / REPORT_MIN_START_TS。

#include "openkv/compute/meta_client.hpp"

#include "openkv/codec.hpp"
#include "openkv/compute/exec_line.hpp"

#include <sstream>
#include <stdexcept>

namespace openkv::compute {
namespace {

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

std::string meta_one_or_redirect(const std::string& addr, const std::string& req, int timeout_ms) {
  auto resp = meta_exec_line_one(addr, req, timeout_ms);
  if (resp.find("not_leader") != std::string::npos) {
    auto redir = parse_redirect(resp);
    if (!redir.empty()) {
      return meta_exec_line_one(redir, req, timeout_ms);
    }
    throw std::runtime_error(resp);
  }
  return resp;
}

}  // namespace

RoutingTable parse_route(std::string_view resp) {
  auto parts = split_ws(resp);
  if (parts.size() < 4 || parts[0] != "OK" || parts[1] != "ROUTE") {
    throw std::runtime_error(std::string("bad route: ") + std::string(resp));
  }
  RoutingTable t;
  t.version = std::stoull(parts[2]);
  const int n = std::stoi(parts[3]);
  for (size_t pi = 5; pi < parts.size(); ++pi) {
    const auto& tok = parts[pi];
    // rid:startHex:endHex:epoch:nrep,...  或旧格式 rid:start:end:nrep,...
    std::vector<std::string> colon;
    {
      size_t start = 0;
      int fields = 0;
      for (size_t i = 0; i < tok.size() && fields < 4; ++i) {
        if (tok[i] == ':') {
          colon.emplace_back(tok.substr(start, i - start));
          start = i + 1;
          ++fields;
        }
      }
      colon.emplace_back(tok.substr(start));
    }
    if (colon.size() < 4) continue;
    RegionRoute rr;
    rr.region_id = static_cast<openkv::RegionId>(std::stoul(colon[0]));
    if (!colon[1].empty()) {
      if (!from_hex(colon[1], &rr.range.start_key)) {
        throw std::runtime_error("bad route start hex");
      }
    }
    if (!colon[2].empty()) {
      if (!from_hex(colon[2], &rr.range.end_key)) {
        throw std::runtime_error("bad route end hex");
      }
    }
    std::string rest;
    if (colon.size() >= 5) {
      rr.epoch = std::stoull(colon[3]);
      rest = colon[4];
    } else {
      rr.epoch = 1;
      rest = colon[3];
    }
    // rest = nrep,node@addr*L,...
    size_t pos = 0;
    bool first = true;
    while (pos <= rest.size()) {
      size_t comma = rest.find(',', pos);
      std::string item =
          comma == std::string::npos ? rest.substr(pos) : rest.substr(pos, comma - pos);
      if (first) {
        first = false;
      } else if (!item.empty()) {
        const bool leader = item.find("*L") != std::string::npos;
        auto scrub = [](std::string s) {
          for (const char* tag : {"*L", "*N"}) {
            for (;;) {
              auto p = s.find(tag);
              if (p == std::string::npos) break;
              s.erase(p, 2);
            }
          }
          return s;
        };
        item = scrub(std::move(item));
        auto at = item.find('@');
        if (at != std::string::npos) {
          ReplicaEndpoint ep;
          ep.node_id = item.substr(0, at);
          ep.address = item.substr(at + 1);
          ep.is_leader = leader;
          rr.replicas.push_back(std::move(ep));
        }
      }
      if (comma == std::string::npos) break;
      pos = comma + 1;
    }
    t.regions.emplace(rr.region_id, rr);
    if (static_cast<int>(t.regions.size()) >= n) break;
  }
  if (t.regions.empty()) {
    RegionRoute rr;
    rr.region_id = 0;
    rr.epoch = 1;
    t.regions.emplace(0, std::move(rr));
  }
  t.shard_count = static_cast<uint32_t>(t.regions.size());
  return t;
}

std::pair<uint64_t, uint32_t> alloc_ts_range(const std::vector<std::string>& meta_addrs,
                                             uint32_t count) {
  if (count == 0) count = 1;
  auto resp = meta_exec_line(meta_addrs, "GET_TS " + std::to_string(count), 3000);
  auto parts = split_ws(resp);
  if (parts.size() >= 3 && parts[0] == "OK" && parts[1] == "TS") {
    uint64_t start = std::stoull(parts[2]);
    uint32_t got = count;
    if (parts.size() >= 4) {
      unsigned long n = std::stoul(parts[3]);
      if (n > 0) got = static_cast<uint32_t>(n);
    }
    return {start, got};
  }
  throw std::runtime_error(std::string("bad ts: ") + resp);
}

RoutingTable fetch_route(const std::vector<std::string>& meta_addrs, int timeout_ms) {
  std::string last = "no meta";
  for (const auto& addr : meta_addrs) {
    try {
      auto resp = meta_one_or_redirect(addr, "GET_ROUTE", timeout_ms);
      return parse_route(resp);
    } catch (const std::exception& e) {
      last = e.what();
    }
  }
  throw std::runtime_error(last);
}

RoutingTable watch_route(const std::vector<std::string>& meta_addrs, uint64_t since,
                         int timeout_ms) {
  if (timeout_ms <= 0) timeout_ms = 5000;
  const std::string req =
      "WATCH_ROUTE " + std::to_string(since) + " " + std::to_string(timeout_ms);
  const int deadline = timeout_ms + 2000;
  std::string last = "no meta";
  for (const auto& addr : meta_addrs) {
    try {
      auto resp = meta_one_or_redirect(addr, req, deadline);
      return parse_route(resp);
    } catch (const std::exception& e) {
      last = e.what();
    }
  }
  throw std::runtime_error(last);
}

void report_min_start_ts(const std::vector<std::string>& meta_addrs, uint64_t min_start,
                         std::string_view reporter) {
  std::ostringstream oss;
  oss << "REPORT_MIN_START_TS " << min_start;
  if (!reporter.empty()) oss << ' ' << reporter;
  auto resp = meta_exec_line(meta_addrs, oss.str(), 3000);
  if (resp.rfind("OK ", 0) != 0) {
    throw std::runtime_error(std::string("report min start: ") + resp);
  }
}

}  // namespace openkv::compute
