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

// @file meta_client.hpp
// Gateway → Meta：TSO / 路由 / min_start_ts（对齐 Go internal/route）。

#pragma once

#include "openkv/compute/router.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openkv::compute {

/// 解析 GET_ROUTE / WATCH_ROUTE 正文为 RoutingTable。
/// 格式：OK ROUTE ver n n rid:startHex:endHex:epoch:nrep,node@addr*L,...
RoutingTable parse_route(std::string_view resp);

/// 申请 [start, start+got) 时间戳区间（GET_TS）。
std::pair<uint64_t, uint32_t> alloc_ts_range(const std::vector<std::string>& meta_addrs,
                                             uint32_t count);

inline uint64_t alloc_ts(const std::vector<std::string>& meta_addrs, uint32_t count) {
  return alloc_ts_range(meta_addrs, count).first;
}

/// 任意副本可读；旧 Meta 仍可能 not_leader，跟随 redirect。
RoutingTable fetch_route(const std::vector<std::string>& meta_addrs, int timeout_ms = 3000);

/// 长轮询直到 route_ver > since 或超时。
RoutingTable watch_route(const std::vector<std::string>& meta_addrs, uint64_t since,
                         int timeout_ms = 5000);

/// 上报活跃事务最小 start_ts；UINT64_MAX 表示无活跃。
void report_min_start_ts(const std::vector<std::string>& meta_addrs, uint64_t min_start,
                         std::string_view reporter = "gateway");

}  // namespace openkv::compute
