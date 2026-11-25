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
// Gateway → Meta：TSO / 路由 / min_start_ts（MetaService 类型化 RPC）。

#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "openkv/compute/router.hpp"

namespace openkv::compute {

/**
 * 向 Meta 申请 [start, start+got) 时间戳区间（GetTs.count 一次分配）。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param count 分配/请求个数
 */
std::pair<uint64_t, uint32_t> alloc_ts_range(const std::vector<std::string>& meta_addrs,
                                             uint32_t count);

/**
 * alloc_ts_range 的简写：只返回起始 ts。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param count 分配/请求个数
 */
inline uint64_t alloc_ts(const std::vector<std::string>& meta_addrs, uint32_t count) {
  return alloc_ts_range(meta_addrs, count).first;
}

/**
 * 拉当前路由表（MetaService.GetRoutingTable）。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param timeout_ms 超时毫秒
 */
RoutingTable fetch_route(const std::vector<std::string>& meta_addrs, int timeout_ms = 3000);

/**
 * 长轮询直到 route_ver > since 或超时；返回新表（WatchRoutingTable）。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param since 已知路由版本；大于此值才返回新表
 * @param timeout_ms 超时毫秒
 */
RoutingTable watch_route(const std::vector<std::string>& meta_addrs, uint64_t since,
                         int timeout_ms = 5000);

/**
 * 上报本 Gateway 活跃事务最小 start_ts；UINT64_MAX 表示无活跃（不挡 GC）。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param min_start 本进程活跃最小 start_ts；UINT64_MAX=无挡
 * @param reporter 上报方标识（如 gateway / node_id）
 */
void report_min_start_ts(const std::vector<std::string>& meta_addrs, uint64_t min_start,
                         std::string_view reporter = "gateway");

/**
 * 异步 MetaService.GetTs（单点取一个 ts）。
 * @param addr 对端地址 host:port
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb);

/**
 * 异步申请 [start, start+got)：单次 GetTs(count)；失败换址。
 * @param meta_addrs Meta 地址列表（可 failover）
 * @param count 分配/请求个数
 * @param timeout_ms 超时毫秒
 * @param cb 完成回调
 */
void meta_alloc_ts_range_async(const std::vector<std::string>& meta_addrs, uint32_t count,
                               int timeout_ms,
                               std::function<void(grpc::Status, uint64_t start, uint32_t got)> cb);

}  // namespace openkv::compute
