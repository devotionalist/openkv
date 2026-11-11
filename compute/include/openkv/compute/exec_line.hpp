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

// @file exec_line.hpp
// Gateway 出站：缓存 Channel 的 Meta/Storage Admin ExecLine。

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace openkv::compute {

/// 从 "… redirect=<addr> …" 解析重定向地址；无则空串。
std::string parse_redirect(std::string_view resp);

/// 按 addr 复用 insecure Channel；传输错误后可用 drop_cached_channel 丢掉。
std::shared_ptr<grpc::Channel> cached_channel(const std::string& addr);
void drop_cached_channel(const std::string& addr);

/// 单次 MetaService.ExecLine（带 admin token / deadline）。
std::string meta_exec_line_one(const std::string& addr, const std::string& line, int timeout_ms);

/// 跟随 not_leader redirect：遍历 addrs，指数退避最多 3 轮、每轮最多 6 次跳转。
std::string meta_exec_line(const std::vector<std::string>& addrs, const std::string& line,
                           int timeout_ms = 3000);

/// 单次 AdminService.ExecLine（Storage）。
std::string store_exec_line(const std::string& addr, const std::string& line, int timeout_ms = 15000);

/// 异步 AdminService.ExecLine（gRPC callback stub）；完成回调可能在 gRPC 内部线程。
void store_exec_line_async(const std::string& addr, const std::string& line, int timeout_ms,
                           std::function<void(grpc::Status, std::string)> cb);

/// 异步 MetaService.GetTs。
void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb);

}  // namespace openkv::compute
