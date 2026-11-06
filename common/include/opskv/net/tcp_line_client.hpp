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

// @file tcp_line_client.hpp
// 地址解析工具（历史文件名；行协议 TCP 客户端已移除）。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace opskv::net {

/// 解析 host:port；失败返回 false。
bool parse_host_port(std::string_view addr, std::string* host, uint16_t* port);

}  // namespace opskv::net
