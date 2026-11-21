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

// @file raft_wire.hpp
// 职责：FileRaftEngine / 编解码共用的磁盘魔数常量（一处定义，禁止散落字符串字面量）。

#pragma once

#include <cstddef>
#include <string_view>

namespace openkv::raft {

inline constexpr size_t kWireMagicLen = 4;

/// 日志文件容器头（4 字节，口语 RLG）。
inline constexpr std::string_view kMagicRlg{"RLG\0", kWireMagicLen};
/// RaftLocalState 记录。
inline constexpr std::string_view kMagicRls{"RLS1"};
/// 单条 LogEntry 记录（含 CRC）。
inline constexpr std::string_view kMagicRle{"RLE2"};
/// 本地 FSM 快照文件头（Apply 落盘 / 恢复）。
inline constexpr std::string_view kMagicFsmSnap{"FSN1"};

}  // namespace openkv::raft
