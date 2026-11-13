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

// @file raft_log_file.hpp
// 职责：Raft 日志文件容器编解码（RLG2 / 兼容 RLG1），与单条 RLE 编解码分离。
// 架构：FileRaftEngine 独占使用；append 路径只追加 RLE 记录，绝不重复写容器魔数。
// 协作：依赖 encode_log_entry / decode_log_entry（raft_engine.hpp）。
// 注意：RLG2 = "RLG2" + 连续 entry 记录；整文件 rewrite 才调用 encode_container。

#pragma once

#include "openkv/raft/types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openkv::raft::log_file {

inline constexpr std::string_view kMagicRlg2 = "RLG2";
inline constexpr std::string_view kMagicRlg1 = "RLG1";

/// 从 offset 起一条 entry 记录的字节长度；无法识别返回 0。
size_t entry_record_size(std::string_view raw, size_t offset);

/// 整文件编码：魔数 + 全部 entries（仅 rewrite 路径使用）。
std::string encode_container(const std::vector<LogEntry>& entries);

/// 解码整文件；空输入视为合法空日志。
bool decode_container(std::string_view raw, std::vector<LogEntry>* out);

/// 无 cut、appends 非空且 index 严格接在 existing 之后时，允许纯追加。
bool can_pure_append(const std::vector<LogEntry>& existing, const std::vector<LogEntry>& appends,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts);

/// 在内存中应用 cut [from,to) 与按 index 覆盖的 append，结果按 index 排序。
void apply_mutations(std::vector<LogEntry>* entries,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts,
                     std::vector<LogEntry> appends);

}  // namespace openkv::raft::log_file
