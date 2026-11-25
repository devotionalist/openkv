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
// 职责：Raft 日志文件容器编解码（RLG），与单条 RLE 编解码分离。
// 架构：FileRaftEngine 独占使用；append 路径只追加 RLE 记录，绝不重复写容器魔数。
// 协作：依赖 encode_log_entry / decode_log_entry（raft_engine.hpp）；魔数见 raft_wire.hpp。
// 注意：容器 = kMagicRlg + 连续 entry 记录；整文件 rewrite 才调用 encode_container。

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "openkv/raft/core/types.hpp"
#include "openkv/raft/engine/raft_wire.hpp"

namespace openkv::raft::log_file {

// 容器魔数：统一用 openkv::raft::kMagicRlg（raft_wire.hpp）。

/**
 * 从 offset 起一条 entry 记录的字节长度；无法识别返回 0。
 * @param raw 文件内容
 * @param offset 起始偏移
 * @return 记录长度；0=无法识别
 */
size_t entry_record_size(std::string_view raw, size_t offset);

/**
 * 整文件编码：魔数 + 全部 entries（仅 rewrite 路径使用）。
 * @param entries 全量日志
 * @return 容器字节
 */
std::string encode_container(const std::vector<LogEntry>& entries);

/**
 * 解码整文件；空输入视为合法空日志。
 * @param raw 文件内容
 * @param out 输出条目
 * @return 是否解码成功
 */
bool decode_container(std::string_view raw, std::vector<LogEntry>* out);

/**
 * 无 cut、appends 非空且 index 严格接在 existing 之后时，允许纯追加。
 * @param existing 磁盘已有条目
 * @param appends 待追加
 * @param cuts 切割区间；非空则不可纯追加
 * @return 是否可走 append_log 快路径
 */
bool can_pure_append(const std::vector<LogEntry>& existing, const std::vector<LogEntry>& appends,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts);

/**
 * 在内存中应用 cut [from,to) 与按 index 覆盖的 append，结果按 index 排序。
 * @param entries 输入/输出日志（原地改）
 * @param cuts 切割区间列表
 * @param appends 覆盖/追加条目
 */
void apply_mutations(std::vector<LogEntry>* entries,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts,
                     std::vector<LogEntry> appends);

}  // namespace openkv::raft::log_file
