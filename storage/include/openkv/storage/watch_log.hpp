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

// @file watch_log.hpp
// O3：每 Region 持久化 Watch 事件（meta key），revision = Raft apply index。

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/storage/kv_engine.hpp"
#include "openkv/storage/watch_hub.hpp"

namespace openkv::storage {

inline constexpr std::string_view kWatchLogPrefix = "\xff\xffskv/watch/";  // watch meta key 前缀

/* 引擎内持久化 Watch 日志：key=prefix+revision，value=事件载荷。 */
class WatchLog {
 public:
  // 默认保留最近 N 条；超出按字典序删最旧。
  static constexpr size_t kDefaultCapacity = 4096;

  /**
   * 拼出 watch meta key：prefix + 定宽 revision。
   * @param revision Raft apply index
   * @return 引擎内物理 key
   */
  static std::string make_key(uint64_t revision);
  /**
   * 从 meta key 解析 revision；非 watch key 返回 false。
   * @param key 引擎 key
   * @param revision 输出 revision
   * @return 是否为合法 watch key
   */
  static bool parse_key(std::string_view key, uint64_t* revision);

  /**
   * 编码一条 watch 事件载荷。
   * @param deleted 是否删除
   * @param key 用户键
   * @param value 值
   * @return 可写入引擎的 payload
   */
  static std::string encode_payload(bool deleted, std::string_view key, std::string_view value);
  /**
   * 解码 watch 事件载荷。
   * @param payload 引擎 value
   * @param deleted 输出是否删除
   * @param key 输出用户键
   * @param value 输出值
   * @return 是否解码成功
   */
  static bool decode_payload(std::string_view payload, bool* deleted, std::string* key,
                             std::string* value);

  /**
   * 追加一条事件并按 capacity 裁剪旧条目。
   * @param eng 目标引擎
   * @param rec 变更记录（用其 revision/deleted/key/value）
   * @param capacity 最多保留条数；默认 kDefaultCapacity
   */
  static void append(KvEngine& eng, const WatchRecord& rec, size_t capacity = kDefaultCapacity);
  /**
   * 批量 apply 写完后裁剪 watch log（避免每条 append 都扫一遍）。
   * @param eng 目标引擎
   * @param capacity 最多保留条数；默认 kDefaultCapacity
   */
  static void trim(KvEngine& eng, size_t capacity = kDefaultCapacity);
  /**
   * 返回 revision > since 的事件（升序）；region_id 填入每条记录。
   * @param eng 目标引擎
   * @param region_id 写入每条 WatchRecord.region_id
   * @param since 只返回大于该 revision 的事件
   * @param limit 最多返回条数
   * @return 升序事件列表
   */
  static std::vector<WatchRecord> poll(const KvEngine& eng, uint32_t region_id, uint64_t since,
                                       size_t limit);
};

}  // namespace openkv::storage
