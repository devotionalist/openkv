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

#include "opskv/storage/kv_engine.hpp"
#include "opskv/storage/watch_hub.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::storage {

inline constexpr std::string_view kWatchLogPrefix = "\xff\xffskv/watch/";

class WatchLog {
 public:
  /// 默认保留最近 N 条；超出按字典序删最旧。
  static constexpr size_t kDefaultCapacity = 4096;

  static std::string make_key(uint64_t revision);
  /// 从 meta key 解析 revision；非 watch key 返回 false。
  static bool parse_key(std::string_view key, uint64_t* revision);

  static std::string encode_payload(bool deleted, std::string_view key, std::string_view value);
  static bool decode_payload(std::string_view payload, bool* deleted, std::string* key,
                             std::string* value);

  static void append(KvEngine& eng, const WatchRecord& rec, size_t capacity = kDefaultCapacity);
  /// 返回 revision > since 的事件（升序）；region_id 填入每条记录。
  static std::vector<WatchRecord> poll(const KvEngine& eng, uint32_t region_id, uint64_t since,
                                       size_t limit);
};

}  // namespace opskv::storage
