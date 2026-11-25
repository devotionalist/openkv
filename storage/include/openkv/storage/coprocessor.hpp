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

// @file coprocessor.hpp
// 服务端 range scan：key/value 前缀过滤、keys_only、count_only。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/storage/kv_engine.hpp"

namespace openkv::storage {

/* 一次 Coprocessor 扫描的过滤与分页参数。 */
struct CoprocessorRequest {
  std::string start_key;     // 扫描起点（含）
  std::string end_key;       // 扫描终点（不含）；空=不限
  std::string key_prefix;    // key 前缀过滤；空=不过滤
  std::string value_prefix;  // value 前缀过滤；空=不过滤
  uint32_t limit{256};       // 最多返回条数
  bool keys_only{false};     // 不返回 value
  bool count_only{false};    // 只计匹配数（pairs 空）
};

/* 扫描命中的一条用户 KV。 */
struct CoprocessorPair {
  std::string key;       // 用户键
  std::string value;     // 值；keys_only 时为空
  uint64_t revision{0};  // 引擎 revision（通常为 apply index）
};

/* 扫描结果：pairs 或仅 matched 计数。 */
struct CoprocessorResponse {
  std::vector<CoprocessorPair> pairs;  // 命中条目；count_only 时为空
  uint64_t matched{0};                 // 扫描命中数（含 count_only；截断前计数）
  bool truncated{false};               // 是否因 limit 截断
};

/* 对本机引擎做带过滤的 range scan（不经 Raft）。 */
class Coprocessor {
 public:
  /**
   * 绑定一个引擎；eng 可为 nullptr（execute 返回空）。
   * @param eng 分片引擎指针；生命周期由调用方保证
   */
  explicit Coprocessor(KvEngine* eng) : eng_(eng) {}

  /**
   * 在 region_range 内按 req 过滤扫描。
   * @param req 前缀 / limit / keys_only / count_only
   * @param region_range 本 Region 半开区间；用于同机共享引擎隔离
   * @return 命中 pairs 或 matched 计数
   */
  CoprocessorResponse execute(const CoprocessorRequest& req,
                              const openkv::KeyRange& region_range) const;

 private:
  KvEngine* eng_{nullptr};  // 扫描目标引擎；可空
};

}  // namespace openkv::storage
