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

#include "opskv/storage/kv_engine.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::storage {

struct CoprocessorRequest {
  std::string start_key;
  std::string end_key;        // exclusive；空=不限
  std::string key_prefix;     // 空=不过滤
  std::string value_prefix;   // 空=不过滤
  uint32_t limit{256};
  bool keys_only{false};      // 不返回 value
  bool count_only{false};     // 只计匹配数（pairs 空）
};

struct CoprocessorPair {
  std::string key;
  std::string value;
  uint64_t revision{0};
};

struct CoprocessorResponse {
  std::vector<CoprocessorPair> pairs;
  uint64_t matched{0};  // 扫描命中数（含 count_only；截断前计数）
  bool truncated{false};
};

class Coprocessor {
 public:
  explicit Coprocessor(KvEngine* eng) : eng_(eng) {}

  CoprocessorResponse execute(const CoprocessorRequest& req,
                              const opskv::KeyRange& region_range) const;

 private:
  KvEngine* eng_{nullptr};
};

}  // namespace opskv::storage
