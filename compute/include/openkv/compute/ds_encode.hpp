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

// @file ds_encode.hpp
// DS 物理 key 与 v1 blob 编解码（对齐 Go dsencode）。

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openkv::compute::ds {

inline constexpr char kTypeHash = 'h';
inline constexpr char kTypeSet = 's';
inline constexpr char kTypeList = 'l';
inline constexpr char kTypeZSet = 'z';

class WrongTypeError : public std::runtime_error {
 public:
  WrongTypeError(char have, char want)
      : std::runtime_error(std::string("wrong_type: have=") + have + " want=" + want),
        have_(have),
        want_(want) {}
  char have() const { return have_; }
  char want() const { return want_; }

 private:
  char have_;
  char want_;
};

class CorruptError : public std::runtime_error {
 public:
  CorruptError() : std::runtime_error("corrupt_ds_blob") {}
};

/// MetaKey = ukey + marker（类型只在 blob 内）。
std::string meta_key(std::string_view ukey);

using Hash = std::map<std::string, std::string>;  // sorted encode
using Set = std::set<std::string>;
using List = std::vector<std::string>;
using ZSet = std::map<std::string, double>;

struct ZEntry {
  std::string member;
  double score{0};
};

std::string encode_hash(const Hash& h);
Hash decode_hash(std::string_view blob);

std::string encode_set(const Set& s);
Set decode_set(std::string_view blob);

std::string encode_list(const List& l);
List decode_list(std::string_view blob);

/// Redis 风格 [start,stop] → [lo, hi) 切片下标；失败 ok=false。
std::optional<std::pair<size_t, size_t>> normalize_range(int64_t n, int64_t start, int64_t stop);

std::string encode_zset(const ZSet& z);
ZSet decode_zset(std::string_view blob);
std::vector<ZEntry> z_sorted(const ZSet& z);

}  // namespace openkv::compute::ds
