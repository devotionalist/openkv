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

/// blob 类型与期望不符（WRONGTYPE）。
class WrongTypeError : public std::runtime_error {
 public:
  WrongTypeError(char have, char want)
      : std::runtime_error(std::string("wrong_type: have=") + have + " want=" + want),
        have_(have),
        want_(want) {}
  /// 实际类型标记。
  char have() const { return have_; }
  /// 期望类型标记。
  char want() const { return want_; }

 private:
  char have_;
  char want_;
};

/// blob 损坏 / 无法解码。
class CorruptError : public std::runtime_error {
 public:
  CorruptError() : std::runtime_error("corrupt_ds_blob") {}
};

/// 用户 key → 物理 MetaKey（ukey + marker；类型只在 blob 内）。
std::string meta_key(std::string_view ukey);

using Hash = std::map<std::string, std::string>;  // sorted encode
using Set = std::set<std::string>;
using List = std::vector<std::string>;
using ZSet = std::map<std::string, double>;

/// ZSet 排序后的 (member, score) 项。
struct ZEntry {
  std::string member;
  double score{0};
};

/// Hash → 带类型头的二进制 blob。
std::string encode_hash(const Hash& h);
/// blob → Hash；类型不符抛 WrongTypeError，损坏抛 CorruptError。
Hash decode_hash(std::string_view blob);

/// Set → blob。
std::string encode_set(const Set& s);
/// blob → Set。
Set decode_set(std::string_view blob);

/// List → blob。
std::string encode_list(const List& l);
/// blob → List。
List decode_list(std::string_view blob);

/// Redis 风格 [start,stop]（可负下标）→ [lo, hi) 切片；非法返回 nullopt。
std::optional<std::pair<size_t, size_t>> normalize_range(int64_t n, int64_t start, int64_t stop);

/// ZSet → blob。
std::string encode_zset(const ZSet& z);
/// blob → ZSet。
ZSet decode_zset(std::string_view blob);
/// 按 score 升序、同分按 member 得到有序列表（ZRange 用）。
std::vector<ZEntry> z_sorted(const ZSet& z);

}  // namespace openkv::compute::ds
