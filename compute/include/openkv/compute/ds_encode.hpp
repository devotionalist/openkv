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

inline constexpr char kTypeHash = 'h';  // Hash blob 类型标记
inline constexpr char kTypeSet = 's';   // Set
inline constexpr char kTypeList = 'l';  // List
inline constexpr char kTypeZSet = 'z';  // ZSet

/* blob 类型与期望不符（WRONGTYPE）。 */
class WrongTypeError : public std::runtime_error {
 public:
  WrongTypeError(char have, char want)
      : std::runtime_error(std::string("wrong_type: have=") + have + " want=" + want),
        have_(have),
        want_(want) {}
  /**
   * 实际类型标记。
   */
  char have() const { return have_; }
  /**
   * 期望类型标记。
   */
  char want() const { return want_; }

 private:
  char have_;  // 实际类型标记
  char want_;  // 期望类型标记
};

/* blob 损坏 / 无法解码。 */
class CorruptError : public std::runtime_error {
 public:
  CorruptError() : std::runtime_error("corrupt_ds_blob") {}
};

/**
 * 用户 key → 物理 MetaKey（ukey + marker；类型只在 blob 内）。
 * @param ukey 用户逻辑 key
 */
std::string meta_key(std::string_view ukey);

using Hash = std::map<std::string, std::string>;  // sorted encode
using Set = std::set<std::string>;
using List = std::vector<std::string>;
using ZSet = std::map<std::string, double>;

/* ZSet 排序后的 (member, score) 项。 */
struct ZEntry {
  std::string member;  // 成员
  double score{0};     // 分值
};

/**
 * Hash → 带类型头的二进制 blob。
 * @param h Hash 映射
 */
std::string encode_hash(const Hash& h);
/**
 * blob → Hash；类型不符抛 WrongTypeError，损坏抛 CorruptError。
 * @param blob 带类型头的二进制 blob
 */
Hash decode_hash(std::string_view blob);

/**
 * Set → blob。
 * @param s Set 集合
 */
std::string encode_set(const Set& s);
/**
 * blob → Set。
 * @param blob 带类型头的二进制 blob
 */
Set decode_set(std::string_view blob);

/**
 * List → blob。
 * @param l List 序列
 */
std::string encode_list(const List& l);
/**
 * blob → List。
 * @param blob 带类型头的二进制 blob
 */
List decode_list(std::string_view blob);

/**
 * Redis 风格 [start,stop]（可负下标）→ [lo, hi) 切片；非法返回 nullopt。
 * @param n 数量
 * @param start 起始下标或区间起点
 * @param stop 结束下标（含，可负）
 */
std::optional<std::pair<size_t, size_t>> normalize_range(int64_t n, int64_t start, int64_t stop);

/**
 * ZSet → blob。
 * @param z ZSet（member→score）
 */
std::string encode_zset(const ZSet& z);
/**
 * blob → ZSet。
 * @param blob 带类型头的二进制 blob
 */
ZSet decode_zset(std::string_view blob);
/**
 * 按 score 升序、同分按 member 得到有序列表（ZRange 用）。
 * @param z ZSet（member→score）
 */
std::vector<ZEntry> z_sorted(const ZSet& z);

}  // namespace openkv::compute::ds
