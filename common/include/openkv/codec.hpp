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

// @file codec.hpp
// 公共编解码：hex（行协议 / 快照分片共用），避免各模块复制实现。

#pragma once

#include <string>
#include <string_view>

namespace openkv {

/**
 * 字节串 → 小写 hex（每字节两位）。
 * @param s 原始字节
 * @return hex 字符串（长度 = 2 * s.size()）
 */
inline std::string to_hex(std::string_view s) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(s.size() * 2, '\0');
  for (size_t i = 0; i < s.size(); ++i) {
    const auto c = static_cast<unsigned char>(s[i]);
    out[i * 2] = kHex[c >> 4];
    out[i * 2 + 1] = kHex[c & 0xf];
  }
  return out;
}

/**
 * hex → 字节串；奇数长度或非法字符返回 false，*out 未保证完整。
 * @param hex 十六进制文本（大小写均可）
 * @param out 写出解码结果；不可空
 * @return 是否解码成功
 */
inline bool from_hex(std::string_view hex, std::string* out) {
  if (!out || hex.size() % 2 != 0) {
    return false;
  }
  out->clear();
  out->reserve(hex.size() / 2);
  auto nibble = [](char c) -> int {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  };
  for (size_t i = 0; i < hex.size(); i += 2) {
    const int hi = nibble(hex[i]);
    const int lo = nibble(hex[i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

}  // namespace openkv
