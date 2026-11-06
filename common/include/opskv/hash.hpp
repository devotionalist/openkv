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

// @file hash.hpp
// SeaStarKV 分片哈希工具（header-only 为主）。
//
// 架构位置：compute 层 Router 与 storage 层分片路由共用同一 hash→shard 算法，保证 key 落点一致。
#pragma once

#include <cstdint>
#include <string_view>

namespace opskv {

// FNV-1a 64-bit — 稳定、无依赖，用于 Key → Shard 映射
/// 对任意字节序列计算 FNV-1a 64 位哈希；跨进程/版本稳定，适合分片。
inline uint64_t fnv1a64(std::string_view data) noexcept {
  uint64_t hash = 14695981039346656037ULL;
  for (unsigned char c : data) {
    hash ^= c;
    hash *= 1099511628211ULL;
  }
  return hash;
}

/// 将 key 映射到 [0, shard_count) 分片号；shard_count==0 时返回 0（防御性约定）。
inline uint32_t shard_of(std::string_view key, uint32_t shard_count) noexcept {
  if (shard_count == 0) {
    return 0;
  }
  return static_cast<uint32_t>(fnv1a64(key) % shard_count);
}

}  // namespace opskv
