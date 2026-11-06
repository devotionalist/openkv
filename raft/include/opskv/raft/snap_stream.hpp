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

// @file snap_stream.hpp
// InstallSnapshot 分片接收缓冲：按 (from, shard, last_idx) 顺序聚合。

#pragma once

#include "opskv/codec.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace opskv::raft {

inline constexpr size_t kDefaultSnapChunkBytes = 16 * 1024;

inline std::string snap_to_hex(std::string_view s) { return opskv::to_hex(s); }
inline bool snap_from_hex(std::string_view hex, std::string* out) {
  return opskv::from_hex(hex, out);
}

/// 入站快照分片状态（线程安全）。要求按 offset 顺序追加。
class SnapStreamBuffer {
 public:
  struct Key {
    std::string from;
    uint32_t shard{0};
    uint64_t last_idx{0};
    bool operator==(const Key& o) const {
      return shard == o.shard && last_idx == o.last_idx && from == o.from;
    }
  };
  struct KeyHash {
    size_t operator()(const Key& k) const {
      return std::hash<std::string>{}(k.from) ^ (static_cast<size_t>(k.shard) << 1) ^
             (static_cast<size_t>(k.last_idx) << 17);
    }
  };

  /// done 且收齐则 *complete=true 并交出 *out_data（二进制）。
  bool append(const Key& key, uint64_t term, uint64_t last_term, uint64_t offset, uint64_t total,
              bool done, std::string_view chunk, bool* complete, std::string* out_data,
              uint64_t* out_term, uint64_t* out_last_term) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& s = sessions_[key];
    if (!s.started) {
      s.term = term;
      s.last_term = last_term;
      s.total = total;
      s.buf.reserve(static_cast<size_t>(total));
      s.started = true;
    } else if (s.total != total || s.term != term || s.last_term != last_term) {
      sessions_.erase(key);
      return false;
    }
    if (offset != s.filled) return false;
    if (offset + chunk.size() > s.total) return false;
    s.buf.append(chunk.data(), chunk.size());
    s.filled = offset + chunk.size();
    if (!done) {
      *complete = false;
      return true;
    }
    if (s.filled != s.total) return false;
    *complete = true;
    *out_data = std::move(s.buf);
    *out_term = s.term;
    *out_last_term = s.last_term;
    sessions_.erase(key);
    return true;
  }

  void drop(const Key& key) {
    std::lock_guard<std::mutex> lock(mu_);
    sessions_.erase(key);
  }

 private:
  struct Session {
    uint64_t term{0};
    uint64_t last_term{0};
    uint64_t total{0};
    uint64_t filled{0};
    bool started{false};
    std::string buf;
  };
  std::mutex mu_;
  std::unordered_map<Key, Session, KeyHash> sessions_;
};

}  // namespace opskv::raft
