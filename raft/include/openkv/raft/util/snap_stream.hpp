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
// 职责：InstallSnapshot **入站**分片缓冲——按 (from, shard, last_idx) 顺序聚合 blob。
// 架构：头文件实现；与 SnapWorker（出站）对称，本类只管收齐分片。
// 协作：Storage 入站 InstallSnapshot 分块路径使用；齐了再调 RaftNode::on_install_snapshot。
// 注意：要求按 offset 顺序追加；乱序应拒绝或重置该 Key。

#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "openkv/codec.hpp"

namespace openkv::raft {

inline constexpr size_t kDefaultSnapChunkBytes = 16 * 1024;  // 默认出站分片大小（字节）

// InstallSnapshot blob 前缀：ConfState CSV + KV 快照（Store 出站 / Replication 入站共用）。
inline constexpr std::string_view kMagicSnapConf = "SKVC1\n";

/**
 * 快照字节转 hex（行协议承载）。
 * @param s 原始字节
 * @return hex 串
 */
inline std::string snap_to_hex(std::string_view s) { return openkv::to_hex(s); }
/**
 * hex 解码回快照字节。
 * @param hex hex 串
 * @param out 输出缓冲
 * @return 是否解码成功
 */
inline bool snap_from_hex(std::string_view hex, std::string* out) {
  return openkv::from_hex(hex, out);
}

/* 入站快照分片状态（线程安全）。要求按 offset 顺序追加。 */
class SnapStreamBuffer {
 public:
  /* 一次快照流的身份：来源 + 分片 + 快照末 index。 */
  struct Key {
    std::string from;      // 发送方 peer id
    uint32_t shard{0};     // 分片 / Region id
    uint64_t last_idx{0};  // 快照 last_included_index
    bool operator==(const Key& o) const {
      return shard == o.shard && last_idx == o.last_idx && from == o.from;
    }
  };
  /* Key 的哈希（供 unordered_map）。 */
  struct KeyHash {
    size_t operator()(const Key& k) const {
      return std::hash<std::string>{}(k.from) ^ (static_cast<size_t>(k.shard) << 1) ^
             (static_cast<size_t>(k.last_idx) << 17);
    }
  };

  /**
   * 按序追加一片；done 且收齐则 *complete=true 并交出 *out_data。
   * @param key 流身份
   * @param term Leader 任期
   * @param last_term 快照末 term
   * @param offset 本片起始偏移
   * @param total 整包总字节
   * @param done 是否最后一片
   * @param chunk 本片数据
   * @param complete 输出：是否已收齐
   * @param out_data 收齐时交出完整 blob
   * @param out_term 收齐时写出 term
   * @param out_last_term 收齐时写出 last_term
   * @return false 表示乱序/元数据冲突等，应丢弃该流
   */
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
    if (offset != s.filled) {
      return false;
    }
    if (offset + chunk.size() > s.total) {
      return false;
    }
    s.buf.append(chunk.data(), chunk.size());
    s.filled = offset + chunk.size();
    if (!done) {
      *complete = false;
      return true;
    }
    if (s.filled != s.total) {
      return false;
    }
    *complete = true;
    *out_data = std::move(s.buf);
    *out_term = s.term;
    *out_last_term = s.last_term;
    sessions_.erase(key);
    return true;
  }

  /**
   * 丢弃某条未完成流（超时/失败）。
   * @param key 流身份
   */
  void drop(const Key& key) {
    std::lock_guard<std::mutex> lock(mu_);
    sessions_.erase(key);
  }

 private:
  /* 单条流的聚合缓冲。 */
  struct Session {
    uint64_t term{0};       // Leader 任期
    uint64_t last_term{0};  // 快照末 term
    uint64_t total{0};      // 期望总字节
    uint64_t filled{0};     // 已收字节（下一片 offset）
    bool started{false};    // 是否已见首片
    std::string buf;        // 已拼接内容
  };
  std::mutex mu_;                                       // 保护 sessions_
  std::unordered_map<Key, Session, KeyHash> sessions_;  // 进行中的流
};

}  // namespace openkv::raft
