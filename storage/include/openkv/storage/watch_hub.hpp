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

// @file watch_hub.hpp
// Apply 路径上的近期变更环缓，供 WATCH_POLL 拉取。

#pragma once

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace openkv::storage {

/* 一条可被 Watch 拉取的变更（apply 后写入）。 */
struct WatchRecord {
  uint32_t region_id{0};  // 所属 Region
  uint64_t revision{0};   // 通常为 Raft apply index
  bool deleted{false};    // true=删除事件
  std::string key;        // 用户键
  std::string value;      // 新值；删除时可为旧值或空
};

/* 内存环缓：最近 N 条 apply 变更，供短时 WATCH_POLL。 */
class WatchHub {
 public:
  /**
   * 构造固定容量环缓；满则丢最旧。
   * @param capacity 最多保留条数；默认 4096
   */
  explicit WatchHub(size_t capacity = 4096);

  /**
   * 追加一条变更；满则弹出队头。
   * @param rec 变更记录（按值移入）
   */
  void push(WatchRecord rec);
  /**
   * 拉取 revision > since 且 region 匹配的事件。
   * @param region_id 目标 Region；UINT32_MAX 表示全部
   * @param since 只返回大于该 revision 的事件
   * @param limit 最多返回条数
   * @return 升序事件列表（可能少于 limit）
   */
  std::vector<WatchRecord> poll(uint32_t region_id, uint64_t since, size_t limit) const;

 private:
  size_t capacity_;              // 环缓上限
  mutable std::mutex mu_;        // 保护 buf_
  std::deque<WatchRecord> buf_;  // 最近变更（旧→新）
};

}  // namespace openkv::storage
