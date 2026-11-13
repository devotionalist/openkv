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

struct WatchRecord {// 观察记录
  uint32_t region_id{0}; // 区域 ID
  uint64_t revision{0}; // 修订版
  bool deleted{false}; // 是否删除
  std::string key; // 键
  std::string value; // 值
};

class WatchHub {// 观察中心
 public:
  explicit WatchHub(size_t capacity = 4096);

  // 推送
  void push(WatchRecord rec);
  // 返回 revision > since 且 region 匹配（region_id==UINT32_MAX 表示全部）的事件。
  std::vector<WatchRecord> poll(uint32_t region_id, uint64_t since, size_t limit) const;

 private:
  size_t capacity_; // 容量
  mutable std::mutex mu_; // 互斥锁
  std::deque<WatchRecord> buf_; // 缓冲区
};

}  // namespace openkv::storage
