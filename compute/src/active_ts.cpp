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

// @file active_ts.cpp
// 活跃 start_ts 跟踪器实现。

#include "openkv/compute/active_ts.hpp"

namespace openkv::compute {

/// 非法/非正 ttl 回落到 60s，避免永不过期挡 GC。
ActiveTsTracker::ActiveTsTracker(std::chrono::milliseconds ttl) : ttl_(ttl) {
  if (ttl_.count() <= 0) ttl_ = std::chrono::seconds(60);
}

/// 记录 ts→now；ts==0 忽略（无效时间戳）。
void ActiveTsTracker::add(uint64_t ts) {
  if (ts == 0) return;
  std::lock_guard<std::mutex> lock(mu_);
  m_[ts] = std::chrono::steady_clock::now();
}

/// 事务/Put 结束后摘掉该 ts。
void ActiveTsTracker::remove(uint64_t ts) {
  std::lock_guard<std::mutex> lock(mu_);
  m_.erase(ts);
}

/// 边扫边剔过期条目，返回仍活跃的最小 ts（空则 UINT64_MAX）。
uint64_t ActiveTsTracker::min() {
  std::lock_guard<std::mutex> lock(mu_);
  const auto now = std::chrono::steady_clock::now();
  uint64_t out = std::numeric_limits<uint64_t>::max();
  for (auto it = m_.begin(); it != m_.end();) {
    if (now - it->second > ttl_) {
      it = m_.erase(it);
      continue;
    }
    if (it->first < out) out = it->first;
    ++it;
  }
  return out;
}

}  // namespace openkv::compute
