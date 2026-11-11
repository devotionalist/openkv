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
// 活跃 start_ts 跟踪器。

#include "openkv/compute/active_ts.hpp"

namespace openkv::compute {

ActiveTsTracker::ActiveTsTracker(std::chrono::milliseconds ttl) : ttl_(ttl) {
  if (ttl_.count() <= 0) ttl_ = std::chrono::seconds(60);
}

void ActiveTsTracker::add(uint64_t ts) {
  if (ts == 0) return;
  std::lock_guard<std::mutex> lock(mu_);
  m_[ts] = std::chrono::steady_clock::now();
}

void ActiveTsTracker::remove(uint64_t ts) {
  std::lock_guard<std::mutex> lock(mu_);
  m_.erase(ts);
}

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
