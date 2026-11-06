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

// @file watch_hub.cpp

#include "opskv/storage/watch_hub.hpp"

#include <limits>

namespace opskv::storage {

WatchHub::WatchHub(size_t capacity) : capacity_(capacity == 0 ? 4096 : capacity) {}

void WatchHub::push(WatchRecord rec) {
  std::lock_guard<std::mutex> lock(mu_);
  buf_.push_back(std::move(rec));
  while (buf_.size() > capacity_) {
    buf_.pop_front();
  }
}

std::vector<WatchRecord> WatchHub::poll(uint32_t region_id, uint64_t since, size_t limit) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<WatchRecord> out;
  if (limit == 0) limit = 64;
  for (const auto& r : buf_) {
    if (r.revision <= since) continue;
    if (region_id != std::numeric_limits<uint32_t>::max() && r.region_id != region_id) continue;
    out.push_back(r);
    if (out.size() >= limit) break;
  }
  return out;
}

}  // namespace opskv::storage
