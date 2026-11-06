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

// @file scheduler.cpp

#include "opskv/storage/scheduler.hpp"

namespace opskv::storage {

uint64_t Latches::lock(const std::vector<std::string>& keys) {
  std::unique_lock<std::mutex> lk(mu_);
  const uint64_t token = next_token_++;
  waiters_[token] = Waiter{keys, false};
  for (;;) {
    bool blocked = false;
    for (const auto& k : keys) {
      auto it = held_.find(k);
      if (it != held_.end() && it->second != token) {
        blocked = true;
        break;
      }
    }
    if (!blocked) {
      for (const auto& k : keys) held_[k] = token;
      waiters_.erase(token);
      return token;
    }
    cv_.wait(lk);
  }
}

void Latches::unlock(uint64_t token) {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto it = held_.begin(); it != held_.end();) {
    if (it->second == token) it = held_.erase(it);
    else ++it;
  }
  cv_.notify_all();
}

bool Scheduler::schedule_write(uint32_t region_id, const std::string& latch_key, std::string data,
                               uint64_t* assigned_index, std::string* error) {
  if (!write_fn_) {
    if (error) *error = "no_write_fn";
    return false;
  }
  const uint64_t tok = latches_.lock({latch_key});
  bool ok = false;
  try {
    ok = write_fn_(region_id, std::move(data), assigned_index, error);
  } catch (...) {
    latches_.unlock(tok);
    throw;
  }
  latches_.unlock(tok);
  return ok;
}

}  // namespace opskv::storage
