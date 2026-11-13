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

#include "openkv/storage/scheduler.hpp"

#include <utility>

namespace openkv::storage {

bool Latches::can_hold_locked(const std::vector<std::string>& keys) const {
  for (const auto& k : keys) {
    if (held_.find(k) != held_.end()) return false;
  }
  return true;
}

uint64_t Latches::grant_locked(const std::vector<std::string>& keys) {
  const uint64_t token = next_token_++;
  for (const auto& k : keys) held_[k] = token;
  return token;
}

void Latches::drain_async_locked(
    std::vector<std::pair<uint64_t, std::function<void(uint64_t)>>>* ready) {
  for (auto it = async_waiters_.begin(); it != async_waiters_.end();) {
    if (!can_hold_locked(it->keys)) {
      ++it;
      continue;
    }
    const uint64_t tok = grant_locked(it->keys);
    ready->emplace_back(tok, std::move(it->cb));
    it = async_waiters_.erase(it);
  }
}

uint64_t Latches::lock(const std::vector<std::string>& keys) {
  std::unique_lock<std::mutex> lk(mu_);
  const uint64_t token = next_token_++;
  sync_waiters_[token] = SyncWaiter{keys};
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
      sync_waiters_.erase(token);
      return token;
    }
    cv_.wait(lk);
  }
}

uint64_t Latches::try_lock(const std::vector<std::string>& keys) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!can_hold_locked(keys)) return 0;
  return grant_locked(keys);
}

void Latches::lock_async(std::vector<std::string> keys, std::function<void(uint64_t)> cb) {
  if (!cb) return;
  uint64_t tok = 0;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (can_hold_locked(keys)) {
      tok = grant_locked(keys);
    } else {
      async_waiters_.push_back(AsyncWaiter{std::move(keys), std::move(cb)});
      return;
    }
  }
  cb(tok);
}

void Latches::unlock(uint64_t token) {
  std::vector<std::pair<uint64_t, std::function<void(uint64_t)>>> ready;
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto it = held_.begin(); it != held_.end();) {
      if (it->second == token) it = held_.erase(it);
      else ++it;
    }
    drain_async_locked(&ready);
    cv_.notify_all();
  }
  for (auto& [tok, fn] : ready) {
    if (fn) fn(tok);
  }
}

bool Scheduler::schedule_write(uint32_t region_id, const std::string& latch_key, std::string data,
                               uint64_t* assigned_index, std::string* error) {
  uint64_t tok = 0;
  if (!schedule_write_hold(region_id, latch_key, std::move(data), assigned_index, &tok, error)) {
    return false;
  }
  unlock(tok);
  return true;
}

bool Scheduler::schedule_write_hold(uint32_t region_id, const std::string& latch_key,
                                    std::string data, uint64_t* assigned_index,
                                    uint64_t* latch_token, std::string* error) {
  if (latch_token) *latch_token = 0;
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
  if (!ok) {
    latches_.unlock(tok);
    return false;
  }
  if (latch_token) *latch_token = tok;
  else latches_.unlock(tok);
  return true;
}

}  // namespace openkv::storage
