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

// @file log_persist.cpp
// 组 batch、刷引擎、Ready persist。

#include "openkv/raft/core/log_persist.hpp"

#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/engine/sync_mode.hpp"

#include <algorithm>
#include <vector>

namespace openkv::raft {

void LogPersist::persist_locked() {
  if (n_.opts_.external_io) {
    n_.ready_io_.ready_hs_dirty_ = true;
    n_.ready_io_.ready_hs_.term = n_.term_;
    n_.ready_io_.ready_hs_.vote = n_.voted_for_;
    n_.ready_io_.ready_hs_.commit = n_.commit_index_;
    return;
  }
  persist_dirty_ = true;
  ++persist_pending_;
  persist_maybe_flush(false);
}

void LogPersist::persist_maybe_flush(bool force) {
  if (n_.opts_.external_io) return;
  if (!n_.engine_) return;
  if (!persist_dirty_) return;
  if (!force && persist_pending_ < n_.opts_.persist_batch_n) return;
  const bool sync = raft_log_sync(n_.opts_.persist_sync);
  engine_flush_locked(sync);
  persist_dirty_ = false;
  persist_pending_ = 0;
}

std::unique_ptr<RaftLogBatch> LogPersist::build_engine_batch_locked(uint64_t& out_first,
                                                                   uint64_t& out_last) {
  if (!n_.engine_) return nullptr;
  out_first = n_.log_.first_index();
  out_last = n_.log_.last_index();
  const uint64_t first = out_first;
  const uint64_t last = out_last;

  auto batch = n_.engine_->make_log_batch();

  if (stable_first_index_ != 0 && first > stable_first_index_) {
    batch->cut_logs(n_.group_id_, stable_first_index_, first);
  }
  if (stable_last_index_ != 0 && last < stable_last_index_) {
    batch->cut_logs(n_.group_id_, last + 1, stable_last_index_ + 1);
  }

  uint64_t from = 0;
  if (stable_last_index_ == 0) {
    from = first;
  } else if (last > stable_last_index_) {
    from = std::max(stable_last_index_ + 1, first);
  }
  if (from != 0 && last >= from) {
    if (stable_last_index_ != 0 && from <= stable_last_index_) {
      batch->cut_logs(n_.group_id_, from, stable_last_index_ + 1);
    }
    std::vector<LogEntry> delta;
    delta.reserve(static_cast<size_t>(last - from + 1));
    for (uint64_t i = from; i <= last; ++i) {
      if (auto e = n_.log_.entry(i)) delta.push_back(*e);
    }
    if (!delta.empty()) batch->append(n_.group_id_, std::move(delta));
  }

  PersistedHardState hs;
  hs.term = n_.term_;
  hs.voted_for = n_.voted_for_;
  hs.commit_index = n_.commit_index_;
  hs.last_applied = n_.last_applied_;
  hs.first_index = first;
  hs.snapshot_term = n_.log_.snapshot_term();
  batch->put_raft_state(n_.group_id_, RaftLocalState::from_persisted(hs, last));
  return batch;
}

void LogPersist::commit_stable_locked(uint64_t first, uint64_t last) {
  if (last > stable_last_index_) {
    stable_last_index_ = last;
    stable_first_index_ = first;
  } else if (last == stable_last_index_ && first > stable_first_index_) {
    stable_first_index_ = first;
  }
}

void LogPersist::engine_flush_locked(bool sync) {
  if (!n_.engine_) return;
  uint64_t first = 0;
  uint64_t last = 0;
  auto batch = build_engine_batch_locked(first, last);
  if (!batch || batch->empty()) return;
  n_.engine_->consume(*batch, sync);
  commit_stable_locked(first, last);
}

void LogPersist::persist_ready(const Ready& rd) {
  // Store persist_fn：持协议锁只组 batch；consume/fsync 在 persist_io_mu_ 下。
  std::unique_ptr<RaftLogBatch> batch;
  bool sync = false;
  uint64_t first = 0;
  uint64_t last = 0;
  {
    std::lock_guard<std::mutex> lock(n_.mu_);
    if (!n_.opts_.external_io || !n_.engine_) return;
    const bool want_sync = rd.must_sync || n_.ready_io_.ready_hs_dirty_ || !rd.entries.empty();
    sync = want_sync && raft_log_sync(n_.opts_.persist_sync);
    batch = build_engine_batch_locked(first, last);
    n_.ready_io_.ready_hs_dirty_ = false;
    persist_dirty_ = false;
    persist_pending_ = 0;
  }
  if (!batch || batch->empty()) return;
  {
    std::lock_guard<std::mutex> io(persist_io_mu_);
    n_.engine_->consume(*batch, sync);
  }
  std::lock_guard<std::mutex> lock(n_.mu_);
  commit_stable_locked(first, last);
}

}  // namespace openkv::raft
