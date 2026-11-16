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

// @file ready_pump.cpp
// enqueue / flush / has_ready / take_ready / notify。

#include "openkv/raft/core/ready_pump.hpp"

#include "openkv/raft/core/raft_node.hpp"

#include <mutex>
#include <utility>

namespace openkv::raft {

void ReadyPump::enqueue(std::string peer_id, std::string type, std::string payload) {
  outbound_.push_back(Outbound{std::move(peer_id), std::move(type), std::move(payload)});
}

void ReadyPump::flush_transport() {
  if (n_.opts_.external_io) return;  // 消息留在 outbound_，由 take_ready 外提
  std::vector<Outbound> batch;
  {
    std::lock_guard<std::mutex> lock(n_.mu_);
    batch.swap(outbound_);
  }
  for (auto& m : batch) {
    if (n_.send_) n_.send_(m.peer_id, m.type, m.payload);
  }
}

bool ReadyPump::has_ready() const {
  std::lock_guard<std::mutex> lock(n_.mu_);
  return n_.ready_io_.ready_hs_dirty_ || !n_.ready_io_.ready_unstable_.empty() ||
         !n_.ready_io_.ready_committed_.empty() || !outbound_.empty() ||
         !n_.ready_io_.ready_snaps_.empty();
}

SoftState ReadyPump::soft_state_locked() const {
  SoftState st;
  st.role = n_.role_;
  st.leader_id = n_.leader_id_;
  st.term = n_.term_;
  st.commit_index = n_.commit_index_;
  st.last_applied = n_.last_applied_;
  st.election_count = n_.election_.election_count_;
  st.snapshot_send_count = n_.replication_.snapshot_send_count_;
  st.in_joint = n_.membership_.in_joint_;
  st.conf_change_busy = n_.membership_.in_joint_ || n_.membership_.auto_leave_pending_;
  return st;
}

Ready ReadyPump::take_ready() {
  std::lock_guard<std::mutex> lock(n_.mu_);
  Ready rd;
  if (n_.ready_io_.ready_hs_dirty_) {
    rd.hard_state = n_.ready_io_.ready_hs_;
    rd.must_sync = true;
    n_.ready_io_.ready_hs_dirty_ = false;
  }
  rd.entries.swap(n_.ready_io_.ready_unstable_);
  rd.committed_entries.swap(n_.ready_io_.ready_committed_);
  rd.messages.reserve(outbound_.size());
  for (auto& m : outbound_) {
    rd.messages.push_back(Message{std::move(m.peer_id), std::move(m.type), std::move(m.payload)});
  }
  outbound_.clear();
  if (!n_.ready_io_.ready_snaps_.empty()) {
    rd.snapshot = n_.ready_io_.ready_snaps_.front();
    n_.ready_io_.ready_snaps_.erase(n_.ready_io_.ready_snaps_.begin());
  }
  rd.soft_state = soft_state_locked();
  return rd;
}

void ReadyPump::advance(const Ready& /*rd*/) {
  std::lock_guard<std::mutex> lock(n_.mu_);
  if (!n_.opts_.external_io) return;
  // 普通条目 last_applied 由 report_applied（ApplyFsm）推进，避免 propose 误以为已落 KV。
}

void ReadyPump::set_ready_notify(std::function<void()> cb) { ready_notify_ = std::move(cb); }

void ReadyPump::maybe_notify_ready() {
  if (!n_.opts_.external_io || !ready_notify_) return;
  if (has_ready()) ready_notify_();
}

}  // namespace openkv::raft
