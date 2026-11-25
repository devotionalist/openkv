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

// @file linear_read.cpp
// lease 快路径 + ReadIndex 多数派确认。

#include "openkv/raft/core/linear_read.hpp"

#include <mutex>

#include "openkv/raft/core/membership.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/core/ready_pump.hpp"
#include "openkv/raft/core/replication.hpp"

namespace openkv::raft {

bool LinearRead::lease_valid_locked() const {
  if (!n_.opts_.enable_lease_read || n_.opts_.leader_lease_ms == 0) return false;
  if (n_.role_ != Role::kLeader) return false;
  return std::chrono::steady_clock::now() < lease_expire_;
}

void LinearRead::renew_lease_locked() {
  lease_expire_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(n_.opts_.leader_lease_ms);
}

void LinearRead::note_read_index_ack(const std::string& peer_id) {
  if (pending_read_gen_ == 0) return;
  read_index_acks_.insert(peer_id);
  read_index_acks_.insert(n_.opts_.self_id);
  if (read_index_acks_.size() >= n_.membership_.majority()) {
    pending_read_gen_ = 0;
    read_cv_.notify_all();
  }
}

bool LinearRead::read_index(uint64_t* read_commit, uint32_t timeout_ms) {
  if (!read_commit) return false;
  {
    std::lock_guard<std::mutex> lock(n_.mu_);
    if (n_.role_ != Role::kLeader) return false;
    *read_commit = n_.commit_index_;
    // 单节点或租约仍有效：本地 commit 即可线性读。
    if (n_.membership_.majority() <= 1 || lease_valid_locked()) return true;
    pending_read_commit_ = n_.commit_index_;
    pending_read_gen_ = ++read_index_gen_;
    read_index_acks_.clear();
    read_index_acks_.insert(n_.opts_.self_id);
    n_.replication_.broadcast_replicate();
    n_.replication_.need_replicate_ = false;
  }
  n_.ready_pump_.flush_transport();
  {
    std::unique_lock<std::mutex> lock(n_.mu_);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (pending_read_gen_ != 0) {
      if (read_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
        pending_read_gen_ = 0;
        return false;
      }
    }
    if (n_.role_ != Role::kLeader) return false;
    *read_commit = pending_read_commit_;
    renew_lease_locked();
  }
  return true;
}

}  // namespace openkv::raft
