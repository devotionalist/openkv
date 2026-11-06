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

// @file peer.cpp

#include "opskv/raftstore/peer.hpp"

namespace opskv::raftstore {

Peer::Peer(uint32_t region_id, std::unique_ptr<raft::RawNode> raw) : raw_(std::move(raw)) {
  meta_.region_id = region_id;
}

Peer::Peer(uint32_t region_id, std::shared_ptr<raft::RaftNode> legacy)
    : legacy_(std::move(legacy)) {
  meta_.region_id = region_id;
}

void Peer::touch_activity() {
  std::lock_guard<std::mutex> lk(act_mu_);
  last_active_ = std::chrono::steady_clock::now();
  hibernating_.store(false, std::memory_order_relaxed);
}

bool Peer::idle_for(std::chrono::milliseconds d) const {
  std::lock_guard<std::mutex> lk(act_mu_);
  return std::chrono::steady_clock::now() - last_active_ >= d;
}

}  // namespace opskv::raftstore
