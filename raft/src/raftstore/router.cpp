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

// @file router.cpp

#include "opskv/raftstore/router.hpp"

namespace opskv::raftstore {

std::shared_ptr<Router::PeerMailbox> Router::register_peer(uint32_t region_id, size_t cap) {
  std::lock_guard<std::mutex> lk(mu_);
  auto mb = std::make_shared<PeerMailbox>(cap);
  peers_[region_id] = mb;
  return mb;
}

std::shared_ptr<Router::ApplyMailbox> Router::register_apply(uint32_t region_id, size_t cap) {
  std::lock_guard<std::mutex> lk(mu_);
  auto mb = std::make_shared<ApplyMailbox>(cap);
  applies_[region_id] = mb;
  return mb;
}

void Router::deregister(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  if (auto it = peers_.find(region_id); it != peers_.end()) {
    it->second->close();
    peers_.erase(it);
  }
  if (auto it = applies_.find(region_id); it != applies_.end()) {
    it->second->close();
    applies_.erase(it);
  }
}

bool Router::send_raft(uint32_t region_id, PeerMsg msg) {
  std::shared_ptr<PeerMailbox> mb;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = peers_.find(region_id);
    if (it == peers_.end()) return false;
    mb = it->second;
  }
  return mb->try_send(std::move(msg));
}

bool Router::send_apply(uint32_t region_id, ApplyControl msg) {
  std::shared_ptr<ApplyMailbox> mb;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = applies_.find(region_id);
    if (it == applies_.end()) return false;
    mb = it->second;
  }
  return mb->try_send(std::move(msg));
}

bool Router::broadcast_tick() {
  std::vector<std::shared_ptr<PeerMailbox>> mbs;
  {
    std::lock_guard<std::mutex> lk(mu_);
    mbs.reserve(peers_.size());
    for (auto& [_, mb] : peers_) mbs.push_back(mb);
  }
  bool any = false;
  for (auto& mb : mbs) {
    if (mb->try_send(TickMsg{})) any = true;
  }
  return any;
}

std::shared_ptr<Router::PeerMailbox> Router::peer_mailbox(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = peers_.find(region_id);
  return it == peers_.end() ? nullptr : it->second;
}

std::shared_ptr<Router::ApplyMailbox> Router::apply_mailbox(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = applies_.find(region_id);
  return it == applies_.end() ? nullptr : it->second;
}

std::vector<uint32_t> Router::peer_regions() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  out.reserve(peers_.size());
  for (const auto& [id, _] : peers_) out.push_back(id);
  return out;
}

size_t Router::peer_queue_depth(uint32_t region_id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = peers_.find(region_id);
  return it == peers_.end() ? 0 : it->second->size();
}

size_t Router::apply_queue_depth(uint32_t region_id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = applies_.find(region_id);
  return it == applies_.end() ? 0 : it->second->size();
}

}  // namespace opskv::raftstore
