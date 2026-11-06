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

// @file router.hpp
// 按 region_id 投递 PeerMsg / ApplyMsg。

#pragma once

#include "opskv/raftstore/mailbox.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace opskv::raftstore {

class Router {
 public:
  using PeerMailbox = Mailbox<PeerMsg>;
  using ApplyMailbox = Mailbox<ApplyControl>;

  std::shared_ptr<PeerMailbox> register_peer(uint32_t region_id, size_t cap = 4096);
  std::shared_ptr<ApplyMailbox> register_apply(uint32_t region_id, size_t cap = 4096);
  void deregister(uint32_t region_id);

  bool send_raft(uint32_t region_id, PeerMsg msg);
  bool send_apply(uint32_t region_id, ApplyControl msg);
  bool broadcast_tick();

  std::shared_ptr<PeerMailbox> peer_mailbox(uint32_t region_id);
  std::shared_ptr<ApplyMailbox> apply_mailbox(uint32_t region_id);
  std::vector<uint32_t> peer_regions() const;

  size_t peer_queue_depth(uint32_t region_id) const;
  size_t apply_queue_depth(uint32_t region_id) const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<uint32_t, std::shared_ptr<PeerMailbox>> peers_;
  std::unordered_map<uint32_t, std::shared_ptr<ApplyMailbox>> applies_;
};

}  // namespace opskv::raftstore
