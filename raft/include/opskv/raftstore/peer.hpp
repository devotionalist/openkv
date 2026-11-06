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

// @file peer.hpp
// Peer：RawNode + Region meta + Hibernate / 冷卸载状态。

#pragma once

#include "opskv/raft/raw_node.hpp"
#include "opskv/raft/raft_node.hpp"
#include "opskv/region.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace opskv::raftstore {

struct PeerMeta {
  uint32_t region_id{0};
  opskv::KeyRange range;
  uint64_t epoch{0};
  std::string leader_id;
  std::vector<raft::Peer> peers;
};

/// 运行时 Peer：协议用 RawNode；兼容期可挂 RaftNode 适配器。
class Peer {
 public:
  Peer(uint32_t region_id, std::unique_ptr<raft::RawNode> raw);
  Peer(uint32_t region_id, std::shared_ptr<raft::RaftNode> legacy);

  uint32_t region_id() const { return meta_.region_id; }
  PeerMeta& meta() { return meta_; }
  const PeerMeta& meta() const { return meta_; }

  raft::RawNode* raw() { return raw_.get(); }
  raft::RaftNode* legacy() { return legacy_.get(); }
  std::shared_ptr<raft::RaftNode> legacy_shared() { return legacy_; }

  bool hibernating() const { return hibernating_.load(std::memory_order_relaxed); }
  void set_hibernating(bool v) { hibernating_.store(v, std::memory_order_relaxed); }
  void touch_activity();
  bool idle_for(std::chrono::milliseconds d) const;

  /// Phase 3：冷卸载后仅保留 meta；唤醒时由 Store 重新 load。
  bool unloaded() const { return unloaded_.load(std::memory_order_relaxed); }
  void mark_unloaded() {
    unloaded_.store(true, std::memory_order_relaxed);
    raw_.reset();
    legacy_.reset();
  }
  void mark_loaded(std::unique_ptr<raft::RawNode> raw) {
    raw_ = std::move(raw);
    unloaded_.store(false, std::memory_order_relaxed);
  }
  void mark_loaded(std::shared_ptr<raft::RaftNode> n) {
    legacy_ = std::move(n);
    unloaded_.store(false, std::memory_order_relaxed);
  }

 private:
  PeerMeta meta_;
  std::unique_ptr<raft::RawNode> raw_;
  std::shared_ptr<raft::RaftNode> legacy_;
  std::atomic<bool> hibernating_{false};
  std::atomic<bool> unloaded_{false};
  mutable std::mutex act_mu_;
  std::chrono::steady_clock::time_point last_active_{std::chrono::steady_clock::now()};
};

}  // namespace opskv::raftstore
