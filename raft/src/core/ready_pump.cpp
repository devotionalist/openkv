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

#include <mutex>
#include <utility>

#include "openkv/raft/core/raft_node.hpp"

namespace openkv::raft {

// 【非网络】只攒出站消息；external_io 下等 take_ready，嵌入路径见 flush_transport。
void ReadyPump::enqueue(std::string peer_id, std::string type, std::string payload) {
  outbound_.push_back(Outbound{std::move(peer_id), std::move(type), std::move(payload)});
}

// external_io：不发，留给 Store。嵌入路径：经 send_（常即 GrpcRaftTransport::bind）→ 真正出网。
void ReadyPump::flush_transport() {
  if (n_.opts_.external_io) return;
  std::vector<Outbound> batch;
  {
    std::lock_guard<std::mutex> lock(n_.mu_);
    batch.swap(outbound_);
  }
  for (auto& m : batch) {
    // 【出网入口·嵌入】send_ 最终进 gRPC 发送队列（见 grpc_transport.cpp bind）。
    if (n_.send_) n_.send_(m.peer_id, m.type, m.payload);
  }
}

// HS / 未落盘日志 / 待 apply / 出站 / 快照 任一有货 → Store 该拉 Ready。
bool ReadyPump::has_ready() const {
  std::lock_guard<std::mutex> lock(n_.mu_);
  return n_.ready_io_.ready_hs_dirty_               // 硬状态脏
         || !n_.ready_io_.ready_unstable_.empty()   // 未落盘日志
         || !n_.ready_io_.ready_committed_.empty()  // 待提交日志
         || !outbound_.empty()                      // 出站 消息
         || !n_.ready_io_.ready_snaps_.empty()      // 快照 消息
      ;                                             // 任一有货 → Store 该拉 Ready
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

/**
 * @brief 获取待发送消息：获取待发送消息是获取待发送消息的外部io，用于获取待发送消息的外部io。
 *
 * 如果硬状态脏，则获取硬状态。
 * 否则，获取未落盘日志。
 * 否则，获取待提交日志。
 * 否则，获取出站消息。
 * 否则，获取快照消息。
 */
Ready ReadyPump::take_ready() {
  std::lock_guard<std::mutex> lock(n_.mu_);
  Ready rd;
  if (n_.ready_io_.ready_hs_dirty_) {  // 硬状态脏
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
