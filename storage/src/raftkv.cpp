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

// @file raftkv.cpp

#include "openkv/storage/raftkv.hpp"

namespace openkv::storage {

RaftKV::RaftKV(raftstore::Store* store) : store_(store) {}

raft::RaftNode* RaftKV::node(uint32_t region_id) {
  if (!store_) return nullptr;
  return store_->get(region_id);
}

bool RaftKV::is_leader(uint32_t region_id) const {
  auto* n = store_ ? store_->get(region_id) : nullptr;
  return n && n->is_leader();
}

raft::SoftState RaftKV::soft_state(uint32_t region_id) const {
  auto* n = store_ ? const_cast<raftstore::Store*>(store_)->get(region_id) : nullptr;
  return n ? n->soft_state() : raft::SoftState{};
}

void RaftKV::flush_ready(uint32_t region_id) {
  if (!store_) return;
  // 本 Region Wake；批内多个 propose 在 Store::on_raft_batch 合并一次 sync。
  store_->drive_ready(region_id);
}


/**
 * @brief 异步写入raftkv，写入raftkv后，需要等待apply 完成
 * 
 * @param region_id 区域ID
 * @param data 数据
 * @param assigned_index 分配的索引
 * @param error 错误信息
 * @return true 成功，false 失败
 */
bool RaftKV::async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                         std::string* error) {
  // 写路径入口（见 docs/flows/35-raft-module-call-flows.md）：
  //   propose 只改内存 RaftLog → request_ready 攒批 → Store 批末 persist_ready → Engine::consume
  //   → Apply worker 写 KV；客户端可见性由 apply 回调 / ProposalQueue 唤醒。
  auto* n = node(region_id);
  // 获取raft节点
  if (!n) {
    if (error) *error = "no_region";
    return false;
  }
  // 判断是否是leader
  if (!n->is_leader()) {
    if (error) *error = "not_leader";
    return false;
  }
  // 写入raftkv
  if (!n->propose(std::move(data), assigned_index)) {
    if (error) *error = "propose_failed";
    return false;
  }
  // 攒批，等待apply
  store_->request_ready(region_id);
  return true;
}

}  // namespace openkv::storage
