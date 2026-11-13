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

// @file raftkv.hpp
// RaftKV：propose / 线性读的唯一入口（经 Store Ready 外提）。

#pragma once

#include "openkv/raft/types.hpp"
#include "openkv/raftstore/store.hpp"

#include <cstdint>
#include <string>

namespace openkv::storage {

class RaftKV {
 public:
  explicit RaftKV(raftstore::Store* store);

  /// 写：propose + request_ready；不等 apply（由 ProposalQueue / wait_async 等待）。
  bool async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                   std::string* error);

  bool is_leader(uint32_t region_id) const;
  raft::SoftState soft_state(uint32_t region_id) const;
  raft::RaftNode* node(uint32_t region_id);

  /// propose 后驱动 Ready（经 Store::drive_ready；批内合并 group commit）。
  void flush_ready(uint32_t region_id);

 private:
  raftstore::Store* store_{nullptr};
};

}  // namespace openkv::storage
