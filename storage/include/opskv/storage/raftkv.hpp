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

#include "opskv/raft/types.hpp"
#include "opskv/raftstore/store.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace opskv::storage {

class RaftKV {
 public:
  using ApplyWaitFn = std::function<bool(uint32_t region_id, uint64_t index, uint32_t timeout_ms)>;

  explicit RaftKV(raftstore::Store* store);

  void set_apply_wait(ApplyWaitFn fn) { apply_wait_ = std::move(fn); }

  /// 写：propose + 驱动 Ready；可选等待 apply。
  bool async_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                   std::string* error);
  bool sync_write(uint32_t region_id, std::string data, uint64_t* assigned_index,
                  std::string* error, uint32_t wait_ms = 5000);

  bool is_leader(uint32_t region_id) const;
  raft::SoftState soft_state(uint32_t region_id) const;
  raft::RaftNode* node(uint32_t region_id);
  raft::RawNode* raw(uint32_t region_id);

  /// propose / step 后立刻处理 Ready。
  void flush_ready(uint32_t region_id);

 private:
  raftstore::Store* store_{nullptr};
  ApplyWaitFn apply_wait_;
};

}  // namespace opskv::storage
