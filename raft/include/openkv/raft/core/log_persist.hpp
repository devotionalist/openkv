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

// @file log_persist.hpp
// Raft 日志/HardState 落盘：组 batch、刷引擎、Ready 路径 persist（协议锁策略见实现）。

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

#include "openkv/raft/core/ready.hpp"
#include "openkv/raft/engine/raft_engine.hpp"

namespace openkv::raft {

class RaftNode;

// 管「协议状态怎么写进 RaftEngine」。
class LogPersist {
 public:
  explicit LogPersist(RaftNode& node) : n_(node) {}  // 构造函数

  // external_io 只脏标记 Ready HardState；嵌入：攒批刷引擎。
  void persist_locked();                 // 持久化
  void persist_maybe_flush(bool force);  // 刷新
  void engine_flush_locked(bool sync);   // 刷新引擎
  std::unique_ptr<RaftLogBatch> build_engine_batch_locked(uint64_t& out_first,
                                                          uint64_t& out_last);  // 构建引擎批次
  void commit_stable_locked(uint64_t first, uint64_t last);                     // 提交稳定落盘
  // Store persist_fn：组 batch 后在 persist_io_mu_ 下 consume（可不持协议锁）。
  void persist_ready(const Ready& rd);  // 持久化准备

  uint32_t persist_pending_{0};  // 等待持久化
  bool persist_dirty_{false};    // 脏状态
  // 已稳定落盘的日志窗（增量 flush 基准；0=还没落过）。
  uint64_t stable_first_index_{0};  // 稳定落盘的日志窗
  uint64_t stable_last_index_{0};   // 稳定落盘的日志窗
  // 串行化 engine_->consume；不持协议 mu_，允许 propose 与写盘重叠。
  std::mutex persist_io_mu_;  // 持久化io互斥锁

 private:
  RaftNode& n_;  // 节点引用
};

}  // namespace openkv::raft
