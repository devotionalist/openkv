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

/* 管「协议状态怎么写进 RaftEngine」。 */
class LogPersist {
 public:
  /**
   * 绑定所属 RaftNode。
   * @param node 宿主节点
   */
  explicit LogPersist(RaftNode& node) : n_(node) {}

  /**
   * external_io 只脏标记 Ready HardState；嵌入：攒批刷引擎。须已持协议锁。
   */
  void persist_locked();
  /**
   * 按 persist_batch_n / force 决定是否真刷引擎。
   * @param force true 则无视攒批计数立刻刷
   */
  void persist_maybe_flush(bool force);
  /**
   * 持协议锁把当前脏日志/HardState consume 进引擎。
   * @param sync 是否要求引擎 sync
   */
  void engine_flush_locked(bool sync);
  /**
   * 组一版 RaftLogBatch；写出本批 first/last index。
   * @param out_first 输出本批首 index
   * @param out_last 输出本批末 index
   * @return 批次；可空
   */
  std::unique_ptr<RaftLogBatch> build_engine_batch_locked(uint64_t& out_first, uint64_t& out_last);
  /**
   * 引擎 consume 成功后推进稳定落盘窗口。
   * @param first 本批首 index
   * @param last 本批末 index
   */
  void commit_stable_locked(uint64_t first, uint64_t last);
  /**
   * Store persist_fn：组 batch 后在 persist_io_mu_ 下 consume（可不持协议锁）。
   * @param rd Ready 批次
   */
  void persist_ready(const Ready& rd);

  uint32_t persist_pending_{0};  // 嵌入路径已脏未刷次数
  bool persist_dirty_{false};    // 是否有未刷脏数据
  // 已稳定落盘的日志窗（增量 flush 基准；0=还没落过）。
  uint64_t stable_first_index_{0};  // 稳定窗首 index
  uint64_t stable_last_index_{0};   // 稳定窗末 index
  std::mutex persist_io_mu_;        // 串行化 engine_->consume；不持协议 mu_

 private:
  RaftNode& n_;  // 宿主节点
};

}  // namespace openkv::raft
