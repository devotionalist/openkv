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

// @file raft_engine.hpp
// RaftEngine + RaftLogBatch（HardState/entries 同批原子落盘）。
//
// 读侧供 RaftNode 恢复；写侧经 LogBatch::append/put_raft_state → consume(sync)。

#pragma once

#include "opskv/raft/raft_persist.hpp"
#include "opskv/raft/types.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace opskv::raft {

/// RaftLocalState：HardState + 日志窗口边界。
struct RaftLocalState {
  uint64_t term{0};
  std::string voted_for;
  uint64_t commit_index{0};
  uint64_t last_applied{0};
  uint64_t first_index{1};
  uint64_t snapshot_term{0};
  uint64_t last_index{0};

  PersistedHardState to_persisted() const {
    PersistedHardState hs;
    hs.term = term;
    hs.voted_for = voted_for;
    hs.commit_index = commit_index;
    hs.last_applied = last_applied;
    hs.first_index = first_index;
    hs.snapshot_term = snapshot_term;
    return hs;
  }

  static RaftLocalState from_persisted(const PersistedHardState& hs, uint64_t last_index = 0) {
    RaftLocalState st;
    st.term = hs.term;
    st.voted_for = hs.voted_for;
    st.commit_index = hs.commit_index;
    st.last_applied = hs.last_applied;
    st.first_index = hs.first_index;
    st.snapshot_term = hs.snapshot_term;
    st.last_index = last_index;
    return st;
  }
};

/// 一次原子写批次（RaftLogBatch）。
class RaftLogBatch {
 public:
  virtual ~RaftLogBatch() = default;

  /// 追加 entries；不更新 RaftLocalState（由 put_raft_state 负责）。
  virtual void append(uint64_t raft_group_id, std::vector<LogEntry> entries) = 0;
  /// 删除 [from, to) 内将被覆盖的旧日志。
  virtual void cut_logs(uint64_t raft_group_id, uint64_t from, uint64_t to) = 0;
  virtual void put_raft_state(uint64_t raft_group_id, const RaftLocalState& state) = 0;
  virtual bool empty() const = 0;
};

/// Multi-Raft 共享引擎（RaftEngine）。
class RaftEngine {
 public:
  virtual ~RaftEngine() = default;

  virtual std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) = 0;
  /// 消费 batch；sync=true 时 fsync/WriteOptions.sync。返回写入字节近似值。
  virtual size_t consume(RaftLogBatch& batch, bool sync) = 0;

  virtual std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const = 0;
  virtual std::optional<LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const = 0;
  /// 拉取 [begin, end) 到 to；受 max_size 限制。返回条数。
  virtual size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end,
                                  size_t max_size, std::vector<LogEntry>* to) const = 0;

  /// 删除 [from, to) 日志；返回删除条数。
  virtual size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) = 0;
  /// 销毁该 group 的 state + 全部日志。
  virtual void clean(uint64_t raft_group_id) = 0;

  virtual SyncMode sync_mode() const = 0;
  virtual void set_sync_mode(SyncMode m) = 0;
};

/// 从引擎装载 group → PersistedHardState + entries（RaftNode 恢复用）。
bool load_group_from_engine(const RaftEngine& eng, uint64_t raft_group_id, PersistedHardState* hs,
                            std::vector<LogEntry>* entries);

/// 把当前内存 log + hs 整窗写入引擎（冷启动兼容 / 全量 flush）。
size_t save_group_to_engine(RaftEngine& eng, uint64_t raft_group_id, const PersistedHardState& hs,
                            const RaftLog& log, bool sync);

// 二进制编解码（File / Rocks 共用）
std::string encode_raft_local_state(const RaftLocalState& st);
bool decode_raft_local_state(std::string_view raw, RaftLocalState* st);
std::string encode_log_entry(const LogEntry& e);
bool decode_log_entry(std::string_view raw, LogEntry* e);

}  // namespace opskv::raft
