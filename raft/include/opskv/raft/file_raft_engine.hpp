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

// @file file_raft_engine.hpp
// 便携 RaftEngine：共享根目录 + per-group 子目录；LogBatch 内 state+log 同批 tmp/rename。
// Meta / UT / 无 RocksDB 场景使用；Storage 生产路径优先 RocksRaftEngine。

#pragma once

#include "opskv/raft/raft_engine.hpp"

#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

namespace opskv::raft {

class FileRaftEngine : public RaftEngine {
 public:
  explicit FileRaftEngine(std::string root_dir, SyncMode sync = SyncMode::kData);

  const std::string& root_dir() const { return root_; }
  bool enabled() const { return !root_.empty(); }

  std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) override;
  size_t consume(RaftLogBatch& batch, bool sync) override;

  std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const override;
  std::optional<LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const override;
  size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end, size_t max_size,
                          std::vector<LogEntry>* to) const override;

  size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) override;
  void clean(uint64_t raft_group_id) override;

  SyncMode sync_mode() const override { return sync_; }
  void set_sync_mode(SyncMode m) override { sync_ = m; }

  /// 若存在旧版 RaftPersist（hard.state / log.entries）则导入为 group 0。
  bool migrate_legacy_persist_dir(const std::string& legacy_dir, uint64_t raft_group_id = 0);

 private:
  friend class FileLogBatch;

  struct GroupMut {
    std::optional<RaftLocalState> state;
    std::vector<LogEntry> appends;
    std::vector<std::pair<uint64_t, uint64_t>> cuts;  // [from, to)
  };

  std::filesystem::path group_dir(uint64_t id) const;
  void sync_path(const std::string& path) const;
  void sync_dir() const;
  bool read_state(uint64_t id, RaftLocalState* out) const;
  bool write_state(uint64_t id, const RaftLocalState& st, bool sync) const;
  bool read_all_entries(uint64_t id, std::vector<LogEntry>* out) const;
  bool write_all_entries(uint64_t id, const std::vector<LogEntry>& ents, bool sync) const;
  /// RLG2 纯追加；非 RLG2 返回 false（调用方应 rewrite）。
  bool append_entries_file(uint64_t id, const std::vector<LogEntry>& ents, bool sync) const;
  size_t apply_group_mut(uint64_t id, GroupMut& mut, bool sync);

  std::string root_;
  SyncMode sync_{SyncMode::kData};
  mutable std::mutex mu_;
};

}  // namespace opskv::raft
