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
// 职责：便携文件版 RaftEngine——共享 root，按 group 子目录存放 state + log。
// 架构：Meta / 单测 / 无 RocksDB 场景；生产 Storage 使用 storage::RocksRaftEngine。
// 协作：实现 RaftEngine；容器格式见 raft_log_file；布局见 raft/ARCHITECTURE.md §5。
// 注意：consume 在单锁内提交；IO 失败返回 0 且打日志，调用方不得推进持久化游标。

#pragma once

#include "openkv/raft/raft_engine.hpp"

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace openkv::raft {

class FileRaftEngine final : public RaftEngine {
 public:
  /// root_dir 为空则所有写操作 no-op（disabled）。
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

  /// 导入旧版 RaftPersist 目录（hard.state / log.entries）到指定 group。
  bool migrate_legacy_persist_dir(const std::string& legacy_dir, uint64_t raft_group_id = 0);

 private:
  friend class FileLogBatch;

  /// 单 group 在一次 LogBatch 内的累积变更。
  struct GroupMut {
    std::optional<RaftLocalState> state;
    std::vector<LogEntry> appends;
    std::vector<std::pair<uint64_t, uint64_t>> cuts;  // [from, to)
  };

  /// per-group 路径布局（相对 root_）。
  struct GroupPaths {
    std::filesystem::path dir;
    std::filesystem::path state;
    std::filesystem::path state_tmp;
    std::filesystem::path log;
    std::filesystem::path log_tmp;

    static GroupPaths for_group(const std::filesystem::path& root, uint64_t group_id);
    bool ensure_dir() const;
  };

  GroupPaths paths_for(uint64_t group_id) const;

  void sync_file(const std::filesystem::path& path) const;
  void sync_root_dir() const;

  bool read_state_unlocked(const GroupPaths& p, RaftLocalState* out) const;
  bool write_state_unlocked(const GroupPaths& p, const RaftLocalState& st, bool sync) const;

  bool read_log_unlocked(const GroupPaths& p, std::vector<LogEntry>* out) const;
  bool rewrite_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents,
                            bool sync) const;
  /// 纯追加 RLE 记录；文件非 RLG2 或 IO 失败返回 false。
  bool append_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents,
                           bool sync) const;

  /// 应用单 group 突变。REQUIRES: mu_ held。失败返回 0。
  size_t apply_group_mut_unlocked(uint64_t group_id, GroupMut& mut, bool sync);

  static RaftLocalState merge_state(const GroupMut& mut, const std::vector<LogEntry>& ents,
                                    const RaftLocalState* existing);
  static size_t approx_entry_bytes(const LogEntry& e);

  std::string root_;
  SyncMode sync_{SyncMode::kData};
  mutable std::mutex mu_;
};

}  // namespace openkv::raft
