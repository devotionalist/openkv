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

// @file rocks_raft_engine.hpp
// RaftEngine：独立 RocksDB（data_dir/raft），按 region_id 前缀存 state/log。

#pragma once

#include "opskv/raft/raft_engine.hpp"

#include <memory>
#include <mutex>
#include <string>

namespace opskv::storage {

class RocksRaftEngine : public raft::RaftEngine {
 public:
  explicit RocksRaftEngine(std::string db_path, raft::SyncMode sync = raft::SyncMode::kData);
  ~RocksRaftEngine() override;

  RocksRaftEngine(const RocksRaftEngine&) = delete;
  RocksRaftEngine& operator=(const RocksRaftEngine&) = delete;

  const std::string& db_path() const { return path_; }

  std::unique_ptr<raft::RaftLogBatch> make_log_batch(size_t capacity_hint = 0) override;
  size_t consume(raft::RaftLogBatch& batch, bool sync) override;

  std::optional<raft::RaftLocalState> get_raft_state(uint64_t raft_group_id) const override;
  std::optional<raft::LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const override;
  size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end, size_t max_size,
                          std::vector<raft::LogEntry>* to) const override;

  size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) override;
  void clean(uint64_t raft_group_id) override;

  raft::SyncMode sync_mode() const override { return sync_; }
  void set_sync_mode(raft::SyncMode m) override;

 private:
  friend class RocksLogBatch;

  struct RocksHandle;

  static std::string state_key(uint64_t group_id);
  static std::string log_key(uint64_t group_id, uint64_t index);
  static std::string log_prefix(uint64_t group_id);

  std::string path_;
  raft::SyncMode sync_{raft::SyncMode::kData};
  std::unique_ptr<RocksHandle> rocks_;
  mutable std::mutex mu_;  // 保护 set_sync_mode 与 WriteOptions
};

}  // namespace opskv::storage
