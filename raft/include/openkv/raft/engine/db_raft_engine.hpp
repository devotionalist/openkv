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

// @file db_raft_engine.hpp
// DbRaftEngine：多 Group 共享嵌入式 KV WAL（当前后端 RocksDB；data_dir/raft），按 region 前缀存
// state/log。

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "openkv/raft/engine/raft_engine.hpp"

namespace openkv::raft {

class DbRaftEngine : public RaftEngine {
 public:
  explicit DbRaftEngine(std::string db_path, SyncMode sync = SyncMode::kData);
  ~DbRaftEngine() override;

  DbRaftEngine(const DbRaftEngine&) = delete;
  DbRaftEngine& operator=(const DbRaftEngine&) = delete;

  // 数据库路径
  const std::string& db_path() const { return path_; }

  std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) override;
  // 消费
  size_t consume(RaftLogBatch& batch, bool sync) override;

  // 获取raft状态
  std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const override;
  // 获取日志
  std::optional<LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const override;
  // 获取日志
  size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end, size_t max_size,
                          std::vector<LogEntry>* to) const override;
  // 垃圾回收
  size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) override;
  // 清理
  void clean(uint64_t raft_group_id) override;
  // 同步模式
  SyncMode sync_mode() const override { return sync_; }
  // 设置同步模式
  void set_sync_mode(SyncMode m) override;

 private:
  friend class DbLogBatch;

  struct DbHandle;

  static std::string state_key(uint64_t group_id);                // 状态键
  static std::string log_key(uint64_t group_id, uint64_t index);  // 日志键
  static std::string log_prefix(uint64_t group_id);               // 日志前缀

  std::string path_;                // 数据库路径
  SyncMode sync_{SyncMode::kData};  // 同步模式
  std::unique_ptr<DbHandle> db_;
  mutable std::mutex mu_;  // 保护 set_sync_mode 与 WriteOptions
};

}  // namespace openkv::raft
