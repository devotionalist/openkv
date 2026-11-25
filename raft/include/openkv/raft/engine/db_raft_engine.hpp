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

/* 嵌入式 KV 版 RaftEngine：多 Group 共享一个 DB，前缀隔离 state/log。 */
class DbRaftEngine : public RaftEngine {
 public:
  /**
   * 打开/创建 DB 路径下的 Raft 引擎。
   * @param db_path 数据库目录
   * @param sync 默认耐久级别
   */
  explicit DbRaftEngine(std::string db_path, SyncMode sync = SyncMode::kData);
  ~DbRaftEngine() override;

  DbRaftEngine(const DbRaftEngine&) = delete;
  DbRaftEngine& operator=(const DbRaftEngine&) = delete;

  /**
   * 数据库路径。
   * @return 路径引用
   */
  const std::string& db_path() const { return path_; }

  /**
   * 创建空批次。
   * @param capacity_hint 预留提示
   * @return 新批次
   */
  std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) override;
  /**
   * 把批次写入底层 KV（可 sync）。
   * @param batch 待提交
   * @param sync 是否 WriteOptions.sync
   * @return 写入字节近似
   */
  size_t consume(RaftLogBatch& batch, bool sync) override;

  /**
   * 读 group 的 RaftLocalState。
   * @param raft_group_id 组 id
   * @return 状态或 nullopt
   */
  std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const override;
  /**
   * 按 index 读日志。
   * @param raft_group_id 组 id
   * @param index 日志下标
   * @return 条目或 nullopt
   */
  std::optional<LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const override;
  /**
   * 拉取 [begin, end) 到 to。
   * @param raft_group_id 组 id
   * @param begin 起始 index（含）
   * @param end 结束 index（不含）
   * @param max_size 字节上限
   * @param to 输出缓冲
   * @return 条数
   */
  size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end, size_t max_size,
                          std::vector<LogEntry>* to) const override;
  /**
   * 删除 [from, to) 日志键。
   * @param raft_group_id 组 id
   * @param from 区间起（含）
   * @param to 区间止（不含）
   * @return 删除条数
   */
  size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) override;
  /**
   * 删除该 group 全部 state/log 键。
   * @param raft_group_id 组 id
   */
  void clean(uint64_t raft_group_id) override;
  /**
   * 当前耐久级别。
   * @return SyncMode
   */
  SyncMode sync_mode() const override { return sync_; }
  /**
   * 运行时改耐久级别（持 mu_）。
   * @param m 新级别
   */
  void set_sync_mode(SyncMode m) override;

 private:
  friend class DbLogBatch;

  struct DbHandle;  // 隐藏 RocksDB 句柄

  /**
   * state 键编码。
   * @param group_id 组 id
   * @return 键串
   */
  static std::string state_key(uint64_t group_id);
  /**
   * 单条日志键编码。
   * @param group_id 组 id
   * @param index 日志下标
   * @return 键串
   */
  static std::string log_key(uint64_t group_id, uint64_t index);
  /**
   * 该 group 日志键前缀（迭代/清理用）。
   * @param group_id 组 id
   * @return 前缀串
   */
  static std::string log_prefix(uint64_t group_id);

  std::string path_;                // 数据库路径
  SyncMode sync_{SyncMode::kData};  // 耐久级别
  std::unique_ptr<DbHandle> db_;    // 底层 DB
  mutable std::mutex mu_;           // 保护 set_sync_mode 与 WriteOptions
};

}  // namespace openkv::raft
