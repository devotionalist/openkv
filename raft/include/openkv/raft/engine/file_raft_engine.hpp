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
// 架构：Meta / 单测 / 无 RocksDB 场景；生产 Storage 使用 raft::DbRaftEngine。
// 协作：实现 RaftEngine；容器格式见 raft_log_file；布局见 raft/ARCHITECTURE.md §5。
// 注意：consume 在单锁内提交；IO 失败返回 0 且打日志，调用方不得推进持久化游标。

#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "openkv/raft/engine/raft_engine.hpp"

namespace openkv::raft {

/* 文件版 RaftEngine：root 下每 group 一个子目录（state + log）。 */
class FileRaftEngine final : public RaftEngine {
 public:
  /**
   * root_dir 为空则所有写操作 no-op（disabled）。
   * @param root_dir 根目录；空=禁用
   * @param sync 默认耐久级别
   */
  explicit FileRaftEngine(std::string root_dir, SyncMode sync = SyncMode::kData);

  /**
   * 根目录路径。
   * @return 路径引用
   */
  const std::string& root_dir() const { return root_; }
  /**
   * 是否启用写盘（root 非空）。
   * @return true 表示会真正落文件
   */
  bool enabled() const { return !root_.empty(); }

  /**
   * 创建空批次。
   * @param capacity_hint 预留提示
   * @return 新批次
   */
  std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) override;
  /**
   * 原子提交批次到文件。
   * @param batch 待提交
   * @param sync 是否 fsync
   * @return 写入字节近似；失败 0
   */
  size_t consume(RaftLogBatch& batch, bool sync) override;

  /**
   * 读 group 状态文件。
   * @param raft_group_id 组 id
   * @return 状态或 nullopt
   */
  std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const override;
  /**
   * 按 index 读日志条目。
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
   * 删除 [from, to) 日志。
   * @param raft_group_id 组 id
   * @param from 区间起（含）
   * @param to 区间止（不含）
   * @return 删除条数
   */
  size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) override;
  /**
   * 删掉该 group 目录下 state/log。
   * @param raft_group_id 组 id
   */
  void clean(uint64_t raft_group_id) override;

  /**
   * 当前耐久级别。
   * @return SyncMode
   */
  SyncMode sync_mode() const override { return sync_; }
  /**
   * 运行时改耐久级别。
   * @param m 新级别
   */
  void set_sync_mode(SyncMode m) override { sync_ = m; }

 private:
  friend class FileLogBatch;

  /* 单 group 在一次 LogBatch 内的累积变更。 */
  struct GroupMut {
    std::optional<RaftLocalState> state;              // 待写状态；空=不改
    std::vector<LogEntry> appends;                    // 待追加
    std::vector<std::pair<uint64_t, uint64_t>> cuts;  // [from, to) 切割
  };

  /* per-group 路径布局（相对 root_）。 */
  struct GroupPaths {
    std::filesystem::path dir;        // group 目录
    std::filesystem::path state;      // 状态文件
    std::filesystem::path state_tmp;  // 状态临时文件
    std::filesystem::path log;        // 日志文件
    std::filesystem::path log_tmp;    // 日志临时文件

    /**
     * 按 root + group_id 拼路径。
     * @param root 引擎根
     * @param group_id 组 id
     * @return 路径集合
     */
    static GroupPaths for_group(const std::filesystem::path& root, uint64_t group_id);
    /**
     * 确保 group 目录存在。
     * @return 是否成功
     */
    bool ensure_dir() const;
  };

  /**
   * 取 group 路径布局。
   * @param group_id 组 id
   * @return GroupPaths
   */
  GroupPaths paths_for(uint64_t group_id) const;

  /**
   * 对单个文件 fsync。
   * @param path 文件路径
   */
  void sync_file(const std::filesystem::path& path) const;
  /**
   * fsync 根目录（rename 耐久）。
   */
  void sync_root_dir() const;

  /**
   * 读状态文件（调用方已持锁或只读安全）。
   * @param p 路径
   * @param out 输出
   * @return 是否成功
   */
  bool read_state_unlocked(const GroupPaths& p, RaftLocalState* out) const;
  /**
   * 写状态文件（tmp + rename）。
   * @param p 路径
   * @param st 状态
   * @param sync 是否 fsync
   * @return 是否成功
   */
  bool write_state_unlocked(const GroupPaths& p, const RaftLocalState& st, bool sync) const;

  /**
   * 解码整份日志文件。
   * @param p 路径
   * @param out 输出条目
   * @return 是否成功
   */
  bool read_log_unlocked(const GroupPaths& p, std::vector<LogEntry>* out) const;
  /**
   * 整文件 rewrite 日志。
   * @param p 路径
   * @param ents 全量条目
   * @param sync 是否 fsync
   * @return 是否成功
   */
  bool rewrite_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents,
                            bool sync) const;
  /**
   * 纯追加 RLE 记录；文件非 RLG 或 IO 失败返回 false。
   * @param p 路径
   * @param ents 追加条目
   * @param sync 是否 fsync
   * @return 是否成功
   */
  bool append_log_unlocked(const GroupPaths& p, const std::vector<LogEntry>& ents, bool sync) const;

  /**
   * 应用单 group 突变。REQUIRES: mu_ held。失败返回 0。
   * @param group_id 组 id
   * @param mut 累积变更
   * @param sync 是否 fsync
   * @return 写入字节近似
   */
  size_t apply_group_mut_unlocked(uint64_t group_id, GroupMut& mut, bool sync);

  /**
   * 合并 mut 与现有条目得到新状态。
   * @param mut 变更
   * @param ents 合并后日志
   * @param existing 磁盘上旧状态；可空
   * @return 新 RaftLocalState
   */
  static RaftLocalState merge_state(const GroupMut& mut, const std::vector<LogEntry>& ents,
                                    const RaftLocalState* existing);
  /**
   * 估算单条日志占用字节。
   * @param e 条目
   * @return 近似字节
   */
  static size_t approx_entry_bytes(const LogEntry& e);

  std::string root_;                // 根目录；空=禁用
  SyncMode sync_{SyncMode::kData};  // 耐久级别
  mutable std::mutex mu_;           // 保护所有文件 IO
};

}  // namespace openkv::raft
