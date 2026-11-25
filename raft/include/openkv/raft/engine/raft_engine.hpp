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
// 职责：Raft 持久化抽象——RaftLocalState + RaftLogBatch + RaftEngine::consume(sync)。
// 架构：写路径 append/cut/put_raft_state 进 Batch，一次 consume 原子落盘；读路径供恢复。
// 协作：RaftNode::engine_flush_locked / persist_ready 调用；实现有 FileRaftEngine、
//       DbRaftEngine；save_group_to_engine 用于整组迁移。
// 注意：SyncMode 定义在 sync_mode.hpp；kData=Raft sync、KV apply 异步。

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "openkv/raft/core/types.hpp"
#include "openkv/raft/engine/raft_wire.hpp"
#include "openkv/raft/engine/sync_mode.hpp"
#include "openkv/raft/log/raft_log.hpp"

namespace openkv::raft {

/* RaftLocalState：HardState + 日志窗口边界。 */
struct RaftLocalState {
  uint64_t term{0};           // 当前任期
  std::string voted_for;      // 本任期投票对象
  uint64_t commit_index{0};   // 已提交水位
  uint64_t last_applied{0};   // 已 apply 水位
  uint64_t first_index{1};    // 日志窗首 index
  uint64_t snapshot_term{0};  // 快照边界 term
  uint64_t last_index{0};     // 日志窗末 index（可与引擎内一致）

  /**
   * 转成 PersistedHardState（不含 last_index）。
   * @return 硬状态副本
   */
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

  /**
   * 从 PersistedHardState 构造；可选带上 last_index。
   * @param hs 硬状态
   * @param last_index 日志末下标；默认 0
   * @return RaftLocalState
   */
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

/* 一次原子写批次：append / cut / put_raft_state，由 consume 一次性提交。 */
class RaftLogBatch {
 public:
  virtual ~RaftLogBatch() = default;

  /**
   * 追加 entries；不更新 RaftLocalState（由 put_raft_state 负责）。
   * @param raft_group_id 组 id
   * @param entries 待追加日志
   */
  virtual void append(uint64_t raft_group_id, std::vector<LogEntry> entries) = 0;
  /**
   * 删除 [from, to) 内将被覆盖的旧日志。
   * @param raft_group_id 组 id
   * @param from 区间起（含）
   * @param to 区间止（不含）
   */
  virtual void cut_logs(uint64_t raft_group_id, uint64_t from, uint64_t to) = 0;
  /**
   * 写入/覆盖该 group 的 RaftLocalState。
   * @param raft_group_id 组 id
   * @param state 完整本地状态
   */
  virtual void put_raft_state(uint64_t raft_group_id, const RaftLocalState& state) = 0;
  /**
   * 批次是否无任何变更。
   * @return true 表示空
   */
  virtual bool empty() const = 0;
};

/* Multi-Raft 共享持久化引擎：consume 原子落盘，读路径供恢复。 */
class RaftEngine {
 public:
  virtual ~RaftEngine() = default;

  /**
   * 创建空批次。
   * @param capacity_hint 预留容量提示；默认 0
   * @return 新批次
   */
  virtual std::unique_ptr<RaftLogBatch> make_log_batch(size_t capacity_hint = 0) = 0;
  /**
   * 原子提交批次；sync=true 时 fsync/WriteOptions.sync。
   * @param batch 待提交批次
   * @param sync 是否耐久刷盘
   * @return 写入字节近似值；失败可为 0
   */
  virtual size_t consume(RaftLogBatch& batch, bool sync) = 0;

  /**
   * 读该 group 的 RaftLocalState。
   * @param raft_group_id 组 id
   * @return 状态或 nullopt
   */
  virtual std::optional<RaftLocalState> get_raft_state(uint64_t raft_group_id) const = 0;
  /**
   * 按 index 读单条日志。
   * @param raft_group_id 组 id
   * @param index 日志下标
   * @return 条目或 nullopt
   */
  virtual std::optional<LogEntry> get_entry(uint64_t raft_group_id, uint64_t index) const = 0;
  /**
   * 拉取 [begin, end) 到 to；受 max_size 限制。
   * @param raft_group_id 组 id
   * @param begin 起始 index（含）
   * @param end 结束 index（不含）
   * @param max_size 累计 data 字节上限
   * @param to 输出缓冲
   * @return 实际条数
   */
  virtual size_t fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end,
                                  size_t max_size, std::vector<LogEntry>* to) const = 0;

  /**
   * 删除 [from, to) 日志。
   * @param raft_group_id 组 id
   * @param from 区间起（含）
   * @param to 区间止（不含）
   * @return 删除条数
   */
  virtual size_t gc(uint64_t raft_group_id, uint64_t from, uint64_t to) = 0;
  /**
   * 销毁该 group 的 state + 全部日志。
   * @param raft_group_id 组 id
   */
  virtual void clean(uint64_t raft_group_id) = 0;

  /**
   * 当前耐久级别。
   * @return SyncMode
   */
  virtual SyncMode sync_mode() const = 0;
  /**
   * 运行时改耐久级别。
   * @param m 新级别
   */
  virtual void set_sync_mode(SyncMode m) = 0;
};

/**
 * 从引擎装载 group → PersistedHardState + entries（RaftNode 恢复用）。
 * @param eng 引擎
 * @param raft_group_id 组 id
 * @param hs 输出硬状态
 * @param entries 输出日志
 * @return 是否装载成功
 */
bool load_group_from_engine(const RaftEngine& eng, uint64_t raft_group_id, PersistedHardState* hs,
                            std::vector<LogEntry>* entries);

/**
 * 把当前内存 log + hs 整窗写入引擎（冷启动兼容 / 全量 flush）。
 * @param eng 引擎
 * @param raft_group_id 组 id
 * @param hs 硬状态
 * @param log 内存日志
 * @param sync 是否 fsync
 * @return 写入字节近似值
 */
size_t save_group_to_engine(RaftEngine& eng, uint64_t raft_group_id, const PersistedHardState& hs,
                            const RaftLog& log, bool sync);

/**
 * 编码 RaftLocalState 为二进制（File / Rocks 共用）。
 * @param st 本地状态
 * @return 字节串
 */
std::string encode_raft_local_state(const RaftLocalState& st);
/**
 * 解码 RaftLocalState。
 * @param raw 字节
 * @param st 输出
 * @return 是否成功
 */
bool decode_raft_local_state(std::string_view raw, RaftLocalState* st);
/**
 * 编码单条 LogEntry（含 CRC）。
 * @param e 日志条目
 * @return 字节串
 */
std::string encode_log_entry(const LogEntry& e);
/**
 * 解码单条 LogEntry。
 * @param raw 字节
 * @param e 输出
 * @return 是否成功
 */
bool decode_log_entry(std::string_view raw, LogEntry* e);

}  // namespace openkv::raft
