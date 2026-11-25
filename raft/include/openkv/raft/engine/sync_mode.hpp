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

// @file sync_mode.hpp
// 职责：Raft/KV 耐久级别（SyncMode）与必须落盘的协议元状态（PersistedHardState）。
// 架构：落盘一律走 RaftEngine；本头只放类型与解析，不含 I/O。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace openkv::raft {

/* 必须落盘的协议元状态（重启靠它恢复选举/提交水位）。 */
struct PersistedHardState {
  uint64_t term{0};           // 当前任期
  std::string voted_for;      // 本任期投票对象
  uint64_t commit_index{0};   // 已提交水位
  uint64_t last_applied{0};   // 已 apply 水位（重启后继续推）
  uint64_t first_index{1};    // 日志窗首 index（compact 后前移）
  uint64_t snapshot_term{0};  // first_index-1 处快照的 term
};

// 耐久级别。默认 kData = 耐久靠 Raft log；KV apply 异步。
enum class SyncMode {
  kNone = 0,  // Raft/KV 都不 sync（最快；崩溃可丢未 sync 的 Raft 尾部）
  kData = 1,  // Raft log fdatasync；业务 KV apply 异步（崩溃用日志重放补齐）
  kAll = 2,   // Raft + KV 都 sync（最稳，吞吐最低）
};

/**
 * Raft 日志是否要求 sync（none 除外）。
 * @param m 耐久级别
 * @return true 表示 consume 应 sync
 */
inline bool raft_log_sync(SyncMode m) { return m != SyncMode::kNone; }
/**
 * KV WriteOptions.sync：仅 kAll；kData 靠 Raft 耐久。
 * @param m 耐久级别
 * @return true 表示 KV apply 也要 sync
 */
inline bool kv_apply_sync(SyncMode m) { return m == SyncMode::kAll; }

/**
 * 把配置串解析成 SyncMode；无法识别则返回 def。
 * @param s none/data/all 或 0/1/2
 * @param def 解析失败时的默认值
 * @return 解析结果
 */
inline SyncMode parse_sync_mode(std::string_view s, SyncMode def = SyncMode::kData) {
  if (s == "none" || s == "None" || s == "0") {
    return SyncMode::kNone;
  }
  if (s == "data" || s == "Data" || s == "1") {
    return SyncMode::kData;
  }
  if (s == "all" || s == "All" || s == "2") {
    return SyncMode::kAll;
  }
  return def;
}

/**
 * SyncMode 的稳定小写名（日志/配置回显）。
 * @param m 耐久级别
 * @return "none" / "data" / "all"
 */
inline const char* sync_mode_name(SyncMode m) {
  switch (m) {
    case SyncMode::kNone:
      return "none";
    case SyncMode::kData:
      return "data";
    case SyncMode::kAll:
      return "all";
  }
  return "data";
}

}  // namespace openkv::raft
