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

// @file raft_log.cpp
// RaftLog 实现：索引换算、Follower 合并逻辑、快照边界处理。

#include "opskv/raft/raft_log.hpp"

#include <stdexcept>

namespace opskv::raft {

RaftLog::RaftLog(uint64_t first_index) : first_index_(first_index) {}

uint64_t RaftLog::last_index() const {
  if (entries_.empty()) {
    // 空日志时“最后一条”为 first-1（与 Raft 论文一致）
    return first_index_ - 1;
  }
  return entries_.back().index;
}

uint64_t RaftLog::last_term() const {
  if (entries_.empty()) {
    return snapshot_term_;
  }
  return entries_.back().term;
}

std::optional<LogEntry> RaftLog::entry(uint64_t index) const {
  if (index < first_index_ || index > last_index()) {
    return std::nullopt;
  }
  return entries_[index - first_index_];
}

std::vector<LogEntry> RaftLog::entries_from(uint64_t index, size_t limit, size_t max_bytes) const {
  std::vector<LogEntry> out;
  if (index < first_index_) {
    index = first_index_;
  }
  size_t bytes = 0;
  for (uint64_t i = index; i <= last_index() && out.size() < limit; ++i) {
    const auto& e = entries_[i - first_index_];
    if (!out.empty() && bytes + e.data.size() > max_bytes) break;
    bytes += e.data.size();
    out.push_back(e);
  }
  return out;
}

uint64_t RaftLog::append(uint64_t term, std::string data) {
  LogEntry e;
  e.term = term;
  e.index = last_index() + 1;
  e.data = std::move(data);
  entries_.push_back(std::move(e));
  return entries_.back().index;
}

bool RaftLog::match(uint64_t index, uint64_t term) const {
  // 快照虚拟条目：index == first_index_ - 1
  if (index == first_index_ - 1) {
    return term == snapshot_term_;
  }
  auto e = entry(index);
  return e && e->term == term;
}

void RaftLog::truncate_from(uint64_t index) {
  if (index < first_index_) {
    entries_.clear();
    return;
  }
  if (index > last_index() + 1) {
    return;
  }
  entries_.resize(index - first_index_);
}

bool RaftLog::append_entries(uint64_t prev_index, uint64_t prev_term,
                             const std::vector<LogEntry>& entries) {
  if (!match(prev_index, prev_term)) {
    return false;
  }
  // 逐条对齐：term 不一致则从该 index 截断再写入 Leader 条目
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& e = entries[i];
    uint64_t idx = prev_index + 1 + i;
    auto existing = entry(idx);
    if (existing) {
      if (existing->term != e.term) {
        truncate_from(idx);
        entries_.push_back(e);
      }
    } else {
      entries_.push_back(e);
    }
  }
  return true;
}

void RaftLog::compact_to(uint64_t last_included_index, uint64_t last_included_term) {
  if (last_included_index < first_index_ - 1) {
    return;
  }
  if (last_included_index <= last_index()) {
    auto offset = last_included_index + 1 - first_index_;
    if (offset < entries_.size()) {
      entries_.erase(entries_.begin(), entries_.begin() + static_cast<long>(offset));
    } else {
      entries_.clear();
    }
  } else {
    entries_.clear();
  }
  first_index_ = last_included_index + 1;
  snapshot_term_ = last_included_term;
}

void RaftLog::load(uint64_t first_index, uint64_t snapshot_term, std::vector<LogEntry> entries) {
  first_index_ = first_index == 0 ? 1 : first_index;
  snapshot_term_ = snapshot_term;
  entries_ = std::move(entries);
}

}  // namespace opskv::raft
