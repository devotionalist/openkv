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

// @file raft_log_file.cpp
// 实现：RLG 容器编解码与内存日志突变合并。

#include "openkv/raft/raft_log_file.hpp"

#include "openkv/raft/raft_engine.hpp"

#include <algorithm>
#include <cstdint>

namespace openkv::raft::log_file {
namespace {

uint64_t read_be64_at(std::string_view raw, size_t off) {
  uint64_t v = 0;
  for (int j = 0; j < 8; ++j) {
    v = (v << 8) | static_cast<uint8_t>(raw[off + static_cast<size_t>(j)]);
  }
  return v;
}

}  // namespace

size_t entry_record_size(std::string_view raw, size_t offset) {
  if (offset + 4 + 24 > raw.size()) return 0;
  const auto mag = raw.substr(offset, 4);
  if (mag == "RLE2") {
    if (offset + 4 + 24 + 4 > raw.size()) return 0;
    const uint64_t dlen = read_be64_at(raw, offset + 4 + 16);
    return 4 + 24 + static_cast<size_t>(dlen) + 4;
  }
  if (mag == "RLE1") {
    const uint64_t dlen = read_be64_at(raw, offset + 4 + 16);
    return 4 + 24 + static_cast<size_t>(dlen);
  }
  return 0;
}

std::string encode_container(const std::vector<LogEntry>& entries) {
  std::string out;
  out.reserve(4 + entries.size() * 48);
  out.append(kMagicRlg2.data(), kMagicRlg2.size());
  for (const auto& e : entries) {
    out.append(encode_log_entry(e));
  }
  return out;
}

bool decode_container(std::string_view raw, std::vector<LogEntry>* out) {
  if (!out) return false;
  out->clear();
  if (raw.empty()) return true;
  if (raw.size() < 4) return false;

  if (raw.substr(0, 4) == kMagicRlg2) {
    size_t off = 4;
    while (off < raw.size()) {
      const size_t need = entry_record_size(raw, off);
      if (need == 0 || off + need > raw.size()) return false;
      LogEntry e;
      if (!decode_log_entry(raw.substr(off, need), &e)) return false;
      out->push_back(std::move(e));
      off += need;
    }
    return true;
  }

  // RLG1：magic + be64 count + records（迁移兼容）
  if (raw.substr(0, 4) != kMagicRlg1 || raw.size() < 12) return false;
  const uint64_t n = read_be64_at(raw, 4);
  size_t off = 12;
  out->reserve(static_cast<size_t>(n));
  for (uint64_t i = 0; i < n; ++i) {
    const size_t need = entry_record_size(raw, off);
    if (need == 0 || off + need > raw.size()) return false;
    LogEntry e;
    if (!decode_log_entry(raw.substr(off, need), &e)) return false;
    out->push_back(std::move(e));
    off += need;
  }
  return true;
}

bool can_pure_append(const std::vector<LogEntry>& existing, const std::vector<LogEntry>& appends,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts) {
  if (!cuts.empty() || appends.empty()) return false;
  uint64_t expect = existing.empty() ? appends.front().index : existing.back().index + 1;
  for (const auto& e : appends) {
    if (e.index != expect) return false;
    ++expect;
  }
  return true;
}

void apply_mutations(std::vector<LogEntry>* entries,
                     const std::vector<std::pair<uint64_t, uint64_t>>& cuts,
                     std::vector<LogEntry> appends) {
  if (!entries) return;
  for (const auto& [from, to] : cuts) {
    entries->erase(std::remove_if(entries->begin(), entries->end(),
                                  [&](const LogEntry& e) { return e.index >= from && e.index < to; }),
                   entries->end());
  }
  for (auto& e : appends) {
    entries->erase(
        std::remove_if(entries->begin(), entries->end(),
                       [&](const LogEntry& x) { return x.index == e.index; }),
        entries->end());
    entries->push_back(std::move(e));
  }
  std::sort(entries->begin(), entries->end(),
            [](const LogEntry& a, const LogEntry& b) { return a.index < b.index; });
}

}  // namespace openkv::raft::log_file
