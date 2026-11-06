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

// @file raft_engine.cpp
// RaftEngine 辅助：编解码（含 CRC）、装载、全量 save。

#include "opskv/raft/raft_engine.hpp"

#include "opskv/raft/raft_log.hpp"

#include <cstdint>
#include <cstring>

namespace opskv::raft {
namespace {

void put_u64(std::string* out, uint64_t v) {
  char b[8];
  for (int i = 7; i >= 0; --i) {
    b[i] = static_cast<char>(v & 0xff);
    v >>= 8;
  }
  out->append(b, 8);
}

bool get_u64(const char*& p, const char* end, uint64_t* v) {
  if (!p || end - p < 8) return false;
  uint64_t x = 0;
  for (int i = 0; i < 8; ++i) x = (x << 8) | static_cast<uint8_t>(p[i]);
  p += 8;
  *v = x;
  return true;
}

void put_u32(std::string* out, uint32_t v) {
  char b[4];
  for (int i = 3; i >= 0; --i) {
    b[i] = static_cast<char>(v & 0xff);
    v >>= 8;
  }
  out->append(b, 4);
}

bool get_u32(const char*& p, const char* end, uint32_t* v) {
  if (!p || end - p < 4) return false;
  uint32_t x = 0;
  for (int i = 0; i < 4; ++i) x = (x << 8) | static_cast<uint8_t>(p[i]);
  p += 4;
  *v = x;
  return true;
}

// IEEE CRC-32（多项式 0xEDB88320）
uint32_t crc32(const uint8_t* data, size_t len) {
  uint32_t c = 0xffffffffu;
  for (size_t i = 0; i < len; ++i) {
    c ^= data[i];
    for (int k = 0; k < 8; ++k) {
      c = (c & 1) ? (0xedb88320u ^ (c >> 1)) : (c >> 1);
    }
  }
  return c ^ 0xffffffffu;
}

}  // namespace

std::string encode_raft_local_state(const RaftLocalState& st) {
  std::string out;
  out.append("RLS1", 4);
  put_u64(&out, st.term);
  put_u64(&out, static_cast<uint64_t>(st.voted_for.size()));
  out.append(st.voted_for);
  put_u64(&out, st.commit_index);
  put_u64(&out, st.last_applied);
  put_u64(&out, st.first_index);
  put_u64(&out, st.snapshot_term);
  put_u64(&out, st.last_index);
  return out;
}

bool decode_raft_local_state(std::string_view raw, RaftLocalState* st) {
  if (!st || raw.size() < 4 + 8 * 7) return false;
  if (raw.substr(0, 4) != "RLS1") return false;
  const char* p = raw.data() + 4;
  const char* end = raw.data() + raw.size();
  uint64_t vote_len = 0;
  if (!get_u64(p, end, &st->term) || !get_u64(p, end, &vote_len)) return false;
  if (end - p < static_cast<std::ptrdiff_t>(vote_len)) return false;
  st->voted_for.assign(p, vote_len);
  p += vote_len;
  return get_u64(p, end, &st->commit_index) && get_u64(p, end, &st->last_applied) &&
         get_u64(p, end, &st->first_index) && get_u64(p, end, &st->snapshot_term) &&
         get_u64(p, end, &st->last_index);
}

std::string encode_log_entry(const LogEntry& e) {
  // RLE2：term|index|len|data|crc32(payload without magic/crc)
  std::string body;
  put_u64(&body, e.term);
  put_u64(&body, e.index);
  put_u64(&body, static_cast<uint64_t>(e.data.size()));
  body.append(e.data);
  const uint32_t c = crc32(reinterpret_cast<const uint8_t*>(body.data()), body.size());
  std::string out;
  out.append("RLE2", 4);
  out.append(body);
  put_u32(&out, c);
  return out;
}

bool decode_log_entry(std::string_view raw, LogEntry* e) {
  if (!e || raw.size() < 4 + 24) return false;
  if (raw.substr(0, 4) == "RLE2") {
    if (raw.size() < 4 + 24 + 4) return false;
    const char* p = raw.data() + 4;
    const char* end = raw.data() + raw.size() - 4;  // exclude crc
    uint64_t dlen = 0;
    if (!get_u64(p, end, &e->term) || !get_u64(p, end, &e->index) || !get_u64(p, end, &dlen)) {
      return false;
    }
    if (end - p < static_cast<std::ptrdiff_t>(dlen)) return false;
    e->data.assign(p, dlen);
    const char* crc_p = raw.data() + raw.size() - 4;
    uint32_t expect = 0;
    if (!get_u32(crc_p, raw.data() + raw.size(), &expect)) return false;
    const size_t body_len = raw.size() - 4 - 4;
    const uint32_t got =
        crc32(reinterpret_cast<const uint8_t*>(raw.data() + 4), body_len);
    return got == expect;
  }
  // 兼容旧 RLE1（无 CRC）
  if (raw.substr(0, 4) != "RLE1") return false;
  const char* p = raw.data() + 4;
  const char* end = raw.data() + raw.size();
  uint64_t dlen = 0;
  if (!get_u64(p, end, &e->term) || !get_u64(p, end, &e->index) || !get_u64(p, end, &dlen)) {
    return false;
  }
  if (end - p < static_cast<std::ptrdiff_t>(dlen)) return false;
  e->data.assign(p, dlen);
  return true;
}

bool load_group_from_engine(const RaftEngine& eng, uint64_t raft_group_id, PersistedHardState* hs,
                            std::vector<LogEntry>* entries) {
  if (!hs || !entries) return false;
  auto st = eng.get_raft_state(raft_group_id);
  if (!st) return false;
  *hs = st->to_persisted();
  entries->clear();
  const uint64_t begin = hs->first_index;
  uint64_t end = st->last_index > 0 ? st->last_index + 1 : begin;
  if (end <= begin) {
    end = begin + 1024 * 1024;
  }
  eng.fetch_entries_to(raft_group_id, begin, end, SIZE_MAX, entries);
  if (!entries->empty() && st->last_index == 0) {
    hs->first_index = entries->front().index;
  }
  return true;
}

size_t save_group_to_engine(RaftEngine& eng, uint64_t raft_group_id, const PersistedHardState& hs,
                            const RaftLog& log, bool sync) {
  eng.clean(raft_group_id);
  auto batch = eng.make_log_batch();
  std::vector<LogEntry> ents;
  for (uint64_t i = log.first_index(); i <= log.last_index(); ++i) {
    if (auto e = log.entry(i)) ents.push_back(*e);
  }
  if (!ents.empty()) batch->append(raft_group_id, std::move(ents));
  auto st = RaftLocalState::from_persisted(hs, log.last_index());
  batch->put_raft_state(raft_group_id, st);
  return eng.consume(*batch, sync);
}

}  // namespace opskv::raft
