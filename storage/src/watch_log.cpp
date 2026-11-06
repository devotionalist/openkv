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

// @file watch_log.cpp

#include "opskv/storage/watch_log.hpp"

#include <cstring>
#include <vector>

namespace opskv::storage {
namespace {

void append_u32(std::string& out, uint32_t v) {
  out.push_back(static_cast<char>(v & 0xff));
  out.push_back(static_cast<char>((v >> 8) & 0xff));
  out.push_back(static_cast<char>((v >> 16) & 0xff));
  out.push_back(static_cast<char>((v >> 24) & 0xff));
}

bool read_u32(std::string_view in, size_t* i, uint32_t* v) {
  if (*i + 4 > in.size()) return false;
  *v = static_cast<uint8_t>(in[*i]) | (static_cast<uint32_t>(static_cast<uint8_t>(in[*i + 1])) << 8) |
       (static_cast<uint32_t>(static_cast<uint8_t>(in[*i + 2])) << 16) |
       (static_cast<uint32_t>(static_cast<uint8_t>(in[*i + 3])) << 24);
  *i += 4;
  return true;
}

}  // namespace

std::string WatchLog::make_key(uint64_t revision) {
  std::string k(kWatchLogPrefix);
  // big-endian：字典序 = 时间序
  for (int shift = 56; shift >= 0; shift -= 8) {
    k.push_back(static_cast<char>((revision >> shift) & 0xff));
  }
  return k;
}

bool WatchLog::parse_key(std::string_view key, uint64_t* revision) {
  if (key.size() != kWatchLogPrefix.size() + 8) return false;
  if (key.compare(0, kWatchLogPrefix.size(), kWatchLogPrefix) != 0) return false;
  uint64_t rev = 0;
  for (size_t i = 0; i < 8; ++i) {
    rev = (rev << 8) | static_cast<uint8_t>(key[kWatchLogPrefix.size() + i]);
  }
  if (revision) *revision = rev;
  return true;
}

std::string WatchLog::encode_payload(bool deleted, std::string_view key, std::string_view value) {
  std::string out;
  out.reserve(1 + 8 + key.size() + value.size());
  out.push_back(deleted ? 1 : 0);
  append_u32(out, static_cast<uint32_t>(key.size()));
  append_u32(out, static_cast<uint32_t>(value.size()));
  out.append(key);
  out.append(value);
  return out;
}

bool WatchLog::decode_payload(std::string_view payload, bool* deleted, std::string* key,
                              std::string* value) {
  if (payload.empty()) return false;
  size_t i = 0;
  const bool del = payload[i++] != 0;
  uint32_t klen = 0, vlen = 0;
  if (!read_u32(payload, &i, &klen) || !read_u32(payload, &i, &vlen)) return false;
  if (i + klen + vlen > payload.size()) return false;
  if (deleted) *deleted = del;
  if (key) *key = std::string(payload.substr(i, klen));
  i += klen;
  if (value) *value = std::string(payload.substr(i, vlen));
  return true;
}

void WatchLog::append(KvEngine& eng, const WatchRecord& rec, size_t capacity) {
  if (capacity == 0) capacity = kDefaultCapacity;
  eng.put(make_key(rec.revision), encode_payload(rec.deleted, rec.key, rec.value), rec.revision);

  std::vector<std::string> keys;
  eng.scan_prefix(kWatchLogPrefix, [&](std::string_view key, const KvRecord&) {
    keys.emplace_back(key);
    return true;
  });
  if (keys.size() <= capacity) return;
  const size_t excess = keys.size() - capacity;
  for (size_t i = 0; i < excess; ++i) {
    eng.del(keys[i], 0);
  }
}

std::vector<WatchRecord> WatchLog::poll(const KvEngine& eng, uint32_t region_id, uint64_t since,
                                        size_t limit) {
  if (limit == 0) limit = 64;
  std::vector<WatchRecord> out;
  const std::string start = make_key(since + 1);
  eng.scan_prefix(kWatchLogPrefix, [&](std::string_view key, const KvRecord& rec) {
    if (key < start) return true;
    uint64_t rev = 0;
    if (!parse_key(key, &rev) || rev <= since) return true;
    bool deleted = false;
    std::string k, v;
    if (!decode_payload(rec.value, &deleted, &k, &v)) return true;
    out.push_back(WatchRecord{region_id, rev, deleted, std::move(k), std::move(v)});
    return out.size() < limit;
  });
  return out;
}

}  // namespace opskv::storage
