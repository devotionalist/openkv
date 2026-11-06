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

#include "opskv/storage/mvcc.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <unordered_map>

namespace opskv::storage {
namespace {

void append_u64_be(std::string& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    out.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
  }
}

uint64_t read_u64_be(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  }
  return v;
}

void append_u32(std::string& out, uint32_t v) {
  out.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

bool read_u32(std::string_view s, size_t& off, uint32_t& v) {
  if (off + sizeof(uint32_t) > s.size()) return false;
  std::memcpy(&v, s.data() + off, sizeof(v));
  off += sizeof(v);
  return true;
}

bool read_u64(std::string_view s, size_t& off, uint64_t& v) {
  if (off + sizeof(uint64_t) > s.size()) return false;
  std::memcpy(&v, s.data() + off, sizeof(v));
  off += sizeof(v);
  return true;
}

bool read_i64(std::string_view s, size_t& off, int64_t& v) {
  if (off + sizeof(int64_t) > s.size()) return false;
  std::memcpy(&v, s.data() + off, sizeof(v));
  off += sizeof(v);
  return true;
}

}  // namespace

int64_t wall_ms_now() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string lock_key(std::string_view user) {
  std::string k;
  k.reserve(user.size() + 2);
  k.append(user);
  k.push_back('\0');
  k.push_back('L');
  return k;
}

std::string data_key(std::string_view user, uint64_t start_ts) {
  std::string k;
  k.reserve(user.size() + 2 + 8);
  k.append(user);
  k.push_back('\0');
  k.push_back('D');
  append_u64_be(k, start_ts);
  return k;
}

std::string write_key(std::string_view user, uint64_t commit_ts) {
  std::string k;
  k.reserve(user.size() + 2 + 8);
  k.append(user);
  k.push_back('\0');
  k.push_back('W');
  append_u64_be(k, ~commit_ts);
  return k;
}

std::string write_key_prefix(std::string_view user) {
  std::string k;
  k.reserve(user.size() + 2);
  k.append(user);
  k.push_back('\0');
  k.push_back('W');
  return k;
}

std::string encode_lock(const LockInfo& li) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&li.start_ts), sizeof(li.start_ts));
  out.push_back(static_cast<char>(li.op));
  append_u32(out, static_cast<uint32_t>(li.primary.size()));
  out.append(li.primary);
  append_u32(out, static_cast<uint32_t>(li.value.size()));
  out.append(li.value);
  out.append(reinterpret_cast<const char*>(&li.for_update_ts), sizeof(li.for_update_ts));
  out.append(reinterpret_cast<const char*>(&li.ttl_ms), sizeof(li.ttl_ms));
  out.append(reinterpret_cast<const char*>(&li.lock_wall_ms), sizeof(li.lock_wall_ms));
  return out;
}

bool decode_lock(std::string_view payload, LockInfo* out) {
  size_t off = 0;
  if (!read_u64(payload, off, out->start_ts) || off >= payload.size()) return false;
  out->op = static_cast<MvccOp>(static_cast<uint8_t>(payload[off++]));
  uint32_t plen = 0, vlen = 0;
  if (!read_u32(payload, off, plen) || off + plen > payload.size()) return false;
  out->primary.assign(payload.data() + off, plen);
  off += plen;
  if (!read_u32(payload, off, vlen) || off + vlen > payload.size()) return false;
  out->value.assign(payload.data() + off, vlen);
  off += vlen;
  out->for_update_ts = 0;
  out->ttl_ms = 5000;
  out->lock_wall_ms = 0;
  if (off + sizeof(uint64_t) <= payload.size()) {
    read_u64(payload, off, out->for_update_ts);
  }
  if (off + sizeof(uint64_t) <= payload.size()) {
    read_u64(payload, off, out->ttl_ms);
  }
  if (off + sizeof(int64_t) <= payload.size()) {
    read_i64(payload, off, out->lock_wall_ms);
  }
  return true;
}

std::string encode_write(const WriteInfo& wi) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&wi.start_ts), sizeof(wi.start_ts));
  out.push_back(static_cast<char>(wi.op));
  return out;
}

bool decode_write(std::string_view payload, WriteInfo* out) {
  size_t off = 0;
  if (!read_u64(payload, off, out->start_ts) || off >= payload.size()) return false;
  out->op = static_cast<MvccOp>(static_cast<uint8_t>(payload[off]));
  return true;
}

std::string user_key_of(std::string_view physical) {
  auto pos = physical.find('\0');
  if (pos == std::string_view::npos) return std::string(physical);
  return std::string(physical.substr(0, pos));
}

std::optional<LockInfo> MvccStore::get_lock(std::string_view user) const {
  auto rec = eng_.get(lock_key(user));
  if (!rec) return std::nullopt;
  LockInfo li;
  if (!decode_lock(rec->value, &li)) return std::nullopt;
  return li;
}

bool MvccStore::has_write_conflict(std::string_view user, uint64_t start_ts) const {
  const std::string pref = write_key_prefix(user);
  bool conflict = false;
  eng_.scan_prefix(pref, [&](std::string_view key, const KvRecord& rec) {
    if (key.size() < pref.size() + 8) return true;
    const uint64_t inv = read_u64_be(key.data() + pref.size());
    const uint64_t commit_ts = ~inv;
    if (commit_ts > start_ts) {
      conflict = true;
      return false;
    }
    (void)rec;
    return false;
  });
  return conflict;
}

std::optional<uint64_t> MvccStore::find_commit_ts(std::string_view user,
                                                   uint64_t start_ts) const {
  const std::string pref = write_key_prefix(user);
  std::optional<uint64_t> found;
  eng_.scan_prefix(pref, [&](std::string_view key, const KvRecord& rec) {
    if (key.size() < pref.size() + 8) return true;
    const uint64_t inv = read_u64_be(key.data() + pref.size());
    const uint64_t commit_ts = ~inv;
    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    if (wi.start_ts == start_ts) {
      found = commit_ts;
      return false;
    }
    return true;
  });
  return found;
}

CheckTxnStatusResult MvccStore::check_txn_status(std::string_view primary,
                                                 uint64_t lock_ts) const {
  CheckTxnStatusResult r;
  if (auto cts = find_commit_ts(primary, lock_ts)) {
    r.status = TxnStatus::kCommitted;
    r.commit_ts = *cts;
    return r;
  }
  auto lk = get_lock(primary);
  if (!lk || lk->start_ts != lock_ts) {
    r.status = TxnStatus::kRolledBack;
    return r;
  }
  r.status = TxnStatus::kLocked;
  r.lock_ttl_ms = lk->ttl_ms;
  if (lk->lock_wall_ms > 0 && lk->ttl_ms > 0) {
    const int64_t age = wall_ms_now() - lk->lock_wall_ms;
    if (age > static_cast<int64_t>(lk->ttl_ms)) {
      r.lock_expired = true;
    }
  }
  return r;
}

std::optional<std::string> MvccStore::txn_get(std::string_view user, uint64_t start_ts,
                                              bool* locked, LockInfo* blocking_lock) const {
  if (locked) *locked = false;
  if (auto lk = get_lock(user)) {
    // 悲观锁不挡同事务读；其它事务看到锁则阻塞
    if (lk->start_ts != start_ts) {
      if (locked) *locked = true;
      if (blocking_lock) *blocking_lock = *lk;
      return std::nullopt;
    }
  }
  const std::string pref = write_key_prefix(user);
  std::optional<WriteInfo> chosen;
  eng_.scan_prefix(pref, [&](std::string_view key, const KvRecord& rec) {
    if (key.size() < pref.size() + 8) return true;
    const uint64_t inv = read_u64_be(key.data() + pref.size());
    const uint64_t commit_ts = ~inv;
    if (commit_ts > start_ts) return true;
    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    chosen = wi;
    return false;
  });
  if (!chosen) return std::nullopt;
  if (chosen->op == MvccOp::kDelete) return std::nullopt;
  auto data = eng_.get(data_key(user, chosen->start_ts));
  if (!data) return std::nullopt;
  return data->value;
}

void MvccStore::put_lock(std::string_view user, const LockInfo& li, uint64_t rev) {
  eng_.put(lock_key(user), encode_lock(li), rev);
}

void MvccStore::put_data(std::string_view user, uint64_t start_ts, std::string_view value,
                         uint64_t rev) {
  eng_.put(data_key(user, start_ts), value, rev);
}

void MvccStore::put_write(std::string_view user, uint64_t commit_ts, const WriteInfo& wi,
                          uint64_t rev) {
  eng_.put(write_key(user, commit_ts), encode_write(wi), rev);
}

void MvccStore::clear_lock(std::string_view user, uint64_t rev) {
  eng_.del(lock_key(user), rev);
}

void MvccStore::clear_data(std::string_view user, uint64_t start_ts, uint64_t rev) {
  eng_.del(data_key(user, start_ts), rev);
}

void MvccStore::clear_write(std::string_view user, uint64_t commit_ts, uint64_t rev) {
  eng_.del(write_key(user, commit_ts), rev);
}

std::vector<std::string> MvccStore::list_expired_locks() const {
  std::vector<std::string> out;
  const int64_t now = wall_ms_now();
  eng_.for_each([&](std::string_view phys, const KvRecord& rec) {
    if (phys.size() < 2 || phys[phys.size() - 1] != 'L') return true;
    if (phys[phys.size() - 2] != '\0') return true;
    LockInfo li;
    if (!decode_lock(rec.value, &li)) return true;
    if (li.lock_wall_ms <= 0 || li.ttl_ms == 0) return true;
    if (now - li.lock_wall_ms > static_cast<int64_t>(li.ttl_ms)) {
      out.push_back(user_key_of(phys));
    }
    return true;
  });
  return out;
}

uint64_t MvccStore::min_lock_start_ts() const {
  uint64_t min_ts = std::numeric_limits<uint64_t>::max();
  eng_.for_each([&](std::string_view phys, const KvRecord& rec) {
    if (phys.size() < 2 || phys[phys.size() - 1] != 'L') return true;
    if (phys[phys.size() - 2] != '\0') return true;
    LockInfo li;
    if (!decode_lock(rec.value, &li)) return true;
    if (li.start_ts < min_ts) min_ts = li.start_ts;
    return true;
  });
  return min_ts;
}

size_t MvccStore::gc_below(uint64_t safe_ts) {
  if (safe_ts == 0) return 0;
  // user -> list of (commit_ts, start_ts, op)
  struct Ver {
    uint64_t commit_ts;
    uint64_t start_ts;
    MvccOp op;
  };
  std::unordered_map<std::string, std::vector<Ver>> by_user;
  eng_.for_each([&](std::string_view phys, const KvRecord& rec) {
    auto pos = phys.find('\0');
    if (pos == std::string_view::npos || pos + 1 >= phys.size()) return true;
    if (phys[pos + 1] != 'W') return true;
    if (phys.size() < pos + 2 + 8) return true;
    const uint64_t inv = read_u64_be(phys.data() + pos + 2);
    const uint64_t commit_ts = ~inv;
    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    by_user[std::string(phys.substr(0, pos))].push_back(Ver{commit_ts, wi.start_ts, wi.op});
    return true;
  });

  size_t removed = 0;
  for (auto& [user, vers] : by_user) {
    std::sort(vers.begin(), vers.end(),
              [](const Ver& a, const Ver& b) { return a.commit_ts > b.commit_ts; });
    // commit_ts >= safe_ts 全留；commit_ts < safe_ts 只留最新一版作 floor（供 start_ts>=safe 读）
    bool kept_floor = false;
    for (const auto& v : vers) {
      if (v.commit_ts >= safe_ts) continue;
      if (!kept_floor) {
        kept_floor = true;
        continue;
      }
      clear_write(user, v.commit_ts, 0);
      if (v.op == MvccOp::kPut) {
        clear_data(user, v.start_ts, 0);
      }
      ++removed;
    }
  }
  return removed;
}

}  // namespace opskv::storage
