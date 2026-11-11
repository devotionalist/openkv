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

#include "openkv/storage/mvcc.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <unordered_map>

namespace openkv::storage {
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
  out.append(reinterpret_cast<const char*>(&li.user_expire_at_ms), sizeof(li.user_expire_at_ms));
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
  out->user_expire_at_ms = 0;
  if (off + sizeof(uint64_t) <= payload.size()) {
    read_u64(payload, off, out->for_update_ts);
  }
  if (off + sizeof(uint64_t) <= payload.size()) {
    read_u64(payload, off, out->ttl_ms);
  }
  if (off + sizeof(int64_t) <= payload.size()) {
    read_i64(payload, off, out->lock_wall_ms);
  }
  if (off + sizeof(int64_t) <= payload.size()) {
    read_i64(payload, off, out->user_expire_at_ms);
  }
  return true;
}

std::string encode_write(const WriteInfo& wi) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&wi.start_ts), sizeof(wi.start_ts));
  out.push_back(static_cast<char>(wi.op));
  out.append(reinterpret_cast<const char*>(&wi.expire_at_ms), sizeof(wi.expire_at_ms));
  return out;
}

bool decode_write(std::string_view payload, WriteInfo* out) {
  size_t off = 0;
  if (!read_u64(payload, off, out->start_ts) || off >= payload.size()) return false;
  out->op = static_cast<MvccOp>(static_cast<uint8_t>(payload[off++]));
  out->expire_at_ms = 0;
  if (off + sizeof(int64_t) <= payload.size()) {
    read_i64(payload, off, out->expire_at_ms);
  }
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

std::optional<uint64_t> MvccStore::latest_commit_ts(std::string_view user) const {
  const std::string pref = write_key_prefix(user);
  std::optional<uint64_t> latest;
  eng_.scan_prefix(pref, [&](std::string_view key, const KvRecord& rec) {
    if (key.size() < pref.size() + 8) return true;
    const uint64_t inv = read_u64_be(key.data() + pref.size());
    const uint64_t commit_ts = ~inv;
    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    latest = commit_ts;
    return false;  // write keys are sorted by ~commit_ts → first is newest
  });
  return latest;
}

std::optional<std::string> MvccStore::txn_get(std::string_view user, uint64_t start_ts,
                                              bool* locked, LockInfo* blocking_lock,
                                              uint64_t* out_commit_ts) const {
  if (locked) *locked = false;
  if (out_commit_ts) *out_commit_ts = 0;
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
  uint64_t chosen_ts = 0;
  eng_.scan_prefix(pref, [&](std::string_view key, const KvRecord& rec) {
    if (key.size() < pref.size() + 8) return true;
    const uint64_t inv = read_u64_be(key.data() + pref.size());
    const uint64_t commit_ts = ~inv;
    if (commit_ts > start_ts) return true;
    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    chosen = wi;
    chosen_ts = commit_ts;
    return false;
  });
  if (!chosen) return std::nullopt;
  if (chosen->op == MvccOp::kDelete) return std::nullopt;
  // TiKV 风格：过期对读不可见（不依赖 compaction 已跑完）
  if (chosen->expire_at_ms > 0 && wall_ms_now() >= chosen->expire_at_ms) {
    if (out_commit_ts) *out_commit_ts = 0;
    return std::nullopt;
  }
  if (out_commit_ts) *out_commit_ts = chosen_ts;
  auto data = eng_.get(data_key(user, chosen->start_ts), /*hide_expired=*/false);
  if (!data) return std::nullopt;
  if (kv_record_expired(*data)) {
    if (out_commit_ts) *out_commit_ts = 0;
    return std::nullopt;
  }
  return data->value;
}

std::optional<int64_t> MvccStore::txn_ttl(std::string_view user, uint64_t start_ts, bool* locked,
                                          LockInfo* blocking_lock) const {
  if (locked) *locked = false;
  if (auto lk = get_lock(user)) {
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
  if (!chosen || chosen->op == MvccOp::kDelete) return int64_t{-2};
  const int64_t now = wall_ms_now();
  if (chosen->expire_at_ms > 0 && now >= chosen->expire_at_ms) return int64_t{-2};
  auto data = eng_.get(data_key(user, chosen->start_ts), /*hide_expired=*/false);
  if (!data || kv_record_expired(*data, now)) return int64_t{-2};
  if (chosen->expire_at_ms <= 0) return int64_t{-1};
  return chosen->expire_at_ms - now;
}

MvccStore::ScanResult MvccStore::txn_scan(std::string_view start_key, std::string_view end_key,
                                          std::string_view start_after, uint64_t start_ts,
                                          uint32_t limit, bool keys_only,
                                          const openkv::KeyRange& region_range) const {
  ScanResult out;
  if (limit == 0) limit = 256;
  const int64_t now = wall_ms_now();

  // 按物理序扫 Write 键；同一 user 的 W 键按 ~commit_ts 升序 → 先遇到的是最新提交。
  std::string cur_user;
  bool decided = false;  // 已为 cur_user 选定可见 Write（或判定不可见）

  auto emit_or_skip = [&](std::string_view user, const WriteInfo& wi, uint64_t commit_ts) -> bool {
    if (wi.op == MvccOp::kDelete) return true;
    if (wi.expire_at_ms > 0 && now >= wi.expire_at_ms) return true;
    ScanPair p;
    p.key = std::string(user);
    p.revision = commit_ts;
    if (!keys_only) {
      auto data = eng_.get(data_key(user, wi.start_ts), /*hide_expired=*/false);
      if (!data || kv_record_expired(*data, now)) return true;
      p.value = std::move(data->value);
    } else {
      // keys_only：仍校验 data 是否过期（与 Get 一致）
      auto data = eng_.get(data_key(user, wi.start_ts), /*hide_expired=*/false);
      if (!data || kv_record_expired(*data, now)) return true;
    }
    out.pairs.push_back(std::move(p));
    if (out.pairs.size() >= limit) {
      out.truncated = true;
      return false;
    }
    return true;
  };

  eng_.for_each([&](std::string_view phys, const KvRecord& rec) {
    auto pos = phys.find('\0');
    if (pos == std::string_view::npos || pos + 1 >= phys.size()) return true;
    if (phys[pos + 1] != 'W') return true;
    if (phys.size() < pos + 2 + 8) return true;

    std::string_view user = phys.substr(0, pos);
    if (!region_range.contains(user)) return true;
    if (!start_key.empty() && user < start_key) return true;
    if (!end_key.empty() && user >= end_key) return false;  // 有序，可停
    if (!start_after.empty() && user <= start_after) return true;

    if (user != cur_user) {
      cur_user = std::string(user);
      decided = false;
    }
    if (decided) return true;

    const uint64_t inv = read_u64_be(phys.data() + pos + 2);
    const uint64_t commit_ts = ~inv;
    if (commit_ts > start_ts) return true;  // 更新版本对当前快照不可见，继续找更老

    WriteInfo wi;
    if (!decode_write(rec.value, &wi)) return true;
    decided = true;
    if (!emit_or_skip(user, wi, commit_ts)) {
      // 已满：next_key = 当前 user（续扫 start_after=当前）
      out.next_key = std::string(user);
      return false;
    }
    return true;
  });

  return out;
}

void MvccStore::put_lock(std::string_view user, const LockInfo& li, uint64_t rev) {
  eng_.put(lock_key(user), encode_lock(li), rev);
}

void MvccStore::put_data(std::string_view user, uint64_t start_ts, std::string_view value,
                         uint64_t rev, int64_t expire_at_ms) {
  eng_.put(data_key(user, start_ts), value, rev, expire_at_ms);
}

void MvccStore::put_write(std::string_view user, uint64_t commit_ts, const WriteInfo& wi,
                          uint64_t rev) {
  eng_.put(write_key(user, commit_ts), encode_write(wi), rev, wi.expire_at_ms);
}

bool MvccStore::put_onepc(std::string_view user, uint64_t start_ts, uint64_t commit_ts, MvccOp op,
                          std::string_view value, uint64_t rev, int64_t expire_at_ms) {
  WriteInfo wi;
  wi.start_ts = start_ts;
  wi.op = op;
  wi.expire_at_ms = expire_at_ms;
  std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>> puts;
  if (op != MvccOp::kDelete) {
    puts.emplace_back(data_key(user, start_ts), std::string(value), rev, expire_at_ms);
  }
  puts.emplace_back(write_key(user, commit_ts), encode_write(wi), rev, expire_at_ms);
  return eng_.write_batch(puts, {});
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

}  // namespace openkv::storage
