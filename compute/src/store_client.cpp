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

// @file store_client.cpp
// Storage Admin ExecLine 客户端（对齐 Go storagetcp）。

#include "openkv/compute/store_client.hpp"

#include "openkv/codec.hpp"
#include "openkv/compute/exec_line.hpp"

#include <sstream>

namespace openkv::compute {
namespace {

std::vector<std::string> split_ws(std::string_view s) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == ' ' || c == '\t') {
      if (!cur.empty()) {
        out.push_back(cur);
        cur.clear();
      }
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty()) out.push_back(cur);
  return out;
}

}  // namespace

LockedError parse_locked_error(const std::string& resp) {
  std::string primary;
  uint64_t lock_ts = 0;
  for (const auto& f : split_ws(resp)) {
    if (f.rfind("primary=", 0) == 0) {
      from_hex(f.substr(8), &primary);
    } else if (f.rfind("lock_ts=", 0) == 0) {
      lock_ts = std::stoull(f.substr(8));
    }
  }
  return LockedError(std::move(primary), lock_ts, resp);
}

StoreClient::StoreClient(std::string addr, int timeout_ms)
    : addr_(std::move(addr)), timeout_ms_(timeout_ms) {}

std::string StoreClient::call(const std::string& line) {
  return store_exec_line(addr_, line, timeout_ms_);
}

void StoreClient::throw_txn_err(const std::string& resp, const char* op) {
  auto parts = split_ws(resp);
  if (!parts.empty() && parts[0] == "ERR") {
    if (resp.find("not_leader") != std::string::npos) {
      throw NotLeaderError(parse_redirect(resp), resp);
    }
  }
  throw std::runtime_error(std::string(op) + ": " + resp);
}

void StoreClient::ping() {
  // AdminService.Ping：不经 ExecLine/WorkPool，避免池堵时就绪探测假死。
  store_admin_ping(addr_, timeout_ms_);
}

std::string StoreClient::call_raw(const std::string& line) { return call(line); }

TxnGetResult StoreClient::txn_get_opt(uint32_t shard, std::string_view key, uint64_t start_ts,
                                      bool linearizable) {
  const int lin = linearizable ? 1 : 0;
  std::ostringstream oss;
  oss << "TXN_GET " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << lin;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (!parts.empty() && parts[0] == "ERR") {
    if (resp.find("locked") != std::string::npos) throw parse_locked_error(resp);
    throw_txn_err(resp, "txn_get");
  }
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "TXN_GET") {
    throw std::runtime_error("txn_get: " + resp);
  }
  TxnGetResult out;
  if (parts.size() >= 4) out.revision = std::stoull(parts[3]);
  if (parts[2] == "-") return out;
  if (!from_hex(parts[2], &out.value)) throw std::runtime_error("txn_get: bad hex");
  out.found = true;
  return out;
}

int64_t StoreClient::txn_ttl_opt(uint32_t shard, std::string_view key, uint64_t start_ts,
                                 bool linearizable) {
  const int lin = linearizable ? 1 : 0;
  std::ostringstream oss;
  oss << "TXN_TTL " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << lin;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (!parts.empty() && parts[0] == "ERR") {
    if (resp.find("locked") != std::string::npos) throw parse_locked_error(resp);
    throw_txn_err(resp, "txn_ttl");
  }
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "TXN_TTL") {
    throw std::runtime_error("txn_ttl: " + resp);
  }
  return std::stoll(parts[2]);
}

uint64_t StoreClient::prewrite_cas(uint32_t shard, std::string_view key, uint64_t start_ts,
                                   std::string_view primary, uint8_t op, std::string_view value,
                                   uint64_t epoch, bool cas, uint64_t expected_rev,
                                   int64_t expire_at_ms) {
  std::string val_hex = to_hex(value);
  if (val_hex.empty()) val_hex = "-";
  std::ostringstream oss;
  oss << "PREWRITE " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << to_hex(primary)
      << ' ' << static_cast<unsigned>(op) << ' ' << val_hex;
  if (cas) {
    oss << ' ' << epoch << " 1 " << expected_rev;
    if (expire_at_ms != 0) oss << ' ' << expire_at_ms;
  } else if (expire_at_ms != 0) {
    oss << ' ' << epoch << ' ' << expire_at_ms;
  } else if (epoch != 0) {
    oss << ' ' << epoch;
  }
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "PREWRITE") {
    throw_txn_err(resp, "prewrite");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::one_pc(uint32_t shard, std::string_view key, uint64_t start_ts,
                             uint64_t commit_ts, std::string_view primary, uint8_t op,
                             std::string_view value, uint64_t epoch, bool cas, uint64_t expected_rev,
                             int64_t expire_at_ms) {
  std::string val_hex = to_hex(value);
  if (val_hex.empty()) val_hex = "-";
  std::ostringstream oss;
  oss << "ONEPC " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << commit_ts << ' '
      << to_hex(primary) << ' ' << static_cast<unsigned>(op) << ' ' << val_hex;
  if (cas) {
    oss << ' ' << epoch << " 1 " << expected_rev;
    if (expire_at_ms != 0) oss << ' ' << expire_at_ms;
  } else if (expire_at_ms != 0) {
    oss << ' ' << epoch << ' ' << expire_at_ms;
  } else if (epoch != 0) {
    oss << ' ' << epoch;
  }
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (!parts.empty() && parts[0] == "ERR") {
    if (resp.find("locked") != std::string::npos) throw parse_locked_error(resp);
    throw_txn_err(resp, "onepc");
  }
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "ONEPC") {
    throw_txn_err(resp, "onepc");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::commit_key(uint32_t shard, std::string_view key, uint64_t start_ts,
                                 uint64_t commit_ts, uint64_t epoch) {
  std::ostringstream oss;
  oss << "COMMIT " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << commit_ts;
  if (epoch != 0) oss << ' ' << epoch;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "COMMIT") {
    throw_txn_err(resp, "commit");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::rollback_key(uint32_t shard, std::string_view key, uint64_t start_ts,
                                   uint64_t epoch) {
  std::ostringstream oss;
  oss << "ROLLBACK " << shard << ' ' << to_hex(key) << ' ' << start_ts;
  if (epoch != 0) oss << ' ' << epoch;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "ROLLBACK") {
    throw_txn_err(resp, "rollback");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::pessimistic_lock(uint32_t shard, std::string_view key, uint64_t start_ts,
                                       uint64_t for_update_ts, std::string_view primary,
                                       uint64_t ttl_ms, uint64_t epoch) {
  std::ostringstream oss;
  oss << "PESSIMISTIC_LOCK " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' '
      << for_update_ts << ' ' << to_hex(primary) << ' ' << ttl_ms;
  if (epoch != 0) oss << ' ' << epoch;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "PESSIMISTIC_LOCK") {
    throw_txn_err(resp, "pessimistic_lock");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::pessimistic_rollback(uint32_t shard, std::string_view key, uint64_t start_ts,
                                           uint64_t epoch) {
  std::ostringstream oss;
  oss << "PESSIMISTIC_ROLLBACK " << shard << ' ' << to_hex(key) << ' ' << start_ts;
  if (epoch != 0) oss << ' ' << epoch;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "PESSIMISTIC_ROLLBACK") {
    throw_txn_err(resp, "pessimistic_rollback");
  }
  return std::stoull(parts[2]);
}

uint64_t StoreClient::resolve_lock(uint32_t shard, std::string_view key, uint64_t start_ts,
                                   uint64_t commit_ts, uint64_t epoch) {
  std::ostringstream oss;
  oss << "RESOLVE_LOCK " << shard << ' ' << to_hex(key) << ' ' << start_ts << ' ' << commit_ts;
  if (epoch != 0) oss << ' ' << epoch;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "RESOLVE_LOCK") {
    throw_txn_err(resp, "resolve_lock");
  }
  return std::stoull(parts[2]);
}

std::pair<std::string, uint64_t> StoreClient::check_txn_status(uint32_t shard,
                                                              std::string_view primary,
                                                              uint64_t lock_ts) {
  std::ostringstream oss;
  oss << "CHECK_TXN_STATUS " << shard << ' ' << to_hex(primary) << ' ' << lock_ts;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 4 || parts[0] != "OK" || parts[1] != "CHECK_TXN_STATUS") {
    throw_txn_err(resp, "check_txn_status");
  }
  uint64_t commit_ts = 0;
  try {
    commit_ts = std::stoull(parts[3]);
  } catch (...) {
  }
  return {parts[2], commit_ts};
}

TxnScanResult StoreClient::txn_scan_opt(uint32_t shard, std::string_view start, std::string_view end,
                                        std::string_view start_after, uint64_t start_ts,
                                        uint32_t limit, bool keys_only, bool linearizable) {
  const std::string start_hex = start.empty() ? "-" : to_hex(start);
  const std::string end_hex = end.empty() ? "-" : to_hex(end);
  const int lin = linearizable ? 1 : 0;
  std::ostringstream oss;
  oss << "TXN_SCAN " << shard << ' ' << start_hex << ' ' << end_hex << ' ' << start_ts << ' '
      << limit;
  if (!start_after.empty()) oss << " after=" << to_hex(start_after);
  if (keys_only) oss << " keys_only";
  oss << ' ' << lin;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (!parts.empty() && parts[0] == "ERR") throw_txn_err(resp, "txn_scan");
  if (parts.size() < 5 || parts[0] != "OK" || parts[1] != "TXN_SCAN") {
    throw std::runtime_error("txn_scan: " + resp);
  }
  TxnScanResult out;
  out.truncated = parts[3] == "1";
  const int n = std::stoi(parts[4]);
  size_t idx = 5;
  for (int i = 0; i < n && idx < parts.size(); ++i) {
    const auto& tok = parts[idx++];
    if (tok.rfind("next=", 0) == 0) {
      --idx;
      break;
    }
    // k:v:rev
    auto c1 = tok.find(':');
    auto c2 = c1 == std::string::npos ? std::string::npos : tok.find(':', c1 + 1);
    if (c1 == std::string::npos || c2 == std::string::npos) {
      throw std::runtime_error("txn_scan pair: " + tok);
    }
    ScanPair p;
    if (!from_hex(tok.substr(0, c1), &p.key)) throw std::runtime_error("txn_scan bad key");
    auto vhex = tok.substr(c1 + 1, c2 - c1 - 1);
    if (vhex != "-") {
      if (!from_hex(vhex, &p.value)) throw std::runtime_error("txn_scan bad value");
    }
    p.revision = std::stoull(tok.substr(c2 + 1));
    out.pairs.push_back(std::move(p));
  }
  for (; idx < parts.size(); ++idx) {
    if (parts[idx].rfind("next=", 0) == 0) {
      from_hex(parts[idx].substr(5), &out.next_key);
    }
  }
  return out;
}

uint64_t StoreClient::watch_head(uint32_t shard) {
  auto resp = call("WATCH_HEAD " + std::to_string(shard));
  auto parts = split_ws(resp);
  if (parts.size() != 3 || parts[0] != "OK" || parts[1] != "WATCH_HEAD") {
    throw std::runtime_error("watch_head: " + resp);
  }
  return std::stoull(parts[2]);
}

std::pair<std::vector<WatchEvent>, uint64_t> StoreClient::watch_poll(uint32_t shard, uint64_t since,
                                                                     int limit) {
  if (limit <= 0) limit = 64;
  std::ostringstream oss;
  oss << "WATCH_POLL " << shard << ' ' << since << ' ' << limit;
  auto resp = call(oss.str());
  auto parts = split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK" || parts[1] != "WATCH") {
    throw std::runtime_error("watch_poll: " + resp);
  }
  const int n = std::stoi(parts[2]);
  if (parts.size() < 4) return {{}, since};
  uint64_t max_rev = 0;
  try {
    max_rev = std::stoull(parts.back());
  } catch (...) {
    throw std::runtime_error("watch_poll: " + resp);
  }
  std::vector<WatchEvent> events;
  // OK WATCH n [events...] max_rev
  for (int i = 0; i < n && static_cast<size_t>(3 + i) < parts.size() - 1; ++i) {
    const auto& tok = parts[3 + i];
    // key:rev:D|P:val
    size_t p0 = tok.find(':');
    size_t p1 = p0 == std::string::npos ? std::string::npos : tok.find(':', p0 + 1);
    size_t p2 = p1 == std::string::npos ? std::string::npos : tok.find(':', p1 + 1);
    if (p0 == std::string::npos || p1 == std::string::npos || p2 == std::string::npos) continue;
    WatchEvent ev;
    from_hex(tok.substr(0, p0), &ev.key);
    ev.revision = std::stoull(tok.substr(p0 + 1, p1 - p0 - 1));
    ev.deleted = tok.substr(p1 + 1, p2 - p1 - 1) == "D";
    auto val = tok.substr(p2 + 1);
    if (!ev.deleted && val != "-") from_hex(val, &ev.value);
    events.push_back(std::move(ev));
  }
  return {std::move(events), max_rev};
}

}  // namespace openkv::compute
