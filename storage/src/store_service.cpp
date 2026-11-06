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

// @file store_service.cpp
// Command 二进制编解码与 StoreFacade 读写编排（含 propose 后轮询 apply 的 demo 同步）。

#include "opskv/storage/store_service.hpp"

#include "opskv/logging.hpp"
#include "opskv/storage/mvcc.hpp"

#include <chrono>
#include <cstring>
#include <thread>

namespace {
struct ScopedRegionTimer {
  opskv::storage::RegionMetrics* metrics{nullptr};
  uint32_t region{0};
  bool propose{true};
  std::chrono::steady_clock::time_point t0{std::chrono::steady_clock::now()};
  ~ScopedRegionTimer() {
    if (!metrics) return;
    const auto us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0)
            .count());
    if (propose) {
      metrics->observe_propose(region, us);
    } else {
      metrics->observe_get(region, us);
    }
  }
};
}  // namespace

namespace opskv::storage {
namespace {

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

}  // namespace

std::string encode_prewrite_value(uint64_t start_ts, uint8_t mvcc_op, std::string_view primary,
                                  std::string_view value) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&start_ts), sizeof(start_ts));
  out.push_back(static_cast<char>(mvcc_op));
  append_u32(out, static_cast<uint32_t>(primary.size()));
  out.append(primary);
  append_u32(out, static_cast<uint32_t>(value.size()));
  out.append(value);
  return out;
}

bool decode_prewrite_value(std::string_view data, uint64_t* start_ts, uint8_t* mvcc_op,
                           std::string* primary, std::string* value) {
  size_t off = 0;
  if (!read_u64(data, off, *start_ts) || off >= data.size()) return false;
  *mvcc_op = static_cast<uint8_t>(data[off++]);
  uint32_t plen = 0, vlen = 0;
  if (!read_u32(data, off, plen) || off + plen > data.size()) return false;
  primary->assign(data.data() + off, plen);
  off += plen;
  if (!read_u32(data, off, vlen) || off + vlen > data.size()) return false;
  value->assign(data.data() + off, vlen);
  return true;
}

std::string encode_commit_value(uint64_t start_ts, uint64_t commit_ts) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&start_ts), sizeof(start_ts));
  out.append(reinterpret_cast<const char*>(&commit_ts), sizeof(commit_ts));
  return out;
}

bool decode_commit_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts) {
  size_t off = 0;
  return read_u64(data, off, *start_ts) && read_u64(data, off, *commit_ts);
}

std::string encode_rollback_value(uint64_t start_ts) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&start_ts), sizeof(start_ts));
  return out;
}

bool decode_rollback_value(std::string_view data, uint64_t* start_ts) {
  size_t off = 0;
  return read_u64(data, off, *start_ts);
}

std::string encode_pessimistic_lock_value(uint64_t start_ts, uint64_t for_update_ts,
                                          uint64_t ttl_ms, std::string_view primary) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&start_ts), sizeof(start_ts));
  out.append(reinterpret_cast<const char*>(&for_update_ts), sizeof(for_update_ts));
  out.append(reinterpret_cast<const char*>(&ttl_ms), sizeof(ttl_ms));
  append_u32(out, static_cast<uint32_t>(primary.size()));
  out.append(primary);
  return out;
}

bool decode_pessimistic_lock_value(std::string_view data, uint64_t* start_ts,
                                   uint64_t* for_update_ts, uint64_t* ttl_ms,
                                   std::string* primary) {
  size_t off = 0;
  if (!read_u64(data, off, *start_ts) || !read_u64(data, off, *for_update_ts) ||
      !read_u64(data, off, *ttl_ms)) {
    return false;
  }
  uint32_t plen = 0;
  if (!read_u32(data, off, plen) || off + plen > data.size()) return false;
  primary->assign(data.data() + off, plen);
  return true;
}

// 二进制布局：op(1) | klen(4) | vlen(4) | ttl_ms(8) | key | value [| request_id(8)]
std::string encode_command(const Command& cmd) {
  std::string out;
  out.push_back(static_cast<char>(cmd.op));
  uint32_t klen = static_cast<uint32_t>(cmd.key.size());
  uint32_t vlen = static_cast<uint32_t>(cmd.value.size());
  out.append(reinterpret_cast<const char*>(&klen), sizeof(klen));
  out.append(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
  out.append(reinterpret_cast<const char*>(&cmd.ttl_ms), sizeof(cmd.ttl_ms));
  out.append(cmd.key);
  out.append(cmd.value);
  if (cmd.request_id != 0) {
    out.append(reinterpret_cast<const char*>(&cmd.request_id), sizeof(cmd.request_id));
  }
  return out;
}

bool decode_command(const std::string& data, Command* out) {
  if (data.size() < 1 + 8 + 8) {
    return false;
  }
  size_t off = 0;
  out->op = static_cast<CmdOp>(data[off++]);
  uint32_t klen = 0, vlen = 0;
  std::memcpy(&klen, data.data() + off, sizeof(klen));
  off += sizeof(klen);
  std::memcpy(&vlen, data.data() + off, sizeof(vlen));
  off += sizeof(vlen);
  std::memcpy(&out->ttl_ms, data.data() + off, sizeof(out->ttl_ms));
  off += sizeof(out->ttl_ms);
  if (off + klen + vlen > data.size()) {
    return false;
  }
  out->key.assign(data.data() + off, klen);
  off += klen;
  out->value.assign(data.data() + off, vlen);
  off += vlen;
  out->request_id = 0;
  if (off == data.size()) return true;
  if (off + sizeof(uint64_t) != data.size()) return false;
  std::memcpy(&out->request_id, data.data() + off, sizeof(out->request_id));
  return true;
}

StoreFacade::StoreFacade(StorageNode& node) : node_(node) {}

ProposeResult StoreFacade::require_leader(uint32_t shard_id, uint64_t expected_epoch) {
  ProposeResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.error = "no_region";
    return r;
  }
  if (!group->is_leader()) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    r.error = "not_leader";
    return r;
  }
  if (expected_epoch != 0) {
    const uint64_t cur = node_.region_epoch(shard_id);
    if (cur != expected_epoch) {
      r.error = "epoch_not_match expected=" + std::to_string(cur);
      return r;
    }
  }
  if (node_.region_splitting(shard_id)) {
    r.error = "splitting";
    return r;
  }
  r.ok = true;
  return r;
}

namespace {
bool cmd_has_user_key(const Command& cmd) {
  switch (cmd.op) {
    case CmdOp::kPut:
    case CmdOp::kDelete:
    case CmdOp::kPrewrite:
    case CmdOp::kCommit:
    case CmdOp::kRollback:
    case CmdOp::kPessimisticLock:
    case CmdOp::kPessimisticRollback:
      return true;
    default:
      return false;
  }
}
}  // namespace

ProposeResult StoreFacade::propose(uint32_t shard_id, const Command& cmd) {
  ScopedRegionTimer timer{&metrics_, shard_id, true};
  ProposeResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.error = "no_region";
    return r;
  }
  if (!group->is_leader()) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    r.error = "not_leader";
    return r;
  }
  // E4：同 request_id 重试直接返回缓存，不再二次 propose
  if (cmd.request_id != 0) {
    if (auto hit = node_.lookup_request(shard_id, cmd.request_id)) {
      r.ok = hit->ok;
      r.revision = hit->revision;
      r.error = hit->error;
      return r;
    }
  }
  // 普通 PUT/DEL 也走分裂暂停（与事务写一致）；窗口仅覆盖 Raft apply 元数据，无物理搬迁。
  if ((cmd.op == CmdOp::kPut || cmd.op == CmdOp::kDelete) && node_.region_splitting(shard_id)) {
    r.error = "splitting";
    return r;
  }
  if (cmd_has_user_key(cmd) && !node_.key_in_region(shard_id, cmd.key)) {
    r.error = "key_not_in_region";
    return r;
  }
  uint64_t idx = 0;
  if (!group->propose(encode_command(cmd), &idx)) {
    r.error = "propose_failed";
    return r;
  }
  for (int i = 0; i < 400; ++i) {
    auto st = group->soft_state();
    if (st.last_applied >= idx) {
      auto ar = node_.take_apply_result(idx);
      if (ar && !ar->empty()) {
        r.error = *ar;
        r.revision = idx;
        if (cmd.request_id != 0) {
          node_.remember_request(shard_id, cmd.request_id,
                                 StorageNode::IdempotentRecord{false, idx, r.error});
        }
        return r;
      }
      r.ok = true;
      r.revision = idx;
      // apply 路径已 remember；此处兜底（竞态下 apply 尚未写入缓存）
      if (cmd.request_id != 0 && !node_.lookup_request(shard_id, cmd.request_id)) {
        node_.remember_request(shard_id, cmd.request_id,
                               StorageNode::IdempotentRecord{true, idx, {}});
      }
      return r;
    }
    if (i % 10 == 9 && group->is_leader()) {
      group->tick();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  r.error = "apply_timeout";
  return r;
}

GetResult StoreFacade::get(uint32_t shard_id, std::string_view key, bool linearizable) {
  ScopedRegionTimer timer{&metrics_, shard_id, false};
  GetResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.redirect_to = "no_region";  // tcp 层识别
    return r;
  }
  r.is_leader = group->is_leader();
  if (linearizable && !r.is_leader) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    return r;
  }
  if (linearizable && r.is_leader) {
    uint64_t read_ci = 0;
    if (!group->read_index(&read_ci)) {
      r.redirect_to = group->leader_address();
      return r;
    }
    // 确保状态机至少 apply 到 ReadIndex 水位
    for (int i = 0; i < 200; ++i) {
      if (group->soft_state().last_applied >= read_ci) break;
      if (i % 10 == 9) group->tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  if (!node_.key_in_region(shard_id, key)) {
    r.redirect_to = "key_not_in_region";
    return r;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) return r;
  if (auto rec = eng->get(key)) {
    r.found = true;
    r.value = rec->value;
    r.revision = rec->revision;
  }
  return r;
}

ProposeResult StoreFacade::prewrite(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                    std::string_view primary, uint8_t mvcc_op,
                                    std::string_view value, uint64_t expected_epoch) {
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts != start_ts) {
      gate.ok = false;
      gate.error = "locked";
      return gate;
    }
    // 同事务悲观锁 → 允许升级为 prewrite
  } else if (mvcc.has_write_conflict(key, start_ts)) {
    gate.ok = false;
    gate.error = "write_conflict";
    return gate;
  }
  Command cmd;
  cmd.op = CmdOp::kPrewrite;
  cmd.key = std::string(key);
  cmd.value = encode_prewrite_value(start_ts, mvcc_op, primary, value);
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::commit(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                  uint64_t commit_ts, uint64_t expected_epoch) {
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  // 已提交则幂等成功（崩溃恢复 resolve）
  if (auto cts = mvcc.find_commit_ts(key, start_ts)) {
    if (*cts == commit_ts || commit_ts != 0) {
      gate.ok = true;
      return gate;
    }
  }
  auto lk = mvcc.get_lock(key);
  if (!lk || lk->start_ts != start_ts) {
    // 无锁且已有 write → 视为已 resolve
    if (mvcc.find_commit_ts(key, start_ts)) {
      gate.ok = true;
      return gate;
    }
    gate.ok = false;
    gate.error = "lock_not_found";
    return gate;
  }
  if (lk->op == MvccOp::kPessimistic) {
    gate.ok = false;
    gate.error = "pessimistic_not_prewritten";
    return gate;
  }
  Command cmd;
  cmd.op = CmdOp::kCommit;
  cmd.key = std::string(key);
  cmd.value = encode_commit_value(start_ts, commit_ts);
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                    uint64_t expected_epoch) {
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  Command cmd;
  cmd.op = CmdOp::kRollback;
  cmd.key = std::string(key);
  cmd.value = encode_rollback_value(start_ts);
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::pessimistic_lock(uint32_t shard_id, std::string_view key,
                                            uint64_t start_ts, uint64_t for_update_ts,
                                            std::string_view primary, uint64_t ttl_ms,
                                            uint64_t expected_epoch) {
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts == start_ts) {
      gate.ok = true;
      return gate;  // idempotent
    }
    gate.ok = false;
    gate.error = "locked";
    return gate;
  }
  if (mvcc.has_write_conflict(key, for_update_ts != 0 ? for_update_ts : start_ts)) {
    gate.ok = false;
    gate.error = "write_conflict";
    return gate;
  }
  Command cmd;
  cmd.op = CmdOp::kPessimisticLock;
  cmd.key = std::string(key);
  cmd.value = encode_pessimistic_lock_value(start_ts, for_update_ts, ttl_ms, primary);
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::pessimistic_rollback(uint32_t shard_id, std::string_view key,
                                                uint64_t start_ts, uint64_t expected_epoch) {
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  Command cmd;
  cmd.op = CmdOp::kPessimisticRollback;
  cmd.key = std::string(key);
  cmd.value = encode_rollback_value(start_ts);
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::resolve_lock(uint32_t shard_id, std::string_view key,
                                        uint64_t start_ts, uint64_t commit_ts,
                                        uint64_t expected_epoch) {
  if (commit_ts == 0) {
    return rollback(shard_id, key, start_ts, expected_epoch);
  }
  return commit(shard_id, key, start_ts, commit_ts, expected_epoch);
}

TxnGetResult StoreFacade::txn_get(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                  bool linearizable) {
  TxnGetResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.redirect_to = "no_region";
    return r;
  }
  r.is_leader = group->is_leader();
  if (linearizable && !r.is_leader) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    return r;
  }
  if (linearizable && r.is_leader) {
    uint64_t read_ci = 0;
    if (!group->read_index(&read_ci)) {
      r.redirect_to = group->leader_address();
      r.error = "read_index_failed";
      return r;
    }
    for (int i = 0; i < 200; ++i) {
      if (group->soft_state().last_applied >= read_ci) break;
      if (i % 10 == 9) group->tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) return r;
  MvccStore mvcc(*eng);
  bool locked = false;
  LockInfo blk;
  auto val = mvcc.txn_get(key, start_ts, &locked, &blk);
  r.locked = locked;
  if (locked) {
    r.blocking_lock = blk;
    r.error = "locked";
    return r;
  }
  if (val) {
    r.found = true;
    r.value = std::move(*val);
  }
  return r;
}

GetLockResult StoreFacade::get_lock(uint32_t shard_id, std::string_view key) {
  GetLockResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.redirect_to = "no_region";
    return r;
  }
  r.is_leader = group->is_leader();
  if (!r.is_leader) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    return r;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) return r;
  if (auto lk = MvccStore(*eng).get_lock(key)) {
    r.found = true;
    r.lock = *lk;
  }
  return r;
}

CheckTxnResult StoreFacade::check_txn_status(uint32_t shard_id, std::string_view primary,
                                             uint64_t lock_ts) {
  CheckTxnResult r;
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.redirect_to = "no_region";
    return r;
  }
  r.is_leader = group->is_leader();
  if (!r.is_leader) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    return r;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) return r;
  auto st = MvccStore(*eng).check_txn_status(primary, lock_ts);
  r.status = st.status;
  r.commit_ts = st.commit_ts;
  r.lock_expired = st.lock_expired;
  return r;
}

}  // namespace opskv::storage
