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

#include "openkv/storage/store_service.hpp"

#include "openkv/codec.hpp"
#include "openkv/logging.hpp"
#include "openkv/storage/mvcc.hpp"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <thread>

namespace {
struct ScopedRegionTimer {
  openkv::storage::RegionMetrics* metrics{nullptr};
  uint32_t region{0};
  bool propose{true};
  std::chrono::steady_clock::time_point t0{std::chrono::steady_clock::now()};

  ScopedRegionTimer() = default;
  ScopedRegionTimer(openkv::storage::RegionMetrics* m, uint32_t r, bool p)
      : metrics(m), region(r), propose(p) {}
  ScopedRegionTimer(const ScopedRegionTimer&) = delete;
  ScopedRegionTimer& operator=(const ScopedRegionTimer&) = delete;
  ScopedRegionTimer(ScopedRegionTimer&& o) noexcept
      : metrics(o.metrics), region(o.region), propose(o.propose), t0(o.t0) {
    o.metrics = nullptr;  // 避免 moved-from 临时对象析构时重复 observe
  }
  ScopedRegionTimer& operator=(ScopedRegionTimer&& o) noexcept {
    if (this == &o) return *this;
    metrics = o.metrics;
    region = o.region;
    propose = o.propose;
    t0 = o.t0;
    o.metrics = nullptr;
    return *this;
  }
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

namespace openkv::storage {
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

std::string encode_onepc_value(uint64_t start_ts, uint64_t commit_ts, uint8_t mvcc_op,
                               std::string_view primary, std::string_view value) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&commit_ts), sizeof(commit_ts));
  out.append(encode_prewrite_value(start_ts, mvcc_op, primary, value));
  return out;
}

bool decode_onepc_value(std::string_view data, uint64_t* start_ts, uint64_t* commit_ts,
                        uint8_t* mvcc_op, std::string* primary, std::string* value) {
  size_t off = 0;
  if (!read_u64(data, off, *commit_ts)) return false;
  return decode_prewrite_value(data.substr(off), start_ts, mvcc_op, primary, value);
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

// 二进制布局：
//   op(1) | klen(4) | vlen(4) | ttl_ms(8) | key | value
//   [| request_id(8)]                                    — 旧格式
//   [| request_id(8) | cas(1) | expected_revision(8)]    — CAS 扩展（17 字节尾）
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
  if (cmd.cas) {
    out.append(reinterpret_cast<const char*>(&cmd.request_id), sizeof(cmd.request_id));
    out.push_back(static_cast<char>(1));
    out.append(reinterpret_cast<const char*>(&cmd.expected_revision), sizeof(cmd.expected_revision));
  } else if (cmd.request_id != 0) {
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
  out->cas = false;
  out->expected_revision = 0;
  if (off == data.size()) return true;
  const size_t rem = data.size() - off;
  if (rem == sizeof(uint64_t)) {
    std::memcpy(&out->request_id, data.data() + off, sizeof(out->request_id));
    return true;
  }
  if (rem == sizeof(uint64_t) + 1 + sizeof(uint64_t)) {
    std::memcpy(&out->request_id, data.data() + off, sizeof(out->request_id));
    off += sizeof(out->request_id);
    out->cas = (static_cast<uint8_t>(data[off++]) != 0);
    std::memcpy(&out->expected_revision, data.data() + off, sizeof(out->expected_revision));
    return true;
  }
  return false;
}

int64_t stamp_expire_at_ms(int64_t ttl_ms) {
  if (ttl_ms <= 0) return 0;
  return wall_ms_now() + ttl_ms;
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
    case CmdOp::kOnePc:
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

namespace {

void fill_cas_meta(ProposeResult& pr) {
  if (pr.error.rfind("cas_failed", 0) != 0) return;
  pr.cas_failed = true;
  const auto pos = pr.error.find("current_revision=");
  if (pos != std::string::npos) {
    pr.current_revision = std::strtoull(pr.error.c_str() + pos + 17, nullptr, 10);
  }
}

ProposeResult finish_propose(StorageNode& node, uint32_t shard_id, const Command& cmd, uint64_t idx,
                             const ProposalQueue::WaitOutcome& outcome, uint64_t latch_tok) {
  if (auto* sched = node.scheduler()) sched->unlock(latch_tok);
  ProposeResult r;
  if (outcome.timed_out) {
    r.error = "apply_timeout";
    return r;
  }
  r.revision = idx;
  if (!outcome.error.empty()) {
    r.error = outcome.error;
    fill_cas_meta(r);
    if (cmd.request_id != 0) {
      node.remember_request(shard_id, cmd.request_id,
                            StorageNode::IdempotentRecord{false, idx, r.error});
    }
    return r;
  }
  r.ok = true;
  if (cmd.request_id != 0 && !node.lookup_request(shard_id, cmd.request_id)) {
    node.remember_request(shard_id, cmd.request_id, StorageNode::IdempotentRecord{true, idx, {}});
  }
  return r;
}

}  // namespace

ProposeResult StoreFacade::propose(uint32_t shard_id, const Command& cmd) {
  std::promise<ProposeResult> done;
  propose_async(shard_id, cmd, [&](ProposeResult out) { done.set_value(std::move(out)); });
  return done.get_future().get();
}

void StoreFacade::propose_async(uint32_t shard_id, const Command& cmd,
                                std::function<void(ProposeResult)> cb, uint64_t expected_epoch) {
  if (!cb) return;
  // 原地构造，避免临时对象析构导致 observe 双打。
  auto timer = std::make_shared<ScopedRegionTimer>(&metrics_, shard_id, true);
  auto fail = [&](std::string err, std::string redirect = {}) {
    ProposeResult r;
    r.error = std::move(err);
    r.redirect_to = std::move(redirect);
    cb(std::move(r));
  };

  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    fail("no_region");
    return;
  }
  if (!group->is_leader()) {
    std::string redir = group->leader_address();
    if (redir.empty()) redir = node_.peer_address(group->soft_state().leader_id);
    fail("not_leader", std::move(redir));
    return;
  }
  if (expected_epoch != 0) {
    const uint64_t cur = node_.region_epoch(shard_id);
    if (cur != expected_epoch) {
      fail("epoch_not_match expected=" + std::to_string(cur));
      return;
    }
  }
  // E4：同 request_id 重试直接返回缓存，不再二次 propose
  if (cmd.request_id != 0) {
    if (auto hit = node_.lookup_request(shard_id, cmd.request_id)) {
      ProposeResult r;
      r.ok = hit->ok;
      r.revision = hit->revision;
      r.error = hit->error;
      cb(std::move(r));
      return;
    }
  }
  // PUT/DEL/ONEPC 走分裂暂停（与事务写一致）；窗口仅覆盖 Raft apply 元数据，无物理搬迁。
  if ((cmd.op == CmdOp::kPut || cmd.op == CmdOp::kDelete || cmd.op == CmdOp::kOnePc) &&
      node_.region_splitting(shard_id)) {
    fail("splitting");
    return;
  }
  if (cmd_has_user_key(cmd) && !node_.key_in_region(shard_id, cmd.key)) {
    fail("key_not_in_region");
    return;
  }

  // Raw 平面 LWW（非 CAS Put/Delete）：Raft apply 已串行，跳过 latch，避免 Propose CQ 上 cv.wait。
  // 事务 / CAS：lock_async 占锁持到 apply；冲突时回调续跑，不阻塞接收线程。
  auto* sched = node_.scheduler();
  const bool skip_latch =
      (cmd.op == CmdOp::kPut || cmd.op == CmdOp::kDelete) && !cmd.cas;

  auto after_latch = [this, shard_id, cmd, timer = std::move(timer), cb = std::move(cb),
                      sched](uint64_t latch_tok) mutable {
    uint64_t idx = 0;
    std::string perr;
    bool proposed = false;
    if (auto* kv = node_.raftkv()) {
      proposed = kv->async_write(shard_id, encode_command(cmd), &idx, &perr);
    } else if (auto* g = node_.multi_raft().get(shard_id)) {
      if (g->propose(encode_command(cmd), &idx)) {
        proposed = true;
      } else {
        perr = "propose_failed";
      }
    } else {
      perr = "no_region";
    }
    if (!proposed) {
      if (sched) sched->unlock(latch_tok);
      ProposeResult r;
      r.error = perr.empty() ? "propose_failed" : std::move(perr);
      cb(std::move(r));
      return;
    }

    // 无 waiter tick：BatchSystem 经 flush_ready + tick_loop 推进；apply → invoke → Finish。
    node_.wait_apply_async(
        idx, std::chrono::milliseconds(5000),
        [this, shard_id, cmd, idx, latch_tok, timer = std::move(timer), cb = std::move(cb)](
            ProposalQueue::WaitOutcome outcome) mutable {
          cb(finish_propose(node_, shard_id, cmd, idx, outcome, latch_tok));
          timer.reset();
        });
  };

  if (skip_latch || !sched) {
    after_latch(0);
    return;
  }
  const std::string latch_key =
      cmd_has_user_key(cmd) ? cmd.key : (std::string("op:") + std::to_string(static_cast<int>(cmd.op)));
  sched->lock_key_async(latch_key, std::move(after_latch));
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
                                    std::string_view value, uint64_t expected_epoch, bool cas,
                                    uint64_t expected_revision, int64_t expire_at_ms) {
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
  // Leader 快路径 CAS 预检（最终仍以 apply 为准）
  if (cas) {
    bool locked = false;
    uint64_t cur_rev = 0;
    auto vis = mvcc.txn_get(key, ~uint64_t{0}, &locked, nullptr, &cur_rev);
    if (locked) {
      gate.ok = false;
      gate.error = "locked";
      return gate;
    }
    const bool match = (expected_revision == 0) ? !vis.has_value() : (cur_rev == expected_revision);
    if (!match) {
      gate.ok = false;
      gate.cas_failed = true;
      gate.current_revision = cur_rev;
      gate.error = "cas_failed current_revision=" + std::to_string(cur_rev);
      return gate;
    }
  }
  Command cmd;
  cmd.op = CmdOp::kPrewrite;
  cmd.key = std::string(key);
  cmd.value = encode_prewrite_value(start_ts, mvcc_op, primary, value);
  cmd.ttl_ms = expire_at_ms;  // 绝对时间；入口已 stamp
  cmd.cas = cas;
  cmd.expected_revision = expected_revision;
  return propose(shard_id, cmd);
}

ProposeResult StoreFacade::one_pc(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                  uint64_t commit_ts, std::string_view primary, uint8_t mvcc_op,
                                  std::string_view value, uint64_t expected_epoch, bool cas,
                                  uint64_t expected_revision, int64_t expire_at_ms) {
  std::promise<ProposeResult> done;
  one_pc_async(shard_id, std::string(key), start_ts, commit_ts, std::string(primary), mvcc_op,
               std::string(value), expected_epoch, cas, expected_revision, expire_at_ms,
               [&](ProposeResult r) { done.set_value(std::move(r)); });
  return done.get_future().get();
}

void StoreFacade::one_pc_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                               uint64_t commit_ts, std::string primary, uint8_t mvcc_op,
                               std::string value, uint64_t expected_epoch, bool cas,
                               uint64_t expected_revision, int64_t expire_at_ms,
                               std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  auto fail = [&](std::string err, bool cas_failed = false, uint64_t cur_rev = 0) {
    gate.ok = false;
    gate.error = std::move(err);
    gate.cas_failed = cas_failed;
    gate.current_revision = cur_rev;
    cb(std::move(gate));
  };
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  if (commit_ts <= start_ts) {
    fail("bad_ts");
    return;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    fail("no_engine");
    return;
  }
  auto locked_err = [](const LockInfo& lk) {
    return "locked primary=" + openkv::to_hex(lk.primary) + " lock_ts=" +
           std::to_string(lk.start_ts);
  };
  MvccStore mvcc(*eng);
  if (auto lk = mvcc.get_lock(key)) {
    fail(locked_err(*lk));
    return;
  }
  if (mvcc.has_write_conflict(key, start_ts)) {
    fail("write_conflict");
    return;
  }
  if (cas) {
    bool locked = false;
    uint64_t cur_rev = 0;
    LockInfo blk;
    auto vis = mvcc.txn_get(key, ~uint64_t{0}, &locked, &blk, &cur_rev);
    if (locked) {
      fail(locked_err(blk));
      return;
    }
    const bool match = (expected_revision == 0) ? !vis.has_value() : (cur_rev == expected_revision);
    if (!match) {
      fail("cas_failed current_revision=" + std::to_string(cur_rev), true, cur_rev);
      return;
    }
  }
  Command cmd;
  cmd.op = CmdOp::kOnePc;
  cmd.key = std::move(key);
  cmd.value = encode_onepc_value(start_ts, commit_ts, mvcc_op, primary, value);
  cmd.ttl_ms = expire_at_ms;
  cmd.cas = cas;
  cmd.expected_revision = expected_revision;
  propose_async(shard_id, cmd, [commit_ts, cb = std::move(cb)](ProposeResult r) {
    if (r.ok) r.revision = commit_ts;
    cb(std::move(r));
  });
}

void StoreFacade::prewrite_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                 std::string primary, uint8_t mvcc_op, std::string value,
                                 uint64_t expected_epoch, bool cas, uint64_t expected_revision,
                                 int64_t expire_at_ms, std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  auto fail = [&](std::string err, bool cas_failed = false, uint64_t cur_rev = 0) {
    gate.ok = false;
    gate.error = std::move(err);
    gate.cas_failed = cas_failed;
    gate.current_revision = cur_rev;
    cb(std::move(gate));
  };
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    fail("no_engine");
    return;
  }
  MvccStore mvcc(*eng);
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts != start_ts) {
      fail("locked");
      return;
    }
    // 同事务悲观锁 → 允许升级为 prewrite
  } else if (mvcc.has_write_conflict(key, start_ts)) {
    fail("write_conflict");
    return;
  }
  if (cas) {
    bool locked = false;
    uint64_t cur_rev = 0;
    auto vis = mvcc.txn_get(key, ~uint64_t{0}, &locked, nullptr, &cur_rev);
    if (locked) {
      fail("locked");
      return;
    }
    const bool match = (expected_revision == 0) ? !vis.has_value() : (cur_rev == expected_revision);
    if (!match) {
      fail("cas_failed current_revision=" + std::to_string(cur_rev), true, cur_rev);
      return;
    }
  }
  Command cmd;
  cmd.op = CmdOp::kPrewrite;
  cmd.key = std::move(key);
  cmd.value = encode_prewrite_value(start_ts, mvcc_op, primary, value);
  cmd.ttl_ms = expire_at_ms;
  cmd.cas = cas;
  cmd.expected_revision = expected_revision;
  propose_async(shard_id, cmd, std::move(cb));
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

void StoreFacade::commit_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                               uint64_t commit_ts, uint64_t expected_epoch,
                               std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  auto fail = [&](std::string err) {
    gate.ok = false;
    gate.error = std::move(err);
    cb(std::move(gate));
  };
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    fail("no_engine");
    return;
  }
  MvccStore mvcc(*eng);
  if (auto cts = mvcc.find_commit_ts(key, start_ts)) {
    if (*cts == commit_ts || commit_ts != 0) {
      gate.ok = true;
      cb(std::move(gate));
      return;
    }
  }
  auto lk = mvcc.get_lock(key);
  if (!lk || lk->start_ts != start_ts) {
    if (mvcc.find_commit_ts(key, start_ts)) {
      gate.ok = true;
      cb(std::move(gate));
      return;
    }
    fail("lock_not_found");
    return;
  }
  if (lk->op == MvccOp::kPessimistic) {
    fail("pessimistic_not_prewritten");
    return;
  }
  Command cmd;
  cmd.op = CmdOp::kCommit;
  cmd.key = std::move(key);
  cmd.value = encode_commit_value(start_ts, commit_ts);
  propose_async(shard_id, cmd, std::move(cb));
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

void StoreFacade::rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                 uint64_t expected_epoch, std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  Command cmd;
  cmd.op = CmdOp::kRollback;
  cmd.key = std::move(key);
  cmd.value = encode_rollback_value(start_ts);
  propose_async(shard_id, cmd, std::move(cb));
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

void StoreFacade::pessimistic_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                         uint64_t for_update_ts, std::string primary,
                                         uint64_t ttl_ms, uint64_t expected_epoch,
                                         std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  auto fail = [&](std::string err) {
    gate.ok = false;
    gate.error = std::move(err);
    cb(std::move(gate));
  };
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    fail("no_engine");
    return;
  }
  MvccStore mvcc(*eng);
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts == start_ts) {
      gate.ok = true;
      cb(std::move(gate));
      return;
    }
    fail("locked");
    return;
  }
  if (mvcc.has_write_conflict(key, for_update_ts != 0 ? for_update_ts : start_ts)) {
    fail("write_conflict");
    return;
  }
  if (ttl_ms == 0) ttl_ms = 5000;
  Command cmd;
  cmd.op = CmdOp::kPessimisticLock;
  cmd.key = std::move(key);
  cmd.value = encode_pessimistic_lock_value(start_ts, for_update_ts, ttl_ms, primary);
  propose_async(shard_id, cmd, std::move(cb));
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

void StoreFacade::pessimistic_rollback_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                             uint64_t expected_epoch,
                                             std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) {
    cb(std::move(gate));
    return;
  }
  Command cmd;
  cmd.op = CmdOp::kPessimisticRollback;
  cmd.key = std::move(key);
  cmd.value = encode_rollback_value(start_ts);
  propose_async(shard_id, cmd, std::move(cb));
}

ProposeResult StoreFacade::resolve_lock(uint32_t shard_id, std::string_view key,
                                        uint64_t start_ts, uint64_t commit_ts,
                                        uint64_t expected_epoch) {
  if (commit_ts == 0) {
    return rollback(shard_id, key, start_ts, expected_epoch);
  }
  return commit(shard_id, key, start_ts, commit_ts, expected_epoch);
}

void StoreFacade::resolve_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                     uint64_t commit_ts, uint64_t expected_epoch,
                                     std::function<void(ProposeResult)> cb) {
  if (!cb) return;
  if (commit_ts == 0) {
    rollback_async(shard_id, std::move(key), start_ts, expected_epoch, std::move(cb));
    return;
  }
  commit_async(shard_id, std::move(key), start_ts, commit_ts, expected_epoch, std::move(cb));
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
  uint64_t commit_ts = 0;
  auto val = mvcc.txn_get(key, start_ts, &locked, &blk, &commit_ts);
  r.locked = locked;
  r.revision = commit_ts;
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

TxnScanResult StoreFacade::txn_scan(uint32_t shard_id, std::string_view start_key,
                                    std::string_view end_key, std::string_view start_after,
                                    uint64_t start_ts, uint32_t limit, bool keys_only,
                                    bool linearizable) {
  TxnScanResult r;
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
  if (limit == 0) limit = 256;
  if (limit > 4096) limit = 4096;
  auto range = node_.region_range(shard_id);
  auto sr = MvccStore(*eng).txn_scan(start_key, end_key, start_after, start_ts, limit, keys_only,
                                     range);
  r.pairs = std::move(sr.pairs);
  r.truncated = sr.truncated;
  r.next_key = std::move(sr.next_key);
  return r;
}

TxnTtlResult StoreFacade::txn_ttl(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                  bool linearizable) {
  TxnTtlResult r;
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
  bool locked = false;
  LockInfo blk;
  auto ttl = MvccStore(*eng).txn_ttl(key, start_ts, &locked, &blk);
  r.locked = locked;
  if (locked) {
    r.blocking_lock = blk;
    r.error = "locked";
    return r;
  }
  if (ttl) r.ttl_ms = *ttl;
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

}  // namespace openkv::storage
