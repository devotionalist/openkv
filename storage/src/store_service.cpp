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
// Command 二进制编解码与 StoreFacade 读写编排。
// CQ 热路径用 *_async；同步 propose/one_pc/… 仅单测与后台（如 txn GC）。

#include "openkv/storage/store_service.hpp"

#include "openkv/codec.hpp"
#include "openkv/logging.hpp"
#include "openkv/storage/mvcc.hpp"
#include "openkv/storage/raftkv.hpp"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <memory>

namespace {
struct ScopedRegionTimer {
  std::shared_ptr<openkv::storage::RegionMetrics> metrics;
  uint32_t region{0};
  bool propose{true};
  std::chrono::steady_clock::time_point t0{std::chrono::steady_clock::now()};

  ScopedRegionTimer() = default;
  ScopedRegionTimer(std::shared_ptr<openkv::storage::RegionMetrics> m, uint32_t r, bool p)
      : metrics(std::move(m)), region(r), propose(p) {}
  ScopedRegionTimer(const ScopedRegionTimer&) = delete;
  ScopedRegionTimer& operator=(const ScopedRegionTimer&) = delete;
  ScopedRegionTimer(ScopedRegionTimer&& o) noexcept
      : metrics(std::move(o.metrics)), region(o.region), propose(o.propose), t0(o.t0) {}
  ScopedRegionTimer& operator=(ScopedRegionTimer&& o) noexcept {
    if (this == &o) return *this;
    metrics = std::move(o.metrics);
    region = o.region;
    propose = o.propose;
    t0 = o.t0;
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

// Prewrite value 布局：start_ts(8) | mvcc_op(1) | plen(4) | primary | vlen(4) | value
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
  // 1) start_ts → 2) mvcc_op → 3) primary → 4) value；任一字段越界则失败。
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

// Commit value：start_ts(8) | commit_ts(8)
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

// ONEPC value：commit_ts(8) | prewrite payload（一次 propose 写 lock+data+write）
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

// Rollback / PessimisticRollback value：仅 start_ts(8)
std::string encode_rollback_value(uint64_t start_ts) {
  std::string out;
  out.append(reinterpret_cast<const char*>(&start_ts), sizeof(start_ts));
  return out;
}

bool decode_rollback_value(std::string_view data, uint64_t* start_ts) {
  size_t off = 0;
  return read_u64(data, off, *start_ts);
}

// PessimisticLock value：start_ts | for_update_ts | ttl_ms | plen | primary
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

// Command 二进制布局（Raft log data）：
//   op(1) | klen(4) | vlen(4) | ttl_ms(8) | key | value
//   [| cas(1) | expected_revision(8)]                         — CAS 尾（9 字节）
// 兼容旧尾（解码忽略）：request_id(8)；request_id(8)|cas(1)|expected_revision(8)
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
    out.push_back(static_cast<char>(1));
    out.append(reinterpret_cast<const char*>(&cmd.expected_revision), sizeof(cmd.expected_revision));
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
  out->cas = false;
  out->expected_revision = 0;
  if (off == data.size()) return true;

  // rem: 8=旧 request_id（忽略）；9=CAS；17=旧 request_id+CAS（跳过 request_id）
  const size_t rem = data.size() - off;
  if (rem == sizeof(uint64_t)) return true;
  if (rem == sizeof(uint64_t) + 1 + sizeof(uint64_t)) {
    off += sizeof(uint64_t);
  } else if (rem != 1 + sizeof(uint64_t)) {
    return false;
  }
  out->cas = (static_cast<uint8_t>(data[off++]) != 0);
  std::memcpy(&out->expected_revision, data.data() + off, sizeof(out->expected_revision));
  return true;
}

int64_t stamp_expire_at_ms(int64_t ttl_ms) {
  // 相对 TTL → 绝对 expire_at；≤0 表示永久（存 0）。
  if (ttl_ms <= 0) return 0;
  return wall_ms_now() + ttl_ms;
}

StoreFacade::StoreFacade(StorageNode& node)
    : node_(node), metrics_(std::make_shared<RegionMetrics>()) {}

ProposeResult StoreFacade::require_leader(uint32_t shard_id, uint64_t expected_epoch) {
  // ---- 事务写预检共用门闩：Region 存在 → Leader → epoch → 非分裂。
  ProposeResult r;
  // 1) 本机是否托管该 Region。
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.error = "no_region";
    return r;
  }
  // 2) 仅 Leader 可写；Follower 填 redirect。
  if (!group->is_leader()) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    r.error = "not_leader";
    return r;
  }
  // 3) 可选 epoch，防路由陈旧。
  if (expected_epoch != 0) {
    const uint64_t cur = node_.region_epoch(shard_id);
    if (cur != expected_epoch) {
      r.error = "epoch_not_match expected=" + std::to_string(cur);
      return r;
    }
  }
  // 4) 分裂窗口内拒绝事务写。
  if (node_.region_splitting(shard_id)) {
    r.error = "splitting";
    return r;
  }
  r.ok = true;
  return r;
}

namespace {
bool cmd_has_user_key(const Command& cmd) {
  // 带用户 key 的 op 才做 key_in_region / 按 key 占 latch；Admin/GC 等无用户 key。
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

bool ensure_applied_catchup(StorageNode& node, raft::RaftNode* group, uint32_t shard_id,
                            uint64_t read_ci) {
  // ---- Leader 线性读：ReadIndex 拿到 commit 点后，等到 last_applied ≥ read_ci。
  // 1) 已追上则立即成功；2) 否则 CV 等待，等待期间 flush_ready 推进 Ready（无 sleep 自旋）。
  if (!group) return false;
  if (group->soft_state().last_applied >= read_ci) return true;
  return group->wait_applied(read_ci, 400, [&] {
    node.raftkv().flush_ready(shard_id);
  });
}

void fill_cas_meta(ProposeResult& pr) {
  // 从 apply 错误串解析 cas_failed + current_revision，供 Gateway 回客户端。
  if (pr.error.rfind("cas_failed", 0) != 0) return;
  pr.cas_failed = true;
  const auto pos = pr.error.find("current_revision=");
  if (pos != std::string::npos) {
    pr.current_revision = std::strtoull(pr.error.c_str() + pos + 17, nullptr, 10);
  }
}

ProposeResult finish_propose(StorageNode& node, uint64_t idx,
                             const ProposalQueue::WaitOutcome& outcome, uint64_t latch_tok) {
  // apply 收尾：unlock latch → 填 ProposeResult（超时 / 业务错 / 成功）。
  node.scheduler().unlock(latch_tok);
  ProposeResult r;
  if (outcome.timed_out) {
    r.error = "apply_timeout";
    return r;
  }
  r.revision = idx;  // 默认 Raft log index；ONEPC 外层改为 commit_ts
  if (!outcome.error.empty()) {
    r.error = outcome.error;
    fill_cas_meta(r);
    return r;
  }
  r.ok = true;
  return r;
}

}  // namespace

ProposeResult StoreFacade::propose(uint32_t shard_id, const Command& cmd) {
  // ---- 同步包装：promise 堵住调用线程直到 apply；仅单测 / 后台（txn GC），CQ 勿用。
  std::promise<ProposeResult> done;
  propose_async(shard_id, cmd, [&](ProposeResult out) { done.set_value(std::move(out)); });
  return done.get_future().get();
}

void StoreFacade::propose_async(uint32_t shard_id, const Command& cmd,
                                std::function<void(ProposeResult)> cb, uint64_t expected_epoch) {
  // ---- 入口：异步 Propose；本函数不阻塞等 apply，结果经 cb 回传（gRPC Finish / 同步 propose 的 future）。
  if (!cb) return;

  // 1) 延迟直方图：shared_ptr 保证临时 StoreFacade（如 txn GC）析构后，apply 回调仍能 observe。
  auto timer = std::make_shared<ScopedRegionTimer>(metrics_, shard_id, true);
  auto fail = [&](std::string err, std::string redirect = {}) {
    ProposeResult r;
    r.error = std::move(err);
    r.redirect_to = std::move(redirect);
    cb(std::move(r));
  };

  // 2) 取本机该 Region 的 Raft 组；未托管则拒绝（禁止 get_or_create，避免误拉孤儿 Leader）。
  StorageNode& node = node_;
  auto* group = node.multi_raft().get(shard_id);
  if (!group) {  // shard 未托管于本节点
    fail("no_region");
    return;
  }

  // 3) 仅 Leader 可写；Follower 带回 redirect，供 Gateway 重路由。
  if (!group->is_leader()) {
    std::string redir = group->leader_address();
    if (redir.empty()) redir = node.peer_address(group->soft_state().leader_id);
    fail("not_leader", std::move(redir));
    return;
  }

  // 4) 可选 epoch 校验：Gateway 带路由 epoch，防分裂/合并后打到陈旧 Region。
  if (expected_epoch != 0) {
    const uint64_t cur = node.region_epoch(shard_id);
    if (cur != expected_epoch) {
      fail("epoch_not_match expected=" + std::to_string(cur));
      return;
    }
  }

  // 5) 分裂窗口：PUT/DEL/ONEPC 暂停写入（仅挡 Raft apply 元数据窗口，无物理搬迁）。
  if ((cmd.op == CmdOp::kPut || cmd.op == CmdOp::kDelete || cmd.op == CmdOp::kOnePc) &&
      node.region_splitting(shard_id)) {
    fail("splitting");
    return;
  }

  // 6) Key 归属：用户 key 必须落在本 Region range，否则 Gateway 应改路由。
  if (cmd_has_user_key(cmd) && !node.key_in_region(shard_id, cmd.key)) {
    fail("key_not_in_region");
    return;
  }

  // 7) 是否跳过 latch：
  //    - Raw 非 CAS Put/Delete：LWW，Raft apply 已串行 → 跳过，省 CQ 上排队。
  //    - 事务 / CAS / 其它写：同 key 先占内存 latch，持到 apply，防并发乱序。
  const bool skip_latch =
      (cmd.op == CmdOp::kPut || cmd.op == CmdOp::kDelete) && !cmd.cas;

  // 8) 占锁成功（或跳过 latch）后的续跑闭包。
  //    只捕获 StorageNode&（节点寿命覆盖异步路径）；禁止捕获 StoreFacade* this。
  auto after_latch = [&node, shard_id, cmd, timer = std::move(timer), cb = std::move(cb)](
                         uint64_t latch_tok) mutable {
    // 8a) 写入门面：encode → RaftNode::propose（仅 append 日志拿 index）→ Store::request_ready 攒批。
    //     此处返回 true 只表示已进日志+登记 Ready，不等于已落盘/复制/apply。
    uint64_t idx = 0;
    std::string perr;
    if (!node.raftkv().async_write(shard_id, encode_command(cmd), &idx, &perr)) {
      node.scheduler().unlock(latch_tok);  // propose 失败立即放锁
      ProposeResult r;
      r.error = perr.empty() ? "propose_failed" : std::move(perr);
      cb(std::move(r));
      return;
    }

    // 8b) 按 log index 挂 ProposalQueue waiter（≤5s）；
    //     Ready→persist→复制→commit→apply→invoke 后唤醒；本线程不 tick、不自旋。
    node.wait_apply_async(
        idx, std::chrono::milliseconds(5000),
        [&node, idx, latch_tok, timer = std::move(timer), cb = std::move(cb)](
            ProposalQueue::WaitOutcome outcome) mutable {
          // 8c) finish_propose：unlock latch → 填 ProposeResult → 调 cb。
          cb(finish_propose(node, idx, outcome, latch_tok));
          timer.reset();  // 结束计时，写入 metrics
        });
  };

  // 9) 无 latch：直接续跑（latch_tok=0，unlock 为空操作）。
  if (skip_latch) {
    after_latch(0);
    return;
  }

  // 10) 有 latch：按用户 key（或 op:N）异步占锁；冲突则入队，unlock 时再回调 after_latch。
  const std::string latch_key =
      cmd_has_user_key(cmd) ? cmd.key : (std::string("op:") + std::to_string(static_cast<int>(cmd.op)));
  node.scheduler().lock_key_async(latch_key, std::move(after_latch));
}

GetResult StoreFacade::get(uint32_t shard_id, std::string_view key, bool linearizable) {
  // ---- Raw / plain 读：可选线性（ReadIndex+catchup），否则本地引擎直读。
  // 1) 读延迟计时。
  ScopedRegionTimer timer{metrics_, shard_id, false};
  GetResult r;
  // 2) Region 存在性。
  auto* group = node_.multi_raft().get(shard_id);
  if (!group) {
    r.redirect_to = "no_region";  // tcp 层识别
    return r;
  }
  r.is_leader = group->is_leader();
  // 3) 线性读要求 Leader；Follower 只回 redirect。
  if (linearizable && !r.is_leader) {
    r.redirect_to = group->leader_address();
    if (r.redirect_to.empty()) {
      r.redirect_to = node_.peer_address(group->soft_state().leader_id);
    }
    return r;
  }
  // 4) Leader 线性：ReadIndex 取 commit 点，再等 apply 追上（防读到未提交可见性）。
  if (linearizable && r.is_leader) {
    uint64_t read_ci = 0;
    if (!group->read_index(&read_ci)) {
      r.redirect_to = group->leader_address();
      return r;
    }
    (void)ensure_applied_catchup(node_, group, shard_id, read_ci);
  }
  // 5) Key 必须在本 Region range。
  if (!node_.key_in_region(shard_id, key)) {
    r.redirect_to = "key_not_in_region";
    return r;
  }
  // 6) 引擎直读 plain KvRecord（非 MVCC txn_get）。
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
  // ---- 同步 Prewrite（2PC 第一阶段）：预检后 propose 写 lock CF。
  // 1) Leader / epoch / 分裂门闩。
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  // 2) 取引擎。
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  // 3) 锁冲突：异事务锁 → locked；同 start_ts 悲观锁可升级为 prewrite。
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts != start_ts) {
      gate.ok = false;
      gate.error = "locked";
      return gate;
    }
  } else if (mvcc.has_write_conflict(key, start_ts)) {
    // 4) 无锁时再查 write 冲突（有更新的 commit）。
    gate.ok = false;
    gate.error = "write_conflict";
    return gate;
  }
  // 5) CAS 快路径预检（最终仍以 apply 为准）。
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
  // 6) 组 Command → 同步 propose（等 apply）。
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
  // ---- 同步 ONEPC 包装：promise 等 one_pc_async 完成。
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
  // ---- 异步 1PC（Put/Delete 热路径）：预检失败同步 cb；成功走 propose_async。
  if (!cb) return;
  // 1) Leader / epoch / 分裂。
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
  // 2) commit_ts 必须严格大于 start_ts。
  if (commit_ts <= start_ts) {
    fail("bad_ts");
    return;
  }
  // 3) 引擎。
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
  // 4) 已有任意锁 → 拒（1PC 不升级悲观锁）。
  if (auto lk = mvcc.get_lock(key)) {
    fail(locked_err(*lk));
    return;
  }
  // 5) write 冲突。
  if (mvcc.has_write_conflict(key, start_ts)) {
    fail("write_conflict");
    return;
  }
  // 6) 可选 CAS 预检。
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
  // 7) 组 kOnePc → propose_async；成功时 revision 对外暴露为 commit_ts（非 log index）。
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
  // ---- 异步 Prewrite：预检同 sync；成功走 propose_async（不占 CQ 等 apply）。
  if (!cb) return;
  // 1) 门闩。
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
  // 2) 引擎。
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    fail("no_engine");
    return;
  }
  MvccStore mvcc(*eng);
  // 3) 锁 / write 冲突（同事务悲观锁可升级）。
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts != start_ts) {
      fail("locked");
      return;
    }
  } else if (mvcc.has_write_conflict(key, start_ts)) {
    fail("write_conflict");
    return;
  }
  // 4) CAS 预检。
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
  // 5) 组 Command → propose_async。
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
  // ---- 同步 Commit（2PC 第二阶段）：清 lock、写 write CF。
  // 1) 门闩。
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  // 2) 已有 write → 幂等成功（崩溃恢复 / 重复 commit）。
  if (auto cts = mvcc.find_commit_ts(key, start_ts)) {
    if (*cts == commit_ts || commit_ts != 0) {
      gate.ok = true;
      return gate;
    }
  }
  // 3) 锁必须存在且 start_ts 匹配；否则再查是否已 resolve。
  auto lk = mvcc.get_lock(key);
  if (!lk || lk->start_ts != start_ts) {
    if (mvcc.find_commit_ts(key, start_ts)) {
      gate.ok = true;
      return gate;
    }
    gate.ok = false;
    gate.error = "lock_not_found";
    return gate;
  }
  // 4) 仍是悲观锁未 prewrite → 不能 commit。
  if (lk->op == MvccOp::kPessimistic) {
    gate.ok = false;
    gate.error = "pessimistic_not_prewritten";
    return gate;
  }
  // 5) 组 kCommit → 同步 propose。
  Command cmd;
  cmd.op = CmdOp::kCommit;
  cmd.key = std::string(key);
  cmd.value = encode_commit_value(start_ts, commit_ts);
  return propose(shard_id, cmd);
}

void StoreFacade::commit_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                               uint64_t commit_ts, uint64_t expected_epoch,
                               std::function<void(ProposeResult)> cb) {
  // ---- 异步 Commit：预检同 sync；成功走 propose_async。
  if (!cb) return;
  // 1) 门闩。
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
  // 2) 幂等：已提交直接成功。
  if (auto cts = mvcc.find_commit_ts(key, start_ts)) {
    if (*cts == commit_ts || commit_ts != 0) {
      gate.ok = true;
      cb(std::move(gate));
      return;
    }
  }
  // 3) 锁校验。
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
  // 4) propose_async。
  Command cmd;
  cmd.op = CmdOp::kCommit;
  cmd.key = std::move(key);
  cmd.value = encode_commit_value(start_ts, commit_ts);
  propose_async(shard_id, cmd, std::move(cb));
}

ProposeResult StoreFacade::rollback(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                    uint64_t expected_epoch) {
  // ---- 同步 Rollback：清 lock / 记 rollback；门闩后直接 propose。
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
  // ---- 异步 Rollback。
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
  // ---- 同步悲观锁：占 lock CF（op=pessimistic），后续 prewrite 可升级。
  ProposeResult gate = require_leader(shard_id, expected_epoch);
  if (!gate.ok) return gate;
  auto* eng = node_.engines().get(shard_id);
  if (!eng) {
    gate.ok = false;
    gate.error = "no_engine";
    return gate;
  }
  MvccStore mvcc(*eng);
  // 1) 已有同事务锁 → 幂等成功；异事务 → locked。
  if (auto lk = mvcc.get_lock(key)) {
    if (lk->start_ts == start_ts) {
      gate.ok = true;
      return gate;
    }
    gate.ok = false;
    gate.error = "locked";
    return gate;
  }
  // 2) 相对 for_update_ts（或 start_ts）的 write 冲突。
  if (mvcc.has_write_conflict(key, for_update_ts != 0 ? for_update_ts : start_ts)) {
    gate.ok = false;
    gate.error = "write_conflict";
    return gate;
  }
  // 3) propose。
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
  // ---- 异步悲观锁：预检同 sync。
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
  if (ttl_ms == 0) ttl_ms = 5000;  // 默认锁 TTL
  Command cmd;
  cmd.op = CmdOp::kPessimisticLock;
  cmd.key = std::move(key);
  cmd.value = encode_pessimistic_lock_value(start_ts, for_update_ts, ttl_ms, primary);
  propose_async(shard_id, cmd, std::move(cb));
}

ProposeResult StoreFacade::pessimistic_rollback(uint32_t shard_id, std::string_view key,
                                                uint64_t start_ts, uint64_t expected_epoch) {
  // ---- 同步悲观回滚：清 pessimistic lock（value 仍用 start_ts）。
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
  // ---- 异步悲观回滚。
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
  // ---- 同步 resolve：commit_ts==0 → rollback，否则 commit（异步锁清理入口）。
  if (commit_ts == 0) {
    return rollback(shard_id, key, start_ts, expected_epoch);
  }
  return commit(shard_id, key, start_ts, commit_ts, expected_epoch);
}

void StoreFacade::resolve_lock_async(uint32_t shard_id, std::string key, uint64_t start_ts,
                                     uint64_t commit_ts, uint64_t expected_epoch,
                                     std::function<void(ProposeResult)> cb) {
  // ---- 异步 resolve：同上分支。
  if (!cb) return;
  if (commit_ts == 0) {
    rollback_async(shard_id, std::move(key), start_ts, expected_epoch, std::move(cb));
    return;
  }
  commit_async(shard_id, std::move(key), start_ts, commit_ts, expected_epoch, std::move(cb));
}

TxnGetResult StoreFacade::txn_get(uint32_t shard_id, std::string_view key, uint64_t start_ts,
                                  bool linearizable) {
  // ---- MVCC 点读：按 start_ts 可见性；遇锁返回 locked + blocking_lock。
  TxnGetResult r;
  // 1) Region / Leader（线性时）。
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
  // 2) Leader 线性：ReadIndex + apply catchup。
  if (linearizable && r.is_leader) {
    uint64_t read_ci = 0;
    if (!group->read_index(&read_ci)) {
      r.redirect_to = group->leader_address();
      r.error = "read_index_failed";
      return r;
    }
    (void)ensure_applied_catchup(node_, group, shard_id, read_ci);
  }
  // 3) MvccStore::txn_get：锁挡 → locked；否则返回可见 value + commit_ts。
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
  // ---- MVCC 范围扫：夹在 Region range 内；limit 默认 256、硬顶 4096。
  TxnScanResult r;
  // 1) Region / 线性读门闩（同 txn_get）。
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
    (void)ensure_applied_catchup(node_, group, shard_id, read_ci);
  }
  auto* eng = node_.engines().get(shard_id);
  if (!eng) return r;
  // 2) 规范化 limit。
  if (limit == 0) limit = 256;
  if (limit > 4096) limit = 4096;
  // 3) 扫引擎；truncated/next_key 供客户端续扫。
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
  // ---- 查剩余 TTL（-2 不存在/过期；-1 永久；>=0 剩余 ms）；遇锁同 txn_get。
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
    (void)ensure_applied_catchup(node_, group, shard_id, read_ci);
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
  // ---- 读 lock CF（须 Leader）：异步 resolve / 诊断用。
  GetLockResult r;
  // 1) Region + 强制 Leader。
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
  // 2) 有锁则填 LockInfo。
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
  // ---- 查 primary 事务状态（须 Leader）：Committed / Locked / RolledBack + 是否过期。
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
