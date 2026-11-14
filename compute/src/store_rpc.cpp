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

// @file store_rpc.cpp
// StoreService / AdminService 类型化出站 RPC。

#include "openkv/compute/store_rpc.hpp"

#include "openkv/codec.hpp"

#include "storage/v1/storage.pb.h"

#include <stdexcept>
#include <vector>

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

ProposeOnePcResult propose_result_from(const ::storage::v1::ProposeResponse& resp) {
  ProposeOnePcResult out;
  out.ok = resp.ok();
  out.revision = resp.revision();
  out.error = resp.error();
  out.redirect_to = resp.redirect_to();
  out.cas_failed = resp.cas_failed();
  out.current_revision = resp.current_revision();
  return out;
}

StoreGetResult store_get_result_from(const ::storage::v1::StoreGetResponse& resp) {
  StoreGetResult out;
  out.found = resp.found();
  out.value = resp.value();
  out.revision = resp.revision();
  out.is_leader = resp.is_leader();
  out.redirect_to = resp.redirect_to();
  out.locked = resp.locked();
  out.error = resp.error();
  out.lock_primary = resp.lock_primary();
  out.lock_ts = resp.lock_ts();
  return out;
}

StoreScanResult store_scan_result_from(const ::storage::v1::StoreScanResponse& resp) {
  StoreScanResult out;
  out.truncated = resp.truncated();
  out.next_key = resp.next_key();
  out.is_leader = resp.is_leader();
  out.redirect_to = resp.redirect_to();
  out.error = resp.error();
  out.pairs.reserve(static_cast<size_t>(resp.pairs_size()));
  for (const auto& p : resp.pairs()) {
    StoreScanPair sp;
    sp.key = p.key();
    sp.value = p.value();
    sp.revision = p.revision();
    out.pairs.push_back(std::move(sp));
  }
  return out;
}

StoreTtlResult store_ttl_result_from(const ::storage::v1::StoreTtlResponse& resp) {
  StoreTtlResult out;
  out.ttl_ms = resp.ttl_ms();
  out.locked = resp.locked();
  out.lock_primary = resp.lock_primary();
  out.lock_ts = resp.lock_ts();
  out.is_leader = resp.is_leader();
  out.redirect_to = resp.redirect_to();
  out.error = resp.error();
  return out;
}

StoreCheckTxnResult store_check_txn_result_from(const ::storage::v1::StoreCheckTxnResponse& resp) {
  StoreCheckTxnResult out;
  out.status = resp.status();
  out.commit_ts = resp.commit_ts();
  out.is_leader = resp.is_leader();
  out.redirect_to = resp.redirect_to();
  out.error = resp.error();
  return out;
}

::storage::v1::ProposeRequest::Op propose_op_to_proto(ProposeOp op) {
  return static_cast<::storage::v1::ProposeRequest::Op>(static_cast<int>(op));
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

void store_admin_ping(const std::string& addr, int timeout_ms) {
  auto stub = cached_admin_stub(addr);
  ::storage::v1::PingRequest req;
  ::storage::v1::PingResponse resp;
  grpc::ClientContext ctx;
  attach_deadline_and_token(&ctx, timeout_ms);
  auto st = stub->Ping(&ctx, req, &resp);
  if (!st.ok()) {
    drop_cached_channel(addr);
    throw std::runtime_error(std::string("storage Ping: ") + st.error_message());
  }
  if (resp.message() != "PONG") {
    throw std::runtime_error("storage Ping: unexpected " + resp.message());
  }
}

void store_admin_set_epoch_async(const std::string& addr, uint32_t region_id, uint64_t epoch,
                                 int timeout_ms,
                                 std::function<void(grpc::Status, std::string error)> cb) {
  if (!cb) return;
  auto stub = cached_admin_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::SetEpochRequest>();
  req->set_region_id(region_id);
  req->set_epoch(epoch);
  auto resp = std::make_shared<::storage::v1::AdminSimpleResponse>();
  stub->async()->SetEpoch(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        std::string err;
        if (st.ok() && !resp->ok()) err = resp->message();
        cb(st, std::move(err));
      });
}

// 异步调用 store_propose_onepc_async
void store_propose_onepc_async(const std::string& addr, ProposeOnePcArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposeOnePcResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ProposeRequest>();
  req->set_shard_id(args.shard_id);
  req->set_op(::storage::v1::ProposeRequest::ONEPC);
  req->set_key(std::move(args.key));
  req->set_value(std::move(args.value));
  req->set_start_ts(args.start_ts);
  req->set_commit_ts(args.commit_ts);
  req->set_primary(args.primary.empty() ? req->key() : args.primary);
  req->set_mvcc_op(args.mvcc_op);
  req->set_epoch(args.epoch);
  req->set_cas(args.cas);
  req->set_expected_revision(args.expected_revision);
  req->set_ttl_ms(args.expire_at_ms);
  auto resp = std::make_shared<::storage::v1::ProposeResponse>();

  // 异步调用 Propose
  stub->async()->Propose(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, propose_result_from(*resp));
      });
}

void store_propose_plain_async(const std::string& addr, ProposePlainArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposePlainResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ProposeRequest>();
  req->set_shard_id(args.shard_id);
  req->set_op(args.is_delete ? ::storage::v1::ProposeRequest::DELETE
                             : ::storage::v1::ProposeRequest::PUT);
  req->set_key(std::move(args.key));
  if (!args.is_delete) req->set_value(std::move(args.value));
  req->set_ttl_ms(args.ttl_ms);
  req->set_epoch(args.epoch);
  req->set_cas(args.cas);
  req->set_expected_revision(args.expected_revision);
  auto resp = std::make_shared<::storage::v1::ProposeResponse>();
  stub->async()->Propose(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, propose_result_from(*resp));
      });
}

void store_get_async(const std::string& addr, uint32_t shard_id, std::string key, bool linearizable,
                     uint64_t start_ts, int timeout_ms,
                     std::function<void(grpc::Status, StoreGetResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreGetRequest>();
  req->set_shard_id(shard_id);
  req->set_key(std::move(key));
  req->set_linearizable(linearizable);
  req->set_start_ts(start_ts);
  auto resp = std::make_shared<::storage::v1::StoreGetResponse>();
  stub->async()->Get(ctx.get(), req.get(), resp.get(),
                     [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
                       if (!st.ok()) drop_cached_channel(addr);
                       cb(st, store_get_result_from(*resp));
                     });
}

void store_propose_op_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                            std::function<void(grpc::Status, ProposeResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::ProposeRequest>();
  req->set_shard_id(args.shard_id);
  req->set_op(propose_op_to_proto(args.op));
  req->set_key(std::move(args.key));
  req->set_value(std::move(args.value));
  req->set_start_ts(args.start_ts);
  req->set_commit_ts(args.commit_ts);
  req->set_primary(args.primary.empty() ? req->key() : args.primary);
  req->set_mvcc_op(args.mvcc_op);
  req->set_epoch(args.epoch);
  req->set_cas(args.cas);
  req->set_expected_revision(args.expected_revision);
  req->set_ttl_ms(args.expire_at_ms);
  req->set_for_update_ts(args.for_update_ts);
  req->set_lock_ttl_ms(args.lock_ttl_ms);
  auto resp = std::make_shared<::storage::v1::ProposeResponse>();
  stub->async()->Propose(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, propose_result_from(*resp));
      });
}

void store_prewrite_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kPrewrite;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_commit_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                        std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kCommit;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                          std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kRollback;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_pessimistic_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                  std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kPessimisticLock;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_pessimistic_rollback_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                                      std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kPessimisticRollback;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_resolve_lock_async(const std::string& addr, ProposeTxnArgs args, int timeout_ms,
                              std::function<void(grpc::Status, ProposeResult)> cb) {
  args.op = ProposeOp::kResolveLock;
  store_propose_op_async(addr, std::move(args), timeout_ms, std::move(cb));
}

void store_scan_async(const std::string& addr, StoreScanArgs args, int timeout_ms,
                      std::function<void(grpc::Status, StoreScanResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreScanRequest>();
  req->set_shard_id(args.shard_id);
  req->set_start_key(std::move(args.start_key));
  req->set_end_key(std::move(args.end_key));
  req->set_start_after(std::move(args.start_after));
  req->set_start_ts(args.start_ts);
  req->set_limit(args.limit);
  req->set_keys_only(args.keys_only);
  req->set_linearizable(args.linearizable);
  auto resp = std::make_shared<::storage::v1::StoreScanResponse>();
  stub->async()->Scan(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, store_scan_result_from(*resp));
      });
}

void store_ttl_async(const std::string& addr, uint32_t shard_id, std::string key, uint64_t start_ts,
                     bool linearizable, int timeout_ms,
                     std::function<void(grpc::Status, StoreTtlResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreTtlRequest>();
  req->set_shard_id(shard_id);
  req->set_key(std::move(key));
  req->set_start_ts(start_ts);
  req->set_linearizable(linearizable);
  auto resp = std::make_shared<::storage::v1::StoreTtlResponse>();
  stub->async()->Ttl(ctx.get(), req.get(), resp.get(),
                     [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
                       if (!st.ok()) drop_cached_channel(addr);
                       cb(st, store_ttl_result_from(*resp));
                     });
}

void store_check_txn_status_async(const std::string& addr, uint32_t shard_id, std::string primary,
                                  uint64_t lock_ts, int timeout_ms,
                                  std::function<void(grpc::Status, StoreCheckTxnResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreCheckTxnRequest>();
  req->set_shard_id(shard_id);
  req->set_primary(std::move(primary));
  req->set_lock_ts(lock_ts);
  auto resp = std::make_shared<::storage::v1::StoreCheckTxnResponse>();
  stub->async()->CheckTxnStatus(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        cb(st, store_check_txn_result_from(*resp));
      });
}

void store_watch_head_async(const std::string& addr, uint32_t shard_id, int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchHeadResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreWatchHeadRequest>();
  req->set_shard_id(shard_id);
  auto resp = std::make_shared<::storage::v1::StoreWatchHeadResponse>();
  stub->async()->WatchHead(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        StoreWatchHeadResult out;
        out.revision = resp->revision();
        out.redirect_to = resp->redirect_to();
        out.error = resp->error();
        cb(st, std::move(out));
      });
}

void store_watch_poll_async(const std::string& addr, uint32_t shard_id, uint64_t since, int32_t limit,
                            int timeout_ms,
                            std::function<void(grpc::Status, StoreWatchPollResult)> cb) {
  if (!cb) return;
  auto stub = cached_store_stub(addr);
  auto ctx = std::make_shared<grpc::ClientContext>();
  attach_deadline_and_token(ctx.get(), timeout_ms);
  auto req = std::make_shared<::storage::v1::StoreWatchPollRequest>();
  req->set_shard_id(shard_id);
  req->set_since(since);
  req->set_limit(limit);
  auto resp = std::make_shared<::storage::v1::StoreWatchPollResponse>();
  stub->async()->WatchPoll(
      ctx.get(), req.get(), resp.get(),
      [addr, stub, ctx, req, resp, cb = std::move(cb)](grpc::Status st) {
        if (!st.ok()) drop_cached_channel(addr);
        StoreWatchPollResult out;
        out.max_revision = resp->max_revision();
        out.redirect_to = resp->redirect_to();
        out.error = resp->error();
        out.events.reserve(static_cast<size_t>(resp->events_size()));
        for (const auto& e : resp->events()) {
          StoreWatchEvent ev;
          ev.key = e.key();
          ev.value = e.value();
          ev.revision = e.revision();
          ev.deleted = e.deleted();
          out.events.push_back(std::move(ev));
        }
        cb(st, std::move(out));
      });
}

}  // namespace openkv::compute
