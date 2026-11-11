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

// @file gateway_server.hpp
// C++ Compute Gateway：KVService + DSService（CallbackService + WorkPool）。

#pragma once

#include "openkv/compute/active_ts.hpp"
#include "openkv/compute/router.hpp"
#include "openkv/compute/store_client.hpp"
#include "openkv/compute/tso.hpp"
#include "openkv/work_pool.hpp"

#include "kv/v1/ds.grpc.pb.h"
#include "kv/v1/kv.grpc.pb.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace openkv::compute {

class GatewayServer final : public kv::v1::KVService::CallbackService,
                            public kv::v1::DSService::CallbackService {
 public:
  GatewayServer(std::vector<std::string> storage_addrs, std::vector<std::string> meta_addrs);
  ~GatewayServer() override;

  GatewayServer(const GatewayServer&) = delete;
  GatewayServer& operator=(const GatewayServer&) = delete;

  void start_background();
  void stop_background();

  size_t workers() const { return pool_ ? pool_->workers() : 0; }

  /// /stats JSON（对齐 Go traffic 字段名；to_store/to_meta 本进程计数）。
  std::string stats_json() const;

  // ---- KVService (Callback) ----
  grpc::ServerUnaryReactor* Get(grpc::CallbackServerContext* ctx, const kv::v1::GetRequest* req,
                                kv::v1::GetResponse* resp) override;
  grpc::ServerUnaryReactor* Put(grpc::CallbackServerContext* ctx, const kv::v1::PutRequest* req,
                                kv::v1::PutResponse* resp) override;
  grpc::ServerUnaryReactor* Delete(grpc::CallbackServerContext* ctx,
                                   const kv::v1::DeleteRequest* req,
                                   kv::v1::DeleteResponse* resp) override;
  grpc::ServerUnaryReactor* RawGet(grpc::CallbackServerContext* ctx,
                                   const kv::v1::RawGetRequest* req,
                                   kv::v1::RawGetResponse* resp) override;
  grpc::ServerUnaryReactor* RawPut(grpc::CallbackServerContext* ctx,
                                   const kv::v1::RawPutRequest* req,
                                   kv::v1::RawPutResponse* resp) override;
  grpc::ServerUnaryReactor* RawDelete(grpc::CallbackServerContext* ctx,
                                      const kv::v1::RawDeleteRequest* req,
                                      kv::v1::RawDeleteResponse* resp) override;
  grpc::ServerUnaryReactor* BatchGet(grpc::CallbackServerContext* ctx,
                                     const kv::v1::BatchGetRequest* req,
                                     kv::v1::BatchGetResponse* resp) override;
  grpc::ServerUnaryReactor* BatchPut(grpc::CallbackServerContext* ctx,
                                     const kv::v1::BatchPutRequest* req,
                                     kv::v1::BatchPutResponse* resp) override;
  grpc::ServerUnaryReactor* Scan(grpc::CallbackServerContext* ctx, const kv::v1::ScanRequest* req,
                                 kv::v1::ScanResponse* resp) override;
  grpc::ServerUnaryReactor* TTL(grpc::CallbackServerContext* ctx, const kv::v1::TTLRequest* req,
                                kv::v1::TTLResponse* resp) override;
  grpc::ServerUnaryReactor* Expire(grpc::CallbackServerContext* ctx,
                                   const kv::v1::ExpireRequest* req,
                                   kv::v1::ExpireResponse* resp) override;
  grpc::ServerUnaryReactor* Persist(grpc::CallbackServerContext* ctx,
                                    const kv::v1::PersistRequest* req,
                                    kv::v1::PersistResponse* resp) override;
  grpc::ServerWriteReactor<kv::v1::WatchEvent>* Watch(grpc::CallbackServerContext* ctx,
                                                      const kv::v1::WatchRequest* req) override;

  grpc::ServerUnaryReactor* TxnBegin(grpc::CallbackServerContext* ctx,
                                     const kv::v1::TxnBeginRequest* req,
                                     kv::v1::TxnBeginResponse* resp) override;
  grpc::ServerUnaryReactor* TxnGet(grpc::CallbackServerContext* ctx,
                                   const kv::v1::TxnGetRequest* req,
                                   kv::v1::TxnGetResponse* resp) override;
  grpc::ServerUnaryReactor* TxnPrewrite(grpc::CallbackServerContext* ctx,
                                        const kv::v1::TxnPrewriteRequest* req,
                                        kv::v1::TxnPrewriteResponse* resp) override;
  grpc::ServerUnaryReactor* TxnCommit(grpc::CallbackServerContext* ctx,
                                      const kv::v1::TxnCommitRequest* req,
                                      kv::v1::TxnCommitResponse* resp) override;
  grpc::ServerUnaryReactor* TxnRollback(grpc::CallbackServerContext* ctx,
                                        const kv::v1::TxnRollbackRequest* req,
                                        kv::v1::TxnRollbackResponse* resp) override;
  grpc::ServerUnaryReactor* TxnPessimisticLock(grpc::CallbackServerContext* ctx,
                                               const kv::v1::TxnPessimisticLockRequest* req,
                                               kv::v1::TxnPessimisticLockResponse* resp) override;
  grpc::ServerUnaryReactor* TxnPessimisticRollback(
      grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticRollbackRequest* req,
      kv::v1::TxnPessimisticRollbackResponse* resp) override;
  grpc::ServerUnaryReactor* TxnResolveLock(grpc::CallbackServerContext* ctx,
                                           const kv::v1::TxnResolveLockRequest* req,
                                           kv::v1::TxnResolveLockResponse* resp) override;
  grpc::ServerUnaryReactor* TxnCheckStatus(grpc::CallbackServerContext* ctx,
                                           const kv::v1::TxnCheckStatusRequest* req,
                                           kv::v1::TxnCheckStatusResponse* resp) override;

  // ---- DSService (Callback) ----
  grpc::ServerUnaryReactor* HSet(grpc::CallbackServerContext*, const kv::v1::HSetRequest*,
                                 kv::v1::HSetResponse*) override;
  grpc::ServerUnaryReactor* HGet(grpc::CallbackServerContext*, const kv::v1::HGetRequest*,
                                 kv::v1::HGetResponse*) override;
  grpc::ServerUnaryReactor* HDel(grpc::CallbackServerContext*, const kv::v1::HDelRequest*,
                                 kv::v1::HDelResponse*) override;
  grpc::ServerUnaryReactor* HExists(grpc::CallbackServerContext*, const kv::v1::HExistsRequest*,
                                    kv::v1::HExistsResponse*) override;
  grpc::ServerUnaryReactor* HLen(grpc::CallbackServerContext*, const kv::v1::HLenRequest*,
                                 kv::v1::HLenResponse*) override;
  grpc::ServerUnaryReactor* HGetAll(grpc::CallbackServerContext*, const kv::v1::HGetAllRequest*,
                                    kv::v1::HGetAllResponse*) override;
  grpc::ServerUnaryReactor* HKeys(grpc::CallbackServerContext*, const kv::v1::HKeysRequest*,
                                  kv::v1::HKeysResponse*) override;
  grpc::ServerUnaryReactor* HMSet(grpc::CallbackServerContext*, const kv::v1::HMSetRequest*,
                                  kv::v1::HMSetResponse*) override;
  grpc::ServerUnaryReactor* HMGet(grpc::CallbackServerContext*, const kv::v1::HMGetRequest*,
                                  kv::v1::HMGetResponse*) override;

  grpc::ServerUnaryReactor* SAdd(grpc::CallbackServerContext*, const kv::v1::SAddRequest*,
                                 kv::v1::SAddResponse*) override;
  grpc::ServerUnaryReactor* SRem(grpc::CallbackServerContext*, const kv::v1::SRemRequest*,
                                 kv::v1::SRemResponse*) override;
  grpc::ServerUnaryReactor* SIsMember(grpc::CallbackServerContext*,
                                      const kv::v1::SIsMemberRequest*,
                                      kv::v1::SIsMemberResponse*) override;
  grpc::ServerUnaryReactor* SCard(grpc::CallbackServerContext*, const kv::v1::SCardRequest*,
                                  kv::v1::SCardResponse*) override;
  grpc::ServerUnaryReactor* SMembers(grpc::CallbackServerContext*, const kv::v1::SMembersRequest*,
                                     kv::v1::SMembersResponse*) override;

  grpc::ServerUnaryReactor* LPush(grpc::CallbackServerContext*, const kv::v1::LPushRequest*,
                                  kv::v1::LPushResponse*) override;
  grpc::ServerUnaryReactor* RPush(grpc::CallbackServerContext*, const kv::v1::RPushRequest*,
                                  kv::v1::RPushResponse*) override;
  grpc::ServerUnaryReactor* LPop(grpc::CallbackServerContext*, const kv::v1::LPopRequest*,
                                 kv::v1::LPopResponse*) override;
  grpc::ServerUnaryReactor* RPop(grpc::CallbackServerContext*, const kv::v1::RPopRequest*,
                                 kv::v1::RPopResponse*) override;
  grpc::ServerUnaryReactor* LLen(grpc::CallbackServerContext*, const kv::v1::LLenRequest*,
                                 kv::v1::LLenResponse*) override;
  grpc::ServerUnaryReactor* LRange(grpc::CallbackServerContext*, const kv::v1::LRangeRequest*,
                                   kv::v1::LRangeResponse*) override;
  grpc::ServerUnaryReactor* LIndex(grpc::CallbackServerContext*, const kv::v1::LIndexRequest*,
                                   kv::v1::LIndexResponse*) override;

  grpc::ServerUnaryReactor* ZAdd(grpc::CallbackServerContext*, const kv::v1::ZAddRequest*,
                                 kv::v1::ZAddResponse*) override;
  grpc::ServerUnaryReactor* ZRem(grpc::CallbackServerContext*, const kv::v1::ZRemRequest*,
                                 kv::v1::ZRemResponse*) override;
  grpc::ServerUnaryReactor* ZScore(grpc::CallbackServerContext*, const kv::v1::ZScoreRequest*,
                                   kv::v1::ZScoreResponse*) override;
  grpc::ServerUnaryReactor* ZCard(grpc::CallbackServerContext*, const kv::v1::ZCardRequest*,
                                  kv::v1::ZCardResponse*) override;
  grpc::ServerUnaryReactor* ZRange(grpc::CallbackServerContext*, const kv::v1::ZRangeRequest*,
                                   kv::v1::ZRangeResponse*) override;

 private:
  using LeaderFn = std::function<void(StoreClient&)>;
  using LeaderEpochFn = std::function<void(StoreClient&, uint64_t epoch)>;

  /// 接收线程只投递：worker 跑 fn 后 Finish。
  grpc::ServerUnaryReactor* defer_unary(grpc::CallbackServerContext* ctx,
                                        std::function<grpc::Status()> fn);

  void note_client_rpc();
  void note_to_store();
  void note_to_store(const std::string& peer_addr);
  void note_to_meta();
  void note_to_meta(const std::string& peer_addr);

  uint64_t alloc_ts(uint32_t n);
  uint32_t region_of(std::string_view key) const;
  uint64_t epoch_of(uint32_t region) const;
  StoreClient* client(const std::string& addr);
  std::vector<StoreClient*> try_order(uint32_t region);
  void remember_leader(uint32_t region, const std::string& addr);
  void clear_leader_hint(uint32_t region);
  void refresh_routes();
  void sync_epochs_async(const RoutingTable& t);
  void route_watch_loop();

  void with_leader(uint32_t region, const LeaderFn& fn);
  void with_leader_epoch(uint32_t region, const LeaderEpochFn& fn);

  void resolve_blocking_lock(const std::string& key, const LockedError& le);
  kv::v1::TxnCommitResponse commit_with_ts(uint64_t start_ts, uint64_t commit_ts,
                                           const std::string& primary,
                                           const std::vector<std::string>& keys);
  void async_resolve_secondaries(uint64_t start_ts, uint64_t commit_ts,
                                 std::vector<std::string> keys);

  // 热路径：本地 TSO 批缓存 alloc_async → store_propose_onepc_async
  void put_async(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp,
                 grpc::ServerUnaryReactor* reactor, int attempt);
  /// Batch 等：完成时调 done（不持有 reactor）。
  void put_async_cb(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp, int attempt,
                    std::function<void(grpc::Status)> done);
  void delete_async(const kv::v1::DeleteRequest* req, kv::v1::DeleteResponse* resp,
                    grpc::ServerUnaryReactor* reactor, int attempt);
  void get_async(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                 grpc::ServerUnaryReactor* reactor, int attempt);
  void get_async_cb(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp, int attempt,
                    std::function<void(grpc::Status)> done);
  // 无事务：Propose PUT/DELETE + Store Get（无 TSO）
  void raw_put_async(const kv::v1::RawPutRequest* req, kv::v1::RawPutResponse* resp,
                     grpc::ServerUnaryReactor* reactor, int attempt);
  void raw_delete_async(const kv::v1::RawDeleteRequest* req, kv::v1::RawDeleteResponse* resp,
                        grpc::ServerUnaryReactor* reactor, int attempt);
  void raw_get_async(const kv::v1::RawGetRequest* req, kv::v1::RawGetResponse* resp,
                     grpc::ServerUnaryReactor* reactor, int attempt);
  /// 本地 alloc_async(2)；失败走 on_err。
  void with_start_commit_ts(std::function<void(grpc::Status)> on_err,
                            std::function<void(uint64_t start, uint64_t commit_ts)> cont);
  void batch_put_async(const kv::v1::BatchPutRequest* req, kv::v1::BatchPutResponse* resp,
                       grpc::ServerUnaryReactor* reactor);
  void batch_get_async(const kv::v1::BatchGetRequest* req, kv::v1::BatchGetResponse* resp,
                       grpc::ServerUnaryReactor* reactor);
  /// not_leader / locked / splitting / epoch… → 刷路由或记 leader 后重试。
  bool retry_async_write(std::string_view body, const std::string& key, uint32_t region,
                         bool retry_write_conflict, const std::function<void()>& retry);

  // 同步业务体（WorkPool / 内部复用）
  grpc::Status GetBody(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp);
  grpc::Status PutBody(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp);
  grpc::Status DeleteBody(const kv::v1::DeleteRequest* req, kv::v1::DeleteResponse* resp);
  grpc::Status BatchGetBody(const kv::v1::BatchGetRequest* req, kv::v1::BatchGetResponse* resp);
  grpc::Status BatchPutBody(const kv::v1::BatchPutRequest* req, kv::v1::BatchPutResponse* resp);
  grpc::Status ScanBody(const kv::v1::ScanRequest* req, kv::v1::ScanResponse* resp);
  grpc::Status TTLBody(const kv::v1::TTLRequest* req, kv::v1::TTLResponse* resp);
  grpc::Status ExpireBody(const kv::v1::ExpireRequest* req, kv::v1::ExpireResponse* resp);
  grpc::Status PersistBody(const kv::v1::PersistRequest* req, kv::v1::PersistResponse* resp);
  void WatchBody(grpc::CallbackServerContext* ctx, const kv::v1::WatchRequest& req,
                 const std::function<bool(const kv::v1::WatchEvent&)>& write_ev);

  grpc::Status TxnBeginBody(const kv::v1::TxnBeginRequest* req, kv::v1::TxnBeginResponse* resp);
  grpc::Status TxnGetBody(const kv::v1::TxnGetRequest* req, kv::v1::TxnGetResponse* resp);
  grpc::Status TxnPrewriteBody(const kv::v1::TxnPrewriteRequest* req,
                               kv::v1::TxnPrewriteResponse* resp);
  grpc::Status TxnCommitBody(const kv::v1::TxnCommitRequest* req, kv::v1::TxnCommitResponse* resp);
  grpc::Status TxnRollbackBody(const kv::v1::TxnRollbackRequest* req,
                               kv::v1::TxnRollbackResponse* resp);
  grpc::Status TxnPessimisticLockBody(const kv::v1::TxnPessimisticLockRequest* req,
                                      kv::v1::TxnPessimisticLockResponse* resp);
  grpc::Status TxnPessimisticRollbackBody(const kv::v1::TxnPessimisticRollbackRequest* req,
                                          kv::v1::TxnPessimisticRollbackResponse* resp);
  grpc::Status TxnResolveLockBody(const kv::v1::TxnResolveLockRequest* req,
                                  kv::v1::TxnResolveLockResponse* resp);
  grpc::Status TxnCheckStatusBody(const kv::v1::TxnCheckStatusRequest* req,
                                  kv::v1::TxnCheckStatusResponse* resp);

  grpc::Status HSetBody(const kv::v1::HSetRequest*, kv::v1::HSetResponse*);
  grpc::Status HGetBody(const kv::v1::HGetRequest*, kv::v1::HGetResponse*);
  grpc::Status HDelBody(const kv::v1::HDelRequest*, kv::v1::HDelResponse*);
  grpc::Status HExistsBody(const kv::v1::HExistsRequest*, kv::v1::HExistsResponse*);
  grpc::Status HLenBody(const kv::v1::HLenRequest*, kv::v1::HLenResponse*);
  grpc::Status HGetAllBody(const kv::v1::HGetAllRequest*, kv::v1::HGetAllResponse*);
  grpc::Status HKeysBody(const kv::v1::HKeysRequest*, kv::v1::HKeysResponse*);
  grpc::Status HMSetBody(const kv::v1::HMSetRequest*, kv::v1::HMSetResponse*);
  grpc::Status HMGetBody(const kv::v1::HMGetRequest*, kv::v1::HMGetResponse*);
  grpc::Status SAddBody(const kv::v1::SAddRequest*, kv::v1::SAddResponse*);
  grpc::Status SRemBody(const kv::v1::SRemRequest*, kv::v1::SRemResponse*);
  grpc::Status SIsMemberBody(const kv::v1::SIsMemberRequest*, kv::v1::SIsMemberResponse*);
  grpc::Status SCardBody(const kv::v1::SCardRequest*, kv::v1::SCardResponse*);
  grpc::Status SMembersBody(const kv::v1::SMembersRequest*, kv::v1::SMembersResponse*);
  grpc::Status LPushBody(const kv::v1::LPushRequest*, kv::v1::LPushResponse*);
  grpc::Status RPushBody(const kv::v1::RPushRequest*, kv::v1::RPushResponse*);
  grpc::Status LPopBody(const kv::v1::LPopRequest*, kv::v1::LPopResponse*);
  grpc::Status RPopBody(const kv::v1::RPopRequest*, kv::v1::RPopResponse*);
  grpc::Status LLenBody(const kv::v1::LLenRequest*, kv::v1::LLenResponse*);
  grpc::Status LRangeBody(const kv::v1::LRangeRequest*, kv::v1::LRangeResponse*);
  grpc::Status LIndexBody(const kv::v1::LIndexRequest*, kv::v1::LIndexResponse*);
  grpc::Status ZAddBody(const kv::v1::ZAddRequest*, kv::v1::ZAddResponse*);
  grpc::Status ZRemBody(const kv::v1::ZRemRequest*, kv::v1::ZRemResponse*);
  grpc::Status ZScoreBody(const kv::v1::ZScoreRequest*, kv::v1::ZScoreResponse*);
  grpc::Status ZCardBody(const kv::v1::ZCardRequest*, kv::v1::ZCardResponse*);
  grpc::Status ZRangeBody(const kv::v1::ZRangeRequest*, kv::v1::ZRangeResponse*);

  grpc::Status get_stale(const std::string& key, kv::v1::GetResponse* resp);
  grpc::Status ttl_stale(const std::string& key, kv::v1::TTLResponse* resp);
  grpc::Status get_for_rewrite(const std::string& key, std::string* value, bool* found,
                               uint64_t* rev);

  TxnScanResult scan_one_region(uint32_t region_id, std::string_view start, std::string_view end,
                                std::string_view after, uint64_t start_ts, uint32_t limit,
                                bool keys_only, bool linearizable);

  bool match_watch(const kv::v1::WatchRequest& req, std::string_view key) const;
  std::vector<uint32_t> regions_for_watch(const kv::v1::WatchRequest& req);

  grpc::Status ds_load(const std::string& mk, std::string* blob, bool* found);
  grpc::Status ds_store(const std::string& mk, const std::string& blob);
  grpc::Status list_push(const std::string& key, const std::vector<std::string>& values, bool left,
                         int64_t* length);
  grpc::Status list_pop(const std::string& key, bool left, std::string* value, bool* found);

  std::vector<std::string> meta_addrs_;
  std::vector<std::shared_ptr<StoreClient>> stores_;
  std::unordered_map<std::string, std::shared_ptr<StoreClient>> by_addr_;
  TSOracle ts_;
  Router routes_;
  ActiveTsTracker active_ts_;
  std::unique_ptr<openkv::WorkPool> pool_;

  mutable std::mutex mu_;
  std::unordered_map<uint32_t, std::string> leader_hint_;

  std::atomic<bool> bg_stop_{false};
  std::thread bg_thread_;

  mutable std::atomic<uint64_t> client_in_{0};
  mutable std::atomic<uint64_t> to_store_{0};
  mutable std::atomic<uint64_t> to_meta_{0};
  mutable std::mutex peer_mu_;
  mutable std::unordered_map<std::string, uint64_t> to_store_peers_;
  mutable std::unordered_map<std::string, uint64_t> to_meta_peers_;
};

bool store_skip_err(const std::string& msg);
uint64_t parse_epoch_expected(const std::string& msg);
uint64_t parse_cas_current_rev(const std::string& msg);
std::string prefix_upper_bound(std::string_view prefix);

}  // namespace openkv::compute
