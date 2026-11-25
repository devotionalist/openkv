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
// C++ Compute Gateway：KVService + DSService（CallbackService）。
//
// 生产入口：gateway_node → 本类。路径矩阵见 docs/flows/29-compute-code-map.md。
//
// 约定（CQ 线程禁止同步 Meta/Storage RPC）：
//   Txn/Scan/TTL/Watch/DS/Put/Get/Raw/Batch 一律 CQ 回调 + StoreService async。
//   WorkPool 仅 Watch（间隔 sleep / write_ev）；热路径 Put/Get 不经池。

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv/v1/ds.grpc.pb.h"
#include "kv/v1/kv.grpc.pb.h"
#include "openkv/compute/active_ts.hpp"
#include "openkv/compute/router.hpp"
#include "openkv/compute/store_rpc.hpp"
#include "openkv/compute/tso.hpp"
#include "openkv/work_pool.hpp"

namespace openkv::compute {

/* Compute Gateway：KVService + DSService；CQ 回调驱动，出站走异步 Store/Meta。 */
class GatewayServer final : public kv::v1::KVService::CallbackService,
                            public kv::v1::DSService::CallbackService {
 public:
  // ---- Lifecycle ----

  /**
   * 构造：绑定 Storage/Meta 地址列表，建 TSO、Router、WorkPool。
   * @param storage_addrs Storage 地址列表（bootstrap/探测）
   * @param meta_addrs Meta 地址列表（可 failover）
   */
  GatewayServer(std::vector<std::string> storage_addrs, std::vector<std::string> meta_addrs);
  /**
   * 析构：stop_background 并销毁 WorkPool。
   */
  ~GatewayServer() override;

  GatewayServer(const GatewayServer&) = delete;
  GatewayServer& operator=(const GatewayServer&) = delete;

  /**
   * 启动后台线程 route_watch_loop（刷路由 + report_min_start_ts）。
   */
  void start_background();
  /**
   * 请求停止后台线程并 join。
   */
  void stop_background();

  /**
   * WorkPool 工作线程数（0 = 池未建）。
   */
  size_t workers() const { return pool_ ? pool_->workers() : 0; }

  /**
   * 导出 /stats JSON：client/gw/to_store/to_meta 及 to_*_peers（总览连线）。
   */
  std::string stats_json() const;

  // ---- KVService 入口：CQ 上启动 *_async ----

  /**
   * 点读：线性 get_async；stale 走 store_get_async(linearizable=false)。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Get(grpc::CallbackServerContext* ctx, const kv::v1::GetRequest* req,
                                kv::v1::GetResponse* resp) override;
  /**
   * 事务 1PC 写：CQ → put_async（alloc_async + Propose ONEPC）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Put(grpc::CallbackServerContext* ctx, const kv::v1::PutRequest* req,
                                kv::v1::PutResponse* resp) override;
  /**
   * 事务 1PC 删：CQ → delete_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Delete(grpc::CallbackServerContext* ctx,
                                   const kv::v1::DeleteRequest* req,
                                   kv::v1::DeleteResponse* resp) override;
  /**
   * 无事务点读：CQ → raw_get_async（start_ts=0 plain Get）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* RawGet(grpc::CallbackServerContext* ctx,
                                   const kv::v1::RawGetRequest* req,
                                   kv::v1::RawGetResponse* resp) override;
  /**
   * 无事务写：CQ → raw_put_async（Propose PUT，无 TSO）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* RawPut(grpc::CallbackServerContext* ctx,
                                   const kv::v1::RawPutRequest* req,
                                   kv::v1::RawPutResponse* resp) override;
  /**
   * 无事务删：CQ → raw_delete_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* RawDelete(grpc::CallbackServerContext* ctx,
                                      const kv::v1::RawDeleteRequest* req,
                                      kv::v1::RawDeleteResponse* resp) override;
  /**
   * 批量点读：CQ → batch_get_async（扇出 get_async_cb，fan-in ≤16）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* BatchGet(grpc::CallbackServerContext* ctx,
                                     const kv::v1::BatchGetRequest* req,
                                     kv::v1::BatchGetResponse* resp) override;
  /**
   * 批量写：CQ → batch_put_async（扇出 put_async_cb）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* BatchPut(grpc::CallbackServerContext* ctx,
                                     const kv::v1::BatchPutRequest* req,
                                     kv::v1::BatchPutResponse* resp) override;
  /**
   * 范围扫：CQ → scan_async（跨 Region store_scan_async）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Scan(grpc::CallbackServerContext* ctx, const kv::v1::ScanRequest* req,
                                 kv::v1::ScanResponse* resp) override;
  /**
   * 查剩余 TTL：CQ → ttl_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TTL(grpc::CallbackServerContext* ctx, const kv::v1::TTLRequest* req,
                                kv::v1::TTLResponse* resp) override;
  /**
   * 设置/刷新过期：CQ → expire_async（get_async_cb + put_async_cb RMW）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Expire(grpc::CallbackServerContext* ctx,
                                   const kv::v1::ExpireRequest* req,
                                   kv::v1::ExpireResponse* resp) override;
  /**
   * 去掉过期：CQ → persist_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* Persist(grpc::CallbackServerContext* ctx,
                                    const kv::v1::PersistRequest* req,
                                    kv::v1::PersistResponse* resp) override;
  /**
   * 变更订阅：WriteReactor + store_watch_*_async（间隔 sleep 可走 WorkPool）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   */
  grpc::ServerWriteReactor<kv::v1::WatchEvent>* Watch(grpc::CallbackServerContext* ctx,
                                                      const kv::v1::WatchRequest* req) override;

  /**
   * 申请 start_ts：CQ → txn_begin_async（alloc_async）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnBegin(grpc::CallbackServerContext* ctx,
                                     const kv::v1::TxnBeginRequest* req,
                                     kv::v1::TxnBeginResponse* resp) override;
  /**
   * 事务内点读：CQ → txn_get_async（store_get_async @ start_ts）。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnGet(grpc::CallbackServerContext* ctx,
                                   const kv::v1::TxnGetRequest* req,
                                   kv::v1::TxnGetResponse* resp) override;
  /**
   * Prewrite：CQ → txn_prewrite_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnPrewrite(grpc::CallbackServerContext* ctx,
                                        const kv::v1::TxnPrewriteRequest* req,
                                        kv::v1::TxnPrewriteResponse* resp) override;
  /**
   * 提交：CQ → txn_commit_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnCommit(grpc::CallbackServerContext* ctx,
                                      const kv::v1::TxnCommitRequest* req,
                                      kv::v1::TxnCommitResponse* resp) override;
  /**
   * 回滚：CQ → txn_rollback_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnRollback(grpc::CallbackServerContext* ctx,
                                        const kv::v1::TxnRollbackRequest* req,
                                        kv::v1::TxnRollbackResponse* resp) override;
  /**
   * 悲观锁：CQ → txn_pessimistic_lock_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnPessimisticLock(grpc::CallbackServerContext* ctx,
                                               const kv::v1::TxnPessimisticLockRequest* req,
                                               kv::v1::TxnPessimisticLockResponse* resp) override;
  /**
   * 释放悲观锁：CQ → txn_pessimistic_rollback_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnPessimisticRollback(
      grpc::CallbackServerContext* ctx, const kv::v1::TxnPessimisticRollbackRequest* req,
      kv::v1::TxnPessimisticRollbackResponse* resp) override;
  /**
   * ResolveLock：CQ → txn_resolve_lock_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnResolveLock(grpc::CallbackServerContext* ctx,
                                           const kv::v1::TxnResolveLockRequest* req,
                                           kv::v1::TxnResolveLockResponse* resp) override;
  /**
   * CheckStatus：CQ → txn_check_status_async。
   * @param ctx 服务端 CallbackServerContext
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   */
  grpc::ServerUnaryReactor* TxnCheckStatus(grpc::CallbackServerContext* ctx,
                                           const kv::v1::TxnCheckStatusRequest* req,
                                           kv::v1::TxnCheckStatusResponse* resp) override;

  // ---- DSService 入口：CQ → *_async（ds_load/ds_store 经 get/put_async）----

  /**
   * Hash 设字段。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HSet(grpc::CallbackServerContext*, const kv::v1::HSetRequest*,
                                 kv::v1::HSetResponse*) override;
  /**
   * Hash 取字段。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HGet(grpc::CallbackServerContext*, const kv::v1::HGetRequest*,
                                 kv::v1::HGetResponse*) override;
  /**
   * Hash 删字段。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HDel(grpc::CallbackServerContext*, const kv::v1::HDelRequest*,
                                 kv::v1::HDelResponse*) override;
  /**
   * Hash 字段是否存在。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HExists(grpc::CallbackServerContext*, const kv::v1::HExistsRequest*,
                                    kv::v1::HExistsResponse*) override;
  /**
   * Hash 字段数。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HLen(grpc::CallbackServerContext*, const kv::v1::HLenRequest*,
                                 kv::v1::HLenResponse*) override;
  /**
   * Hash 全量 field→value。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HGetAll(grpc::CallbackServerContext*, const kv::v1::HGetAllRequest*,
                                    kv::v1::HGetAllResponse*) override;
  /**
   * Hash 全部 field 名。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HKeys(grpc::CallbackServerContext*, const kv::v1::HKeysRequest*,
                                  kv::v1::HKeysResponse*) override;
  /**
   * Hash 批量设字段。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HMSet(grpc::CallbackServerContext*, const kv::v1::HMSetRequest*,
                                  kv::v1::HMSetResponse*) override;
  /**
   * Hash 批量取字段。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* HMGet(grpc::CallbackServerContext*, const kv::v1::HMGetRequest*,
                                  kv::v1::HMGetResponse*) override;

  /**
   * Set 加成员。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* SAdd(grpc::CallbackServerContext*, const kv::v1::SAddRequest*,
                                 kv::v1::SAddResponse*) override;
  /**
   * Set 删成员。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* SRem(grpc::CallbackServerContext*, const kv::v1::SRemRequest*,
                                 kv::v1::SRemResponse*) override;
  /**
   * Set 成员是否存在。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* SIsMember(grpc::CallbackServerContext*, const kv::v1::SIsMemberRequest*,
                                      kv::v1::SIsMemberResponse*) override;
  /**
   * Set 基数。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* SCard(grpc::CallbackServerContext*, const kv::v1::SCardRequest*,
                                  kv::v1::SCardResponse*) override;
  /**
   * Set 全部成员。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* SMembers(grpc::CallbackServerContext*, const kv::v1::SMembersRequest*,
                                     kv::v1::SMembersResponse*) override;

  /**
   * List 左侧推入。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* LPush(grpc::CallbackServerContext*, const kv::v1::LPushRequest*,
                                  kv::v1::LPushResponse*) override;
  /**
   * List 右侧推入。
   */
  grpc::ServerUnaryReactor* RPush(grpc::CallbackServerContext*, const kv::v1::RPushRequest*,
                                  kv::v1::RPushResponse*) override;
  /**
   * List 左侧弹出。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* LPop(grpc::CallbackServerContext*, const kv::v1::LPopRequest*,
                                 kv::v1::LPopResponse*) override;
  /**
   * List 右侧弹出。
   */
  grpc::ServerUnaryReactor* RPop(grpc::CallbackServerContext*, const kv::v1::RPopRequest*,
                                 kv::v1::RPopResponse*) override;
  /**
   * List 长度。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* LLen(grpc::CallbackServerContext*, const kv::v1::LLenRequest*,
                                 kv::v1::LLenResponse*) override;
  /**
   * List 下标区间。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* LRange(grpc::CallbackServerContext*, const kv::v1::LRangeRequest*,
                                   kv::v1::LRangeResponse*) override;
  /**
   * List 按下标取值。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* LIndex(grpc::CallbackServerContext*, const kv::v1::LIndexRequest*,
                                   kv::v1::LIndexResponse*) override;

  /**
   * ZSet 加/更新成员分。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* ZAdd(grpc::CallbackServerContext*, const kv::v1::ZAddRequest*,
                                 kv::v1::ZAddResponse*) override;
  /**
   * ZSet 删成员。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* ZRem(grpc::CallbackServerContext*, const kv::v1::ZRemRequest*,
                                 kv::v1::ZRemResponse*) override;
  /**
   * ZSet 查成员分。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* ZScore(grpc::CallbackServerContext*, const kv::v1::ZScoreRequest*,
                                   kv::v1::ZScoreResponse*) override;
  /**
   * ZSet 基数。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* ZCard(grpc::CallbackServerContext*, const kv::v1::ZCardRequest*,
                                  kv::v1::ZCardResponse*) override;
  /**
   * ZSet 按排名区间。
   * @param ctx 服务端 CallbackServerContext（未使用可忽略）
   * @param req 入站请求
   * @param resp 出站响应
   * @return Unary reactor
   */
  grpc::ServerUnaryReactor* ZRange(grpc::CallbackServerContext*, const kv::v1::ZRangeRequest*,
                                   kv::v1::ZRangeResponse*) override;

 private:
  using StoreAsyncRpcFn = std::function<void(
      const std::string& addr,
      std::function<void(grpc::Status sst, bool ok, std::string error, std::string redirect_to)>
          on_rpc)>;
  using StoreAsyncDoneFn = std::function<void(grpc::Status sst, bool ok, std::string error,
                                              std::string redirect_to, std::string used_addr)>;

  // ---- Stats ----

  /**
   * 客户端入站 RPC +1（client_in_）。
   */
  void note_client_rpc();
  /**
   * 出站 Storage 总量 +1（无 peer 明细）。
   */
  void note_to_store();
  /**
   * 出站 Storage +1，并记入 to_store_peers_[peer]（总览连线）。
   * @param peer_addr 对端 Storage 地址
   */
  void note_to_store(const std::string& peer_addr);
  /**
   * 出站 Meta 总量 +1。
   */
  void note_to_meta();
  /**
   * 出站 Meta +1，并记入 to_meta_peers_[peer]。
   * @param peer_addr 对端 Storage 地址
   */
  void note_to_meta(const std::string& peer_addr);

  // ---- Routing / Leader ----

  /**
   * key → region_id；未命中依赖 Router（可能为 0）。Propose 字段名仍叫 shard_id。
   * @param key 键
   */
  uint32_t region_of(std::string_view key) const;
  /**
   * 本地缓存的 Region conf epoch。
   * @param region 命中的 Region id
   */
  uint64_t epoch_of(uint32_t region) const;
  /**
   * 尝试顺序：leader_hint 优先，再 Router.replica_addrs，再 boot storage_addrs。
   * @param region 命中的 Region id
   */
  std::vector<std::string> try_order(uint32_t region);
  /**
   * 挑下一个可拨地址；skip 里是本轮已失败/已跳过的副本。
   */
  std::optional<std::string> pick_leader_addr(uint32_t region,
                                              const std::unordered_set<std::string>& skip = {});
  /**
   * 记住 not_leader 后的真实 leader 地址。
   * @param region 命中的 Region id
   * @param addr 对端地址 host:port
   */
  void remember_leader(uint32_t region, const std::string& addr);
  /**
   * 清掉该 Region 的 leader_hint（路由刷新后）。
   * @param region 命中的 Region id
   */
  void clear_leader_hint(uint32_t region);
  /**
   * 同步拉 Meta GetRoutingTable 并 update Router，再 sync_epochs_async。
   */
  void refresh_routes();
  /**
   * detach 线程向各 Store 推 AdminService.SetEpoch；无 shutdown 汇合（已知粗糙点）。
   * @param t 本地事务句柄
   */
  void sync_epochs_async(const RoutingTable& t);
  /**
   * 后台循环：WatchRoutingTable + 定期 report_min_start_ts。
   */
  void route_watch_loop();

  /**
   * CQ 安全：挑副本 → make_call。
   * 传输失败（store_skip_err）/ not_leader / redirect / splitting：换副本重试（≤4）。
   * @param region 命中的 Region id
   * @param attempt 当前重试序号（从 0）
   * @param make_call 构造并发起一次出站调用
   * @param on_done 全部完成或最终失败回调
   */
  void with_store_async(uint32_t region, int attempt, StoreAsyncRpcFn make_call,
                        StoreAsyncDoneFn on_done);
  /**
   * 同上，带本轮已跳过地址集合（递归重试时累积）。
   * @param region 命中的 Region id
   * @param attempt 当前重试序号（从 0）
   * @param make_call 构造并发起一次出站调用
   * @param on_done 全部完成或最终失败回调
   * @param skipped 已跳过的地址集合
   */
  void with_store_async(uint32_t region, int attempt, StoreAsyncRpcFn make_call,
                        StoreAsyncDoneFn on_done,
                        std::shared_ptr<std::unordered_set<std::string>> skipped);

  // ---- Txn helpers（gateway_txn.cpp）----

  /**
   * 异步 resolve 链：CheckTxnStatus → ResolveLock；完成后 cont。
   * @param key 键
   * @param le LockedError
   * @param cont 继续后续步骤的回调
   */
  void resolve_blocking_lock_async(const std::string& key, const LockedError& le,
                                   std::function<void(grpc::Status)> cont);
  /**
   * detach：对 secondaries 做 best-effort resolve_lock；无与 stop_background 汇合。
   * @param start_ts 事务/快照 start_ts
   * @param commit_ts 提交时间戳
   * @param keys 涉及的键列表
   */
  void async_resolve_secondaries(uint64_t start_ts, uint64_t commit_ts,
                                 std::vector<std::string> keys);

  // ---- Async KV（CQ 安全）----

  /**
   * Put 异步入口：包装 put_async_cb，完成后 reactor->Finish。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void put_async(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp,
                 grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * Put 异步核心：TSO → Propose ONEPC；失败走 retry_async_write。Batch 复用本函数。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param attempt 当前重试序号（从 0）
   * @param done 是否最后一片 / 完成回调
   */
  void put_async_cb(const kv::v1::PutRequest* req, kv::v1::PutResponse* resp, int attempt,
                    std::function<void(grpc::Status)> done);
  /**
   * Delete 异步：ONEPC Delete op。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void delete_async(const kv::v1::DeleteRequest* req, kv::v1::DeleteResponse* resp,
                    grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * Get 异步入口：包装 get_async_cb。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void get_async(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp,
                 grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * Get 异步核心：alloc_async → store_get_async(start_ts)；Batch 复用。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param attempt 当前重试序号（从 0）
   * @param done 是否最后一片 / 完成回调
   */
  void get_async_cb(const kv::v1::GetRequest* req, kv::v1::GetResponse* resp, int attempt,
                    std::function<void(grpc::Status)> done);
  /**
   * RawPut 异步：Propose plain PUT（无 TSO）。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void raw_put_async(const kv::v1::RawPutRequest* req, kv::v1::RawPutResponse* resp,
                     grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * RawDelete 异步：Propose plain DELETE。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void raw_delete_async(const kv::v1::RawDeleteRequest* req, kv::v1::RawDeleteResponse* resp,
                        grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * RawGet 异步：store_get_async(start_ts=0)。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   * @param attempt 当前重试序号（从 0）
   */
  void raw_get_async(const kv::v1::RawGetRequest* req, kv::v1::RawGetResponse* resp,
                     grpc::ServerUnaryReactor* reactor, int attempt);
  /**
   * alloc_async(2) → cont(start, start+1)；失败调 on_err。
   * @param on_err 错误回调
   * @param cont 继续后续步骤的回调
   */
  void with_start_commit_ts(std::function<void(grpc::Status)> on_err,
                            std::function<void(uint64_t start, uint64_t commit_ts)> cont);
  /**
   * BatchPut 扇出 put_async_cb，全部完成后 Finish。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   */
  void batch_put_async(const kv::v1::BatchPutRequest* req, kv::v1::BatchPutResponse* resp,
                       grpc::ServerUnaryReactor* reactor);
  /**
   * BatchGet 扇出 get_async_cb，全部完成后 Finish。
   * @param req 入站 protobuf 请求
   * @param resp 出站 protobuf 响应
   * @param reactor gRPC 异步 reactor
   */
  void batch_get_async(const kv::v1::BatchGetRequest* req, kv::v1::BatchGetResponse* resp,
                       grpc::ServerUnaryReactor* reactor);
  /**
   * 解析 Propose/Get 错误子串；可重试则调 retry 并返回 true（attempt 封顶）。
   * @param body 响应/请求正文
   * @param key 键
   * @param region 命中的 Region id
   * @param retry_write_conflict 写冲突是否再试
   * @param retry 重试回调
   */
  bool retry_async_write(std::string_view body, const std::string& key, uint32_t region,
                         bool retry_write_conflict, const std::function<void()>& retry);

  /**
   * 异步 Scan（可跨 Region 拼页）。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void scan_async(const kv::v1::ScanRequest* req, kv::v1::ScanResponse* resp,
                  std::function<void(grpc::Status)> done);
  /**
   * 异步 TTL；attempt 用于换副本重试。
   * @param req 入站请求
   * @param resp 出站响应
   * @param attempt 当前重试序号（从 0）
   * @param done 完成回调
   */
  void ttl_async(const kv::v1::TTLRequest* req, kv::v1::TTLResponse* resp, int attempt,
                 std::function<void(grpc::Status)> done);
  /**
   * 异步 Expire（设 TTL）。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void expire_async(const kv::v1::ExpireRequest* req, kv::v1::ExpireResponse* resp,
                    std::function<void(grpc::Status)> done);
  /**
   * 异步 Persist（去掉 TTL）。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void persist_async(const kv::v1::PersistRequest* req, kv::v1::PersistResponse* resp,
                     std::function<void(grpc::Status)> done);

  /**
   * 异步开事务：分配 start_ts 并登记 ActiveTs。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_begin_async(const kv::v1::TxnBeginRequest* req, kv::v1::TxnBeginResponse* resp,
                       std::function<void(grpc::Status)> done);
  /**
   * 异步事务内点读。
   * @param req 入站请求
   * @param resp 出站响应
   * @param attempt 当前重试序号（从 0）
   * @param done 完成回调
   */
  void txn_get_async(const kv::v1::TxnGetRequest* req, kv::v1::TxnGetResponse* resp, int attempt,
                     std::function<void(grpc::Status)> done);
  /**
   * 异步 Prewrite。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_prewrite_async(const kv::v1::TxnPrewriteRequest* req, kv::v1::TxnPrewriteResponse* resp,
                          std::function<void(grpc::Status)> done);
  /**
   * 异步 Commit（primary + 次键 Resolve）。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_commit_async(const kv::v1::TxnCommitRequest* req, kv::v1::TxnCommitResponse* resp,
                        std::function<void(grpc::Status)> done);
  /**
   * 异步 Rollback。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_rollback_async(const kv::v1::TxnRollbackRequest* req, kv::v1::TxnRollbackResponse* resp,
                          std::function<void(grpc::Status)> done);
  /**
   * 异步悲观锁。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_pessimistic_lock_async(const kv::v1::TxnPessimisticLockRequest* req,
                                  kv::v1::TxnPessimisticLockResponse* resp,
                                  std::function<void(grpc::Status)> done);
  /**
   * 异步悲观锁回滚。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_pessimistic_rollback_async(const kv::v1::TxnPessimisticRollbackRequest* req,
                                      kv::v1::TxnPessimisticRollbackResponse* resp,
                                      std::function<void(grpc::Status)> done);
  /**
   * 异步 ResolveLock。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_resolve_lock_async(const kv::v1::TxnResolveLockRequest* req,
                              kv::v1::TxnResolveLockResponse* resp,
                              std::function<void(grpc::Status)> done);
  /**
   * 异步 CheckTxnStatus。
   * @param req 入站请求
   * @param resp 出站响应
   * @param done 完成回调
   */
  void txn_check_status_async(const kv::v1::TxnCheckStatusRequest* req,
                              kv::v1::TxnCheckStatusResponse* resp,
                              std::function<void(grpc::Status)> done);

  /**
   * still_open：reactor 未取消/未 OnDone 时返回 true（勿再读 CallbackServerContext*）。
   * @param req Watch 请求（按值持有，跨异步存活）
   * @param write_ev 写出事件回调；返回 false 表示停止
   * @param on_done 全部完成或最终失败回调
   * @param still_open 流是否仍打开的谓词
   */
  void watch_async(kv::v1::WatchRequest req,
                   const std::function<bool(const kv::v1::WatchEvent&)>& write_ev,
                   std::function<void()> on_done, std::function<bool()> still_open);

  /**
   * 异步加载 DS 物理 MetaKey 对应 blob。
   * @param mk 物理 MetaKey
   * @param done 完成回调（status, blob, found）
   */
  void ds_load_async(const std::string& mk,
                     std::function<void(grpc::Status, std::string blob, bool found)> done);
  /**
   * 异步写回 DS blob（底层 RawPut）。
   * @param mk 物理 MetaKey
   * @param blob 编码后的 blob
   * @param done 完成回调
   * @param attempt 当前重试序号（从 0）
   */
  void ds_store_async(const std::string& mk, const std::string& blob,
                      std::function<void(grpc::Status)> done, int attempt = 0);

  // ---- DS 异步 Bodies（CQ 安全；对应上方 DSService 入口）----

  /**
   * Hash 设字段（异步实现）。
   * @param req 入站 HSet 请求
   * @param resp 出站 HSet 响应
   * @param done 完成回调
   */
  void HSetAsync(const kv::v1::HSetRequest*, kv::v1::HSetResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Hash 取字段（异步实现）。
   * @param req 入站 HGet 请求
   * @param resp 出站 HGet 响应
   * @param done 完成回调
   */
  void HGetAsync(const kv::v1::HGetRequest*, kv::v1::HGetResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Hash 删字段（异步实现）。
   * @param req 入站 HDel 请求
   * @param resp 出站 HDel 响应
   * @param done 完成回调
   */
  void HDelAsync(const kv::v1::HDelRequest*, kv::v1::HDelResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Hash 字段是否存在（异步实现）。
   * @param req 入站 HExists 请求
   * @param resp 出站 HExists 响应
   * @param done 完成回调
   */
  void HExistsAsync(const kv::v1::HExistsRequest*, kv::v1::HExistsResponse*,
                    std::function<void(grpc::Status)>);

  /**
   * Hash 字段数（异步实现）。
   * @param req 入站 HLen 请求
   * @param resp 出站 HLen 响应
   * @param done 完成回调
   */
  void HLenAsync(const kv::v1::HLenRequest*, kv::v1::HLenResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Hash 全量 field→value（异步实现）。
   * @param req 入站 HGetAll 请求
   * @param resp 出站 HGetAll 响应
   * @param done 完成回调
   */
  void HGetAllAsync(const kv::v1::HGetAllRequest*, kv::v1::HGetAllResponse*,
                    std::function<void(grpc::Status)>);

  /**
   * Hash 全部 field 名（异步实现）。
   * @param req 入站 HKeys 请求
   * @param resp 出站 HKeys 响应
   * @param done 完成回调
   */
  void HKeysAsync(const kv::v1::HKeysRequest*, kv::v1::HKeysResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * Hash 批量设字段（异步实现）。
   * @param req 入站 HMSet 请求
   * @param resp 出站 HMSet 响应
   * @param done 完成回调
   */
  void HMSetAsync(const kv::v1::HMSetRequest*, kv::v1::HMSetResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * Hash 批量取字段（异步实现）。
   * @param req 入站 HMGet 请求
   * @param resp 出站 HMGet 响应
   * @param done 完成回调
   */
  void HMGetAsync(const kv::v1::HMGetRequest*, kv::v1::HMGetResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * Set 加成员（异步实现）。
   * @param req 入站 SAdd 请求
   * @param resp 出站 SAdd 响应
   * @param done 完成回调
   */
  void SAddAsync(const kv::v1::SAddRequest*, kv::v1::SAddResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Set 删成员（异步实现）。
   * @param req 入站 SRem 请求
   * @param resp 出站 SRem 响应
   * @param done 完成回调
   */
  void SRemAsync(const kv::v1::SRemRequest*, kv::v1::SRemResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * Set 成员是否存在（异步实现）。
   * @param req 入站 SIsMember 请求
   * @param resp 出站 SIsMember 响应
   * @param done 完成回调
   */
  void SIsMemberAsync(const kv::v1::SIsMemberRequest*, kv::v1::SIsMemberResponse*,
                      std::function<void(grpc::Status)>);

  /**
   * Set 基数（异步实现）。
   * @param req 入站 SCard 请求
   * @param resp 出站 SCard 响应
   * @param done 完成回调
   */
  void SCardAsync(const kv::v1::SCardRequest*, kv::v1::SCardResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * Set 全部成员（异步实现）。
   * @param req 入站 SMembers 请求
   * @param resp 出站 SMembers 响应
   * @param done 完成回调
   */
  void SMembersAsync(const kv::v1::SMembersRequest*, kv::v1::SMembersResponse*,
                     std::function<void(grpc::Status)>);

  /**
   * List 左侧推入（异步实现）。
   * @param req 入站 LPush 请求
   * @param resp 出站 LPush 响应
   * @param done 完成回调
   */
  void LPushAsync(const kv::v1::LPushRequest*, kv::v1::LPushResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * List 右侧推入（异步实现）。
   * @param req 入站 RPush 请求
   * @param resp 出站 RPush 响应
   * @param done 完成回调
   */
  void RPushAsync(const kv::v1::RPushRequest*, kv::v1::RPushResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * List 左侧弹出（异步实现）。
   * @param req 入站 LPop 请求
   * @param resp 出站 LPop 响应
   * @param done 完成回调
   */
  void LPopAsync(const kv::v1::LPopRequest*, kv::v1::LPopResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * List 右侧弹出（异步实现）。
   * @param req 入站 RPop 请求
   * @param resp 出站 RPop 响应
   * @param done 完成回调
   */
  void RPopAsync(const kv::v1::RPopRequest*, kv::v1::RPopResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * List 长度（异步实现）。
   * @param req 入站 LLen 请求
   * @param resp 出站 LLen 响应
   * @param done 完成回调
   */
  void LLenAsync(const kv::v1::LLenRequest*, kv::v1::LLenResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * List 下标区间（异步实现）。
   * @param req 入站 LRange 请求
   * @param resp 出站 LRange 响应
   * @param done 完成回调
   */
  void LRangeAsync(const kv::v1::LRangeRequest*, kv::v1::LRangeResponse*,
                   std::function<void(grpc::Status)>);

  /**
   * List 按下标取值（异步实现）。
   * @param req 入站 LIndex 请求
   * @param resp 出站 LIndex 响应
   * @param done 完成回调
   */
  void LIndexAsync(const kv::v1::LIndexRequest*, kv::v1::LIndexResponse*,
                   std::function<void(grpc::Status)>);

  /**
   * ZSet 加/更新成员分（异步实现）。
   * @param req 入站 ZAdd 请求
   * @param resp 出站 ZAdd 响应
   * @param done 完成回调
   */
  void ZAddAsync(const kv::v1::ZAddRequest*, kv::v1::ZAddResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * ZSet 删成员（异步实现）。
   * @param req 入站 ZRem 请求
   * @param resp 出站 ZRem 响应
   * @param done 完成回调
   */
  void ZRemAsync(const kv::v1::ZRemRequest*, kv::v1::ZRemResponse*,
                 std::function<void(grpc::Status)>);

  /**
   * ZSet 查成员分（异步实现）。
   * @param req 入站 ZScore 请求
   * @param resp 出站 ZScore 响应
   * @param done 完成回调
   */
  void ZScoreAsync(const kv::v1::ZScoreRequest*, kv::v1::ZScoreResponse*,
                   std::function<void(grpc::Status)>);

  /**
   * ZSet 基数（异步实现）。
   * @param req 入站 ZCard 请求
   * @param resp 出站 ZCard 响应
   * @param done 完成回调
   */
  void ZCardAsync(const kv::v1::ZCardRequest*, kv::v1::ZCardResponse*,
                  std::function<void(grpc::Status)>);

  /**
   * ZSet 按排名区间（异步实现）。
   * @param req 入站 ZRange 请求
   * @param resp 出站 ZRange 响应
   * @param done 完成回调
   */
  void ZRangeAsync(const kv::v1::ZRangeRequest*, kv::v1::ZRangeResponse*,
                   std::function<void(grpc::Status)>);

  /**
   * Watch 过滤：key/prefix 是否命中订阅。
   * @param req Watch 请求
   * @param key 待匹配键
   */
  bool match_watch(const kv::v1::WatchRequest& req, std::string_view key) const;
  /**
   * Watch 涉及的 Region 列表（按 key 或 prefix 覆盖）。
   * @param req Watch 请求
   * @return Region id 列表
   */
  std::vector<uint32_t> regions_for_watch(const kv::v1::WatchRequest& req);

  /**
   * List push 异步：load → 左/右插入 → store。
   * @param key 用户键
   * @param values 待推入的元素
   * @param left true=左侧推入；false=右侧
   * @param done 完成回调（status, 新长度）
   */
  void list_push_async(const std::string& key, std::vector<std::string> values, bool left,
                       std::function<void(grpc::Status, int64_t length)> done);
  /**
   * List pop 异步：load → 左/右弹出 → store。
   * @param key 用户键
   * @param left true=左侧弹出；false=右侧
   * @param done 完成回调（status, value, found）
   */
  void list_pop_async(const std::string& key, bool left,
                      std::function<void(grpc::Status, std::string value, bool found)> done);

  std::vector<std::string> meta_addrs_;     // Meta 地址列表
  std::vector<std::string> storage_addrs_;  // bootstrap Storage 列表
  TSOracle ts_;                             // 本地 TSO
  Router routes_;                           // 路由缓存
  ActiveTsTracker active_ts_;               // 本进程活跃 start_ts
  std::unique_ptr<openkv::WorkPool> pool_;  // Watch 专用线程池

  mutable std::mutex mu_;                                  // 保护 leader_hint_
  std::unordered_map<uint32_t, std::string> leader_hint_;  // region → addr（not_leader 记忆）

  std::atomic<bool> bg_stop_{false};  // 后台线程停止标志
  std::thread bg_thread_;             // route_watch_loop

  mutable std::atomic<uint64_t> client_in_{0};                        // 入站客户端 RPC 计数
  mutable std::atomic<uint64_t> to_store_{0};                         // 出站 Storage 计数
  mutable std::atomic<uint64_t> to_meta_{0};                          // 出站 Meta 计数
  mutable std::mutex peer_mu_;                                        // 保护按对端计数
  mutable std::unordered_map<std::string, uint64_t> to_store_peers_;  // Storage 按地址
  mutable std::unordered_map<std::string, uint64_t> to_meta_peers_;   // Meta 按地址
};

/**
 * 传输类错误（连不上等）→ 调用方换副本 / 刷路由。
 * @param msg 错误/状态正文
 */
bool store_skip_err(const std::string& msg);
/**
 * 从 epoch_not_match 文案解析 expected epoch；失败返回 0。
 * @param msg 错误/状态正文
 */
uint64_t parse_epoch_expected(const std::string& msg);
/**
 * 从 cas_failed 文案解析当前 revision；失败返回 0。
 * @param msg 错误/状态正文
 */
uint64_t parse_cas_current_rev(const std::string& msg);
/**
 * 前缀上界（下一字典序前缀），供 Scan/ListPrefix 算 end。
 * @param prefix 键前缀
 */
std::string prefix_upper_bound(std::string_view prefix);

}  // namespace openkv::compute
