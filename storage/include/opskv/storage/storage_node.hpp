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

// @file storage_node.hpp
// 存储节点：Multi-Raft（多副本）+ 分片引擎 + Raft TCP 传输。

#pragma once

#include "opskv/config.hpp"
#include "opskv/peer_counter.hpp"
#include "opskv/raft/multi_raft.hpp"
#include "opskv/raft/snap_stream.hpp"
#include "opskv/region.hpp"
#include "opskv/storage/kv_engine.hpp"
#include "opskv/storage/raftkv.hpp"
#include "opskv/storage/rocks_raft_engine.hpp"
#include "opskv/storage/scheduler.hpp"
#include "opskv/storage/coprocessor.hpp"
#include "opskv/storage/watch_hub.hpp"
#include "opskv/raft/grpc_transport.hpp"

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace opskv::storage {

class StorageNode {
 public:
  explicit StorageNode(NodeConfig cfg);
  ~StorageNode();

  void start();
  void stop();

  void on_apply(uint32_t shard_id, uint64_t index, const std::string& data);

  /// S2：propose 等待 apply 后读取结果；空串=成功。无条目视为成功。
  std::optional<std::string> take_apply_result(uint64_t index);

  /// E4：request_id 去重缓存（Leader 重试短路 / apply 跳过重复）。
  struct IdempotentRecord {
    bool ok{true};
    uint64_t revision{0};
    std::string error;
  };
  std::optional<IdempotentRecord> lookup_request(uint32_t shard_id, uint64_t request_id) const;
  void remember_request(uint32_t shard_id, uint64_t request_id, IdempotentRecord rec);

  bool add_learner(uint32_t shard_id, const raft::Peer& peer);
  bool promote_learner(uint32_t shard_id, const std::string& peer_id);
  bool remove_peer(uint32_t shard_id, const std::string& peer_id);

  /// 确保本机托管该 Region；peers 非空时用于首次创建（可含 learner）。
  std::string admin_ensure_region(uint32_t region_id, const std::vector<raft::Peer>& peers);
  std::string admin_add_learner(uint32_t region_id, const raft::Peer& peer);
  std::string admin_promote(uint32_t region_id, const std::string& peer_id);
  std::string admin_remove_peer(uint32_t region_id, const std::string& peer_id);
  /// 本机停止托管 Region（place 踢出后防脑裂）。
  std::string admin_destroy_region(uint32_t region_id);
  /// Leader：OK PROGRESS match commit last caught_up(0|1) learner(0|1)
  std::string admin_peer_progress(uint32_t region_id, const std::string& peer_id);
  /// Leader 让位并通知 target 竞选：OK TRANSFER <target>
  std::string admin_transfer_leader(uint32_t region_id, const std::string& target_id);
  std::string admin_campaign(uint32_t region_id);

  raft::MultiRaft& multi_raft() { return *multi_raft_; }
  ShardEngineManager& engines() { return *engines_; }
  RaftKV* raftkv() { return raftkv_.get(); }
  Scheduler* scheduler() { return scheduler_.get(); }
  WatchHub& watch_hub() { return watch_hub_; }
  /// Coprocessor：对本机 Region range scan（key/value 前缀、keys_only、count_only）。
  CoprocessorResponse coprocessor_execute(uint32_t region_id, const CoprocessorRequest& req);
  const NodeConfig& config() const { return cfg_; }
  raft::GrpcRaftTransport& transport() { return transport_; }

  /// TCP 入站 Raft RPC。
  std::string handle_raft_vote(uint32_t shard, const std::string& from, uint64_t term,
                               uint64_t last_idx, uint64_t last_term);
  std::string handle_raft_pre_vote(uint32_t shard, const std::string& from, uint64_t term,
                                   uint64_t last_idx, uint64_t last_term);
  std::string handle_raft_append(uint32_t shard, const std::string& from, uint64_t term,
                                 uint64_t prev_idx, uint64_t prev_term, uint64_t leader_commit,
                                 const std::vector<raft::LogEntry>& entries);
  std::string handle_raft_snapshot(uint32_t shard, const std::string& from, uint64_t term,
                                   uint64_t last_idx, uint64_t last_term, std::string_view data);
  /// 流式快照分片：RAFT_SNAPC；未收齐返回 OK SNAPC，收齐后同 handle_raft_snapshot。
  std::string handle_raft_snap_chunk(uint32_t shard, const std::string& from, uint64_t term,
                                     uint64_t last_idx, uint64_t last_term, uint64_t offset,
                                     uint64_t total, bool done, std::string_view chunk);

  /// Leader id → 可达地址（供 redirect）。
  std::string peer_address(const std::string& node_id) const;

  /// 分裂流量暂停：拒绝 left 上写路径，直到 admin_split 完成。
  std::string admin_prepare_split(uint32_t left_region, std::string_view split_key);
  /// Range 分裂：先 seed 右 Region 成员，再 Raft propose migrate。
  std::string admin_split(uint32_t left_region, uint32_t right_region, std::string_view split_key);
  /// Range 合并：把 right 全部 key 并入 left。
  std::string admin_merge(uint32_t left_region, uint32_t right_region);
  /// 解析 Region 成员（本机 Raft → Meta → bootstrap）。
  std::vector<raft::Peer> resolve_region_peers(uint32_t region_id);

  /// 若 Region 处于分裂暂停窗口，返回 true（写应失败 splitting）。
  bool region_splitting(uint32_t region_id) const;
  /// Leader tick：清理过期锁 + 按 Meta GC safe point 删 MVCC 旧版本。
  void maybe_txn_gc(uint32_t region_id);
  /// 向 Meta 拉 GC safe point；失败返回 0。
  uint64_t fetch_gc_safe_ts();
  /// 周期性检查阈值，触发 Meta SPLIT + 全副本 ADMIN_SPLIT。
  void maybe_auto_split(uint32_t region_id);

  /// 完整 place 编排（与 skv place 同路径）；供 ADMIN_PLACE_REGION / scatter。
  bool place_region(uint32_t region_id, const std::vector<std::string>& desired_peers);
  std::string admin_place_region(uint32_t region_id, const std::vector<std::string>& desired);

  /// Region epoch：分裂/合并/place 递增；txn 路径可校验。
  uint64_t region_epoch(uint32_t region_id) const;
  void set_region_epoch(uint32_t region_id, uint64_t epoch);
  void bump_region_epoch(uint32_t region_id);
  /// 本机 Region 区间（Split 后同机共享引擎，用区间过滤）。
  opskv::KeyRange region_range(uint32_t region_id) const;
  void set_region_range(uint32_t region_id, opskv::KeyRange range);
  bool key_in_region(uint32_t region_id, std::string_view key) const;
  std::string admin_set_epoch(uint32_t region_id, uint64_t epoch);
  /// E3：对本机 Region 引擎做 Checkpoint；path 空则 data_dir/ckpt_region_<id>_<ts>。
  std::string admin_checkpoint(uint32_t region_id, std::string_view path = {});
  /// 本节点发往 Meta 的出站 RPC 累计次数（STATS meta_out）。
  uint64_t meta_out_count() const { return meta_out_.load(std::memory_order_relaxed); }
  std::string meta_out_peers_encode() const { return meta_out_peers_.encode(); }

 private:
  void tick_loop();
  void register_and_heartbeat();
  void ensure_region(uint32_t region_id);
  bool meta_split(uint32_t left, const std::string& split_key, uint32_t* right_out);
  void broadcast_admin_split(uint32_t left, uint32_t right, const std::string& split_key);
  /// S2：在左 Region 全体成员上 ENSURE 右 Region（同成员集），再允许 migrate。
  std::string seed_right_region(uint32_t left_region, uint32_t right_region);

  NodeConfig cfg_;
  std::unique_ptr<ShardEngineManager> engines_;
  /// 共享 RaftEngine（data_dir/raft RocksDB）。
  std::shared_ptr<RocksRaftEngine> raft_engine_;
  std::unique_ptr<raft::MultiRaft> multi_raft_;
  std::unique_ptr<RaftKV> raftkv_;
  std::unique_ptr<Scheduler> scheduler_;
  WatchHub watch_hub_;
  raft::GrpcRaftTransport transport_;
  raft::SnapStreamBuffer snap_buf_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> meta_out_{0};  // 本节点 → Meta 出站 RPC
  PeerCounter meta_out_peers_;         // Meta 地址 → 出站次数
  std::thread tick_thread_;
  std::thread hb_thread_;
  std::vector<raft::Peer> bootstrap_peers_;
  size_t auto_split_keys_{0};   // 0=关；SKV_SPLIT_KEYS
  size_t auto_split_bytes_{0};  // 0=关；SKV_SPLIT_BYTES
  size_t auto_merge_keys_{0};   // 0=关；SKV_MERGE_KEYS（两侧都低于阈值才合）
  std::mutex split_mu_;
  std::mutex create_mu_;
  std::mutex place_mu_;
  mutable std::mutex destroy_mu_;
  std::set<uint32_t> placing_;  // 进行中的 place，防重入
  /// DESTROY 后拒绝 RAFT get_or_create，直到 ENSURE 清除（防踢出后再被迟到 AE 复活）。
  std::set<uint32_t> destroyed_regions_;
  std::unordered_map<uint32_t, std::vector<raft::Peer>> pending_peers_;
  bool region_destroyed(uint32_t region_id) const;
  void mark_region_destroyed(uint32_t region_id);
  void clear_region_destroyed(uint32_t region_id);
  /// REGISTER / 自描述地址：优先 advertise_addr，否则 listen（0.0.0.0→127.0.0.1）。
  std::string advertise_address() const;
  mutable std::mutex epoch_mu_;
  std::unordered_map<uint32_t, uint64_t> region_epochs_;  // default 1
  mutable std::mutex range_mu_;
  std::unordered_map<uint32_t, opskv::KeyRange> region_ranges_;

  struct SplitPause {
    bool active{false};
    std::string split_key;
  };
  mutable std::mutex split_pause_mu_;
  std::unordered_map<uint32_t, SplitPause> split_pauses_;

  void set_split_pause(uint32_t region_id, bool on, std::string_view split_key = {});
  void broadcast_prepare_split(uint32_t left, const std::string& split_key);
  /// S1：已编码 Admin/业务 Command 经 Raft propose 并等待 apply。
  bool propose_encoded(uint32_t shard_id, std::string encoded, std::string* error,
                       std::string* redirect);
  void persist_epoch(uint32_t region_id, uint64_t epoch);
  uint64_t load_epoch_from_engine(uint32_t region_id);
  void publish_apply_result(uint64_t index, std::string error);

  mutable std::mutex apply_res_mu_;
  std::unordered_map<uint64_t, std::string> apply_results_;

  mutable std::mutex req_mu_;
  std::unordered_map<uint32_t, std::unordered_map<uint64_t, IdempotentRecord>> req_cache_;
  std::unordered_map<uint32_t, std::deque<uint64_t>> req_order_;

  void maybe_auto_merge(uint32_t region_id);
  bool meta_suggest_place(std::vector<std::string>* out,
                          const std::vector<std::string>& avoid);
  bool meta_merge(uint32_t left, uint32_t right);
  bool meta_find_merge_right(uint32_t left, uint32_t* right_out);
  bool meta_list_storage(std::unordered_map<std::string, std::string>* id_to_addr);
  bool meta_place(uint32_t region_id, const std::vector<std::string>& peers);
  bool meta_region_peers(uint32_t region_id, std::vector<std::pair<std::string, std::string>>* out,
                         std::string* leader_id);
  std::string meta_rpc(const std::string& req);
  std::string admin_rpc(const std::string& req, const std::vector<std::string>& seed_addrs);
  bool wait_peer_caught_up(uint32_t region_id, const std::string& peer_id,
                           const std::vector<std::string>& seed_addrs);
  /// 限频 CAMPAIGN：仅当无主或距上次 ≥ min_gap；避免 place 重试打出选举风暴。
  bool maybe_campaign(uint32_t region_id, const std::string& addr,
                      std::chrono::milliseconds min_gap = std::chrono::milliseconds(800));

  mutable std::mutex campaign_mu_;
  std::unordered_map<uint32_t, std::chrono::steady_clock::time_point> last_campaign_at_;
};

}  // namespace opskv::storage
