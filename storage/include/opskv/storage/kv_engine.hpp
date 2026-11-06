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

// @file kv_engine.hpp
// 分片级 KV 存储引擎（默认 / 生产路径：RocksDB）。
//
// 架构位置：Raft apply → KvEngine::put/del；线性一致读 → KvEngine::get。
// 构建要求：RocksDB 由 FetchContent 固定版本与工程一起编译（OPSKV_HAS_ROCKSDB=1）。
// 未定义该宏时的内存 map 仅作极端 stub，正常构建不会走到。

#pragma once

#include "opskv/raft/raft_persist.hpp"
#include "opskv/region.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace opskv::storage {

/// 单 key 的值与 Raft log index 作为 revision（便于 MVCC/调试）。
struct KvRecord {
  std::string value;
  uint64_t revision{0};
};

// RocksDB 封装；未链接 RocksDB 时回退到内存 map（便于本地开发）
/// `store_level=false`：data_dir/shard_<id>；`true`：data_dir 即为 DB 根（单 Store）。
class KvEngine {
 public:
  explicit KvEngine(std::string data_dir, uint32_t shard_id,
                    raft::SyncMode sync = raft::SyncMode::kData, bool store_level = false);
  ~KvEngine();

  KvEngine(const KvEngine&) = delete;
  KvEngine& operator=(const KvEngine&) = delete;

  /// revision 通常取 Raft apply index；内存后端会覆盖同 key。
  bool put(std::string_view key, std::string_view value, uint64_t revision);
  bool del(std::string_view key, uint64_t revision);
  /// 批量写（跨 Region Apply WriteBatch）；pairs 为 (key,value,revision)，del_keys 为删除。
  bool write_batch(const std::vector<std::tuple<std::string, std::string, uint64_t>>& puts,
                   const std::vector<std::string>& dels);
  std::optional<KvRecord> get(std::string_view key) const;

  /// 按字典序扫描 prefix 下的 key；fn 返回 false 则停止。
  void scan_prefix(std::string_view prefix,
                   const std::function<bool(std::string_view key, const KvRecord&)>& fn) const;
  /// 全表扫描；fn 返回 false 则停止。
  void for_each(const std::function<bool(std::string_view key, const KvRecord&)>& fn) const;

  // Raft 快照：导出/导入；range 非空时只导出/替换该半开区间（同机共享引擎）。
  /// 序列化（Learner 追赶 / 快照传输）；格式见 kv_engine.cpp。
  std::string create_snapshot() const { return create_snapshot(opskv::KeyRange{}); }
  std::string create_snapshot(const opskv::KeyRange& range) const;
  /// range 全空：清空整库再导入；非空：只删除 range 内用户 key 再导入（保留兄弟 Region）。
  void apply_snapshot(std::string_view data) { apply_snapshot(data, opskv::KeyRange{}); }
  void apply_snapshot(std::string_view data, const opskv::KeyRange& range);
  /// 删除落在 range 内的用户 key（共享引擎 DESTROY 右片时用）；跳过 `\xff\xffskv/`。
  size_t delete_range(const opskv::KeyRange& range);

  /// E3：RocksDB Checkpoint（硬链 SST，廉价一致点）；dest_dir 须不存在或为空。
  bool create_checkpoint(std::string_view dest_dir) const;
  /// E3：从 checkpoint 目录导入覆盖本机（演练 / 本地恢复）；先清空再写入。
  bool restore_from_checkpoint(std::string_view src_dir);

  /// 近似 key 数 / 字节数（自动分裂阈值）。
  size_t approx_key_count() const;
  size_t approx_bytes() const;
  /// 选分裂点：排序后中位 key；不足 2 个返回空。
  std::string pick_split_key() const;
  /// 将 key >= split_key 的记录搬到 dst（跳过 `\xff\xffskv/` meta）。
  /// RocksDB：dst PutBatch 成功后再 src DeleteBatch；返回搬迁条数。
  size_t migrate_ge(KvEngine& dst, std::string_view split_key);
  /// 将全部记录搬到 dst（Region merge），返回条数。
  size_t migrate_all(KvEngine& dst);

  uint32_t shard_id() const { return shard_id_; }
  bool store_level() const { return store_level_; }
  raft::SyncMode sync_mode() const { return sync_; }
  void set_sync_mode(raft::SyncMode m);
  /// E2：按物理 key 归类的 CF 名（default|lock|data|write）；供单测。
  static const char* cf_name_for_key(std::string_view key);
  /// DB 根路径（store_level 时即 data_dir，否则 data_dir/shard_id）。
  std::string db_path() const;

 private:
#if defined(OPSKV_HAS_ROCKSDB) && OPSKV_HAS_ROCKSDB
  void refresh_write_options();
  void migrate_legacy_default_cf();
#endif

  std::string data_dir_;
  uint32_t shard_id_;
  bool store_level_{false};
  raft::SyncMode sync_{raft::SyncMode::kData};
#if defined(OPSKV_HAS_ROCKSDB) && OPSKV_HAS_ROCKSDB
  struct RocksHandle;
  std::unique_ptr<RocksHandle> rocks_;
#else
  mutable std::mutex mu_;
  std::unordered_map<std::string, KvRecord> mem_;
#endif
};

/// 单 Store 一份 RocksDB（data_dir/kv）；Region 仅靠 KeyRange 隔离。
/// get_or_create(any) 返回同一引擎；share_with 为 no-op 成功。
class ShardEngineManager {
 public:
  explicit ShardEngineManager(std::string data_dir,
                              raft::SyncMode sync = raft::SyncMode::kData);

  KvEngine& get_or_create(uint32_t shard_id);
  KvEngine* get(uint32_t shard_id);
  /// 单 Store 下恒为 true（映射到同一引擎）。
  bool share_with(uint32_t shard_id, uint32_t source_shard);
  bool shares_engine(uint32_t a, uint32_t b) const;
  /// 解除 region 映射；单 Store 下永不销毁共享引擎（仅摘 map）。
  size_t release(uint32_t shard_id);
  raft::SyncMode sync_mode() const { return sync_; }
  /// 启动时把旧 data_dir/shard_* 迁入 data_dir/kv（若 kv 已存在则跳过冲突 key）。
  void migrate_legacy_shards_if_needed();

 private:
  void ensure_shared_locked();

  std::string data_dir_;
  raft::SyncMode sync_;
  mutable std::mutex mu_;
  std::shared_ptr<KvEngine> shared_;
  std::unordered_map<uint32_t, std::shared_ptr<KvEngine>> engines_;
};

}  // namespace opskv::storage
