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
// 构建要求：RocksDB 由 FetchContent 固定版本与工程一起编译（OPENKV_HAS_ROCKSDB=1）。
// 未定义该宏时的内存 map 仅作极端 stub，正常构建不会走到。

#pragma once

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

#include "openkv/raft/engine/sync_mode.hpp"
#include "openkv/region.hpp"

namespace openkv::storage {

// revision 最高位标记：payload 含 expire_at_ms（value 头扩展；兼容旧无 TTL 记录）。
inline constexpr uint64_t kKvRevExpireFlag = 1ULL << 63;

/* 单 key 的值与 Raft log index 作为 revision（便于 MVCC/调试）。 */
struct KvRecord {
  std::string value;     // 用户值（不含 TTL 头）
  uint64_t revision{0};  // 通常为 Raft apply index
  // 绝对过期时间（unix ms）；0 = 永不过期。
  int64_t expire_at_ms{0};
};

/**
 * 编码写入引擎的 payload：revision[+expire] + value。
 * @param revision apply index；若 expire_at_ms>0 会置 kKvRevExpireFlag
 * @param expire_at_ms 绝对过期 unix ms；0=永久
 * @param value 用户值
 * @return 可 Put 的字节串
 */
std::string encode_kv_payload(uint64_t revision, int64_t expire_at_ms, std::string_view value);
/**
 * 解码引擎 payload 到 KvRecord。
 * @param payload 引擎 value
 * @param out 输出记录
 * @return 是否解码成功
 */
bool decode_kv_payload(std::string_view payload, KvRecord* out);
/**
 * expire_at>0 且 now>=expire_at → 过期；now_ms<=0 时取当前墙钟。
 * @param rec 待判记录
 * @param now_ms 墙钟 ms；<=0 表示用当前时间
 * @return true 表示已过期
 */
bool kv_record_expired(const KvRecord& rec, int64_t now_ms = 0);

/*
 * RocksDB 封装；未链接 RocksDB 时回退到内存 map（便于本地开发）
 * `store_level=false`：data_dir/shard_<id>；`true`：data_dir 即为 DB 根（单 Store）。
 */
class KvEngine {
 public:
  /**
   * 打开或创建引擎目录。
   * @param data_dir 数据根目录
   * @param shard_id 分片 id（非 store_level 时拼进路径）
   * @param sync 写同步模式；默认 kData
   * @param store_level true=单 Store 共用 data_dir 为 DB 根
   */
  explicit KvEngine(std::string data_dir, uint32_t shard_id,
                    raft::SyncMode sync = raft::SyncMode::kData, bool store_level = false);
  /**
   * 关闭 DB / 释放资源。
   */
  ~KvEngine();

  KvEngine(const KvEngine&) = delete;
  KvEngine& operator=(const KvEngine&) = delete;

  /**
   * 写入单 key；revision 通常取 Raft apply index；expire_at_ms=0 永不过期。
   * @param key 用户键
   * @param value 用户值
   * @param revision apply index
   * @param expire_at_ms 绝对过期 unix ms；0=永久
   * @return 是否写入成功
   */
  bool put(std::string_view key, std::string_view value, uint64_t revision,
           int64_t expire_at_ms = 0);
  /**
   * 删除单 key（写入墓碑或直接删，取决于实现）。
   * @param key 用户键
   * @param revision apply index（记入元信息）
   * @return 是否删除成功
   */
  bool del(std::string_view key, uint64_t revision);
  /**
   * 批量写；puts 为 (key,value,revision)，expire 缺省为 0。
   * @param puts 待写入三元组列表
   * @param dels 待删除 key 列表
   * @return 整批是否成功
   */
  bool write_batch(const std::vector<std::tuple<std::string, std::string, uint64_t>>& puts,
                   const std::vector<std::string>& dels);
  /**
   * 批量写；puts 为 (key,value,revision,expire_at_ms)。
   * @param puts 待写入四元组列表
   * @param dels 待删除 key 列表
   * @return 整批是否成功
   */
  bool write_batch(const std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>>& puts,
                   const std::vector<std::string>& dels);
  /**
   * 读单 key；hide_expired=true（默认）时过期 key 视为不存在。
   * @param key 用户键
   * @param hide_expired 是否隐藏已过期记录
   * @return 存在则返回记录，否则 nullopt
   */
  std::optional<KvRecord> get(std::string_view key, bool hide_expired = true) const;
  /**
   * 催促 TTL CompactionFilter 回收过期 key。
   */
  void compact_for_ttl();

  /**
   * 按字典序扫描 prefix 下的 key；fn 返回 false 则停止。
   * @param prefix key 前缀
   * @param fn 每条回调；(key, record) → 是否继续
   */
  void scan_prefix(std::string_view prefix,
                   const std::function<bool(std::string_view key, const KvRecord&)>& fn) const;
  /**
   * 全表扫描；fn 返回 false 则停止。
   * @param fn 每条回调；(key, record) → 是否继续
   */
  void for_each(const std::function<bool(std::string_view key, const KvRecord&)>& fn) const;

  /**
   * Raft 快照：导出全库（空 range）。
   * @return 序列化快照字节
   */
  std::string create_snapshot() const { return create_snapshot(openkv::KeyRange{}); }
  /**
   * Raft 快照：导出；range 非空时只导出该半开区间（同机共享引擎）。
   * @param range 半开区间；两端空=全空间
   * @return 序列化快照字节
   */
  std::string create_snapshot(const openkv::KeyRange& range) const;
  /**
   * 导入快照覆盖整库（空 range）。
   * @param data 快照载荷
   */
  void apply_snapshot(std::string_view data) { apply_snapshot(data, openkv::KeyRange{}); }
  /**
   * 导入快照；range 全空：清空整库再导入；非空：只删 range 内用户 key 再导入。
   * @param data 快照载荷
   * @param range 半开区间；两端空=整库
   */
  void apply_snapshot(std::string_view data, const openkv::KeyRange& range);
  /**
   * 删除落在 range 内的用户 key（共享引擎 DESTROY 右片时用）；跳过 `\xff\xffskv/`。
   * @param range key 范围
   * @return 删除条数
   */
  size_t delete_range(const openkv::KeyRange& range);

  /**
   * E3：RocksDB Checkpoint（硬链 SST，廉价一致点）；dest_dir 须不存在或为空。
   * @param dest_dir 目标目录
   * @return 是否成功
   */
  bool create_checkpoint(std::string_view dest_dir) const;
  /**
   * E3：从 checkpoint 目录导入覆盖本机（演练 / 本地恢复）；先清空再写入。
   * @param src_dir checkpoint 目录
   * @return 是否成功
   */
  bool restore_from_checkpoint(std::string_view src_dir);

  /**
   * 近似唯一用户 key 数（全空间）。
   * @return 估算 key 数
   */
  size_t approx_key_count() const { return approx_key_count(openkv::KeyRange{}); }
  /**
   * 近似用户数据字节数（全空间）。
   * @return 估算字节数
   */
  size_t approx_bytes() const { return approx_bytes(openkv::KeyRange{}); }
  /**
   * 近似唯一用户 key 数；空 range = 全空间（自动分裂阈值）。
   * @param range 半开区间
   * @return 估算 key 数
   */
  size_t approx_key_count(const openkv::KeyRange& range) const;
  /**
   * 近似用户数据字节数；空 range = 全空间。
   * @param range 半开区间
   * @return 估算字节数
   */
  size_t approx_bytes(const openkv::KeyRange& range) const;
  /**
   * 选分裂点：全空间用户 key 排序后中位；不足 2 个返回空。
   * @return split_key；失败为空串
   */
  std::string pick_split_key() const { return pick_split_key(openkv::KeyRange{}); }
  /**
   * 选分裂点：range 内用户 key 排序后中位；不足 2 个返回空。
   * @param range 半开区间
   * @return split_key；失败为空串
   */
  std::string pick_split_key(const openkv::KeyRange& range) const;
  /**
   * 将 key >= split_key 的记录搬到 dst（跳过 `\xff\xffskv/` meta）。
   * RocksDB：dst PutBatch 成功后再 src DeleteBatch；返回搬迁条数。
   * @param dst 目标引擎
   * @param split_key 分界（含）
   * @return 搬迁条数
   */
  size_t migrate_ge(KvEngine& dst, std::string_view split_key);
  /**
   * 将全部记录搬到 dst（Region merge），返回条数。
   * @param dst 目标引擎
   * @return 搬迁条数
   */
  size_t migrate_all(KvEngine& dst);

  /**
   * 本引擎绑定的分片 id。
   * @return shard_id
   */
  uint32_t shard_id() const { return shard_id_; }
  /**
   * 是否单 Store 级引擎（多 Region 共享）。
   * @return store_level_
   */
  bool store_level() const { return store_level_; }
  /**
   * 当前写同步模式。
   * @return SyncMode
   */
  raft::SyncMode sync_mode() const { return sync_; }
  /**
   * 运行时切换写同步模式并刷新 WriteOptions。
   * @param m 新模式
   */
  void set_sync_mode(raft::SyncMode m);
  /**
   * DB 根路径（store_level 时即 data_dir，否则 data_dir/shard_id）。
   * @return 绝对或相对路径串
   */
  std::string db_path() const;

 private:
#if defined(OPENKV_HAS_ROCKSDB) && OPENKV_HAS_ROCKSDB
  /**
   * 按 sync_ 刷新 RocksDB WriteOptions。
   */
  void refresh_write_options();
  /**
   * 把旧 default CF 数据迁到用户 CF（一次性升级）。
   */
  void migrate_legacy_default_cf();
#endif

  std::string data_dir_;                        // 数据根目录
  uint32_t shard_id_;                           // 分片 id
  bool store_level_{false};                     // true=单 Store 共用 DB
  raft::SyncMode sync_{raft::SyncMode::kData};  // 写同步模式
#if defined(OPENKV_HAS_ROCKSDB) && OPENKV_HAS_ROCKSDB
  struct RocksHandle;                   // RocksDB 句柄（定义在 .cpp）
  std::unique_ptr<RocksHandle> rocks_;  // RocksDB 实现
#else
  mutable std::mutex mu_;                          // 保护 mem_
  std::unordered_map<std::string, KvRecord> mem_;  // 无 RocksDB 时的内存 stub
#endif
};

/*
 * 单 Store 一份 RocksDB（data_dir/kv）；Region 仅靠 KeyRange 隔离。
 * get_or_create(any) 返回同一引擎；share_with 为 no-op 成功。
 */
class ShardEngineManager {
 public:
  /**
   * 构造管理器；真正打开 DB 在首次 get_or_create。
   * @param data_dir 数据根目录
   * @param sync 写同步模式；默认 kData
   */
  explicit ShardEngineManager(std::string data_dir, raft::SyncMode sync = raft::SyncMode::kData);

  /**
   * 取或创建 shard 映射；单 Store 下恒返回同一共享引擎。
   * @param shard_id 分片 / Region id
   * @return 引擎引用
   */
  KvEngine& get_or_create(uint32_t shard_id);
  /**
   * 取已映射引擎；未创建返回 nullptr。
   * @param shard_id 分片 / Region id
   * @return 引擎指针或 nullptr
   */
  KvEngine* get(uint32_t shard_id);
  /**
   * 单 Store 下恒为 true（映射到同一引擎）。
   * @param shard_id 目标分片
   * @param source_shard 源分片（共享来源）
   * @return 是否共享成功
   */
  bool share_with(uint32_t shard_id, uint32_t source_shard);
  /**
   * 两分片是否映射到同一引擎实例。
   * @param a 分片 A
   * @param b 分片 B
   * @return true 表示同引擎
   */
  bool shares_engine(uint32_t a, uint32_t b) const;
  /**
   * 解除 region 映射；单 Store 下永不销毁共享引擎（仅摘 map）。
   * @param shard_id 分片 / Region id
   * @return 剩余映射数
   */
  size_t release(uint32_t shard_id);
  /**
   * 当前写同步模式。
   * @return SyncMode
   */
  raft::SyncMode sync_mode() const { return sync_; }
  /**
   * 启动时把旧 data_dir/shard_* 迁入 data_dir/kv（若 kv 已存在则跳过冲突 key）。
   */
  void migrate_legacy_shards_if_needed();

 private:
  /**
   * 确保 shared_ 已打开（已持 mu_）。
   */
  void ensure_shared_locked();

  std::string data_dir_;                                             // 数据目录
  raft::SyncMode sync_;                                              // 同步模式
  mutable std::mutex mu_;                                            // 保护 shared_ / engines_
  std::shared_ptr<KvEngine> shared_;                                 // 单 Store 共享引擎
  std::unordered_map<uint32_t, std::shared_ptr<KvEngine>> engines_;  // Region → 引擎映射
};

}  // namespace openkv::storage
