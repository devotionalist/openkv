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

// =============================================================================
// @file kv_engine.cpp
// -----------------------------------------------------------------------------
// 分片级 KV 持久化引擎实现（Storage 层的「最后一公里」落盘）。
//
// 在架构中的位置：
//   Client → Compute Gateway → Storage TCP → StoreFacade::propose
//        → Raft 多数派提交 → on_apply → KvEngine::put / del
//   线性一致读：StoreFacade::get →（校验 Leader）→ KvEngine::get
//
// 双后端：
//   1) RocksDB：CMake FetchContent 固定版本随工程编译（默认，生产路径）
//   2) 内存 unordered_map：未定义 OPENKV_HAS_ROCKSDB 时的 stub（正常构建不会走到）
//
// 线程安全：
//   - 内存后端：所有公开方法持 mu_
//   - RocksDB 后端：依赖 rocksdb::DB 自身的并发控制（本文件不再加锁）
//
// 与 Raft 的关系：
//   revision 通常传入 Raft apply 后的 log index，便于排查与后续 MVCC 扩展；
//   当前实现不做多版本保留，同 key 后写覆盖前写。
// =============================================================================

#include "openkv/storage/kv_engine.hpp"

#include "openkv/logging.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace openkv::storage {
namespace {

int64_t wall_ms_now_local() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace

std::string encode_kv_payload(uint64_t revision, int64_t expire_at_ms, std::string_view value) {
  // 始终写新格式（含 expire）；revision 最高位标记可读旧库无 flag 的记录。
  const uint64_t wire_rev = (revision & ~kKvRevExpireFlag) | kKvRevExpireFlag;
  std::string payload(sizeof(uint64_t) + sizeof(int64_t) + value.size(), '\0');
  std::memcpy(payload.data(), &wire_rev, sizeof(wire_rev));
  std::memcpy(payload.data() + sizeof(uint64_t), &expire_at_ms, sizeof(expire_at_ms));
  if (!value.empty()) {
    std::memcpy(payload.data() + sizeof(uint64_t) + sizeof(int64_t), value.data(), value.size());
  }
  return payload;
}

bool decode_kv_payload(std::string_view payload, KvRecord* out) {
  if (!out || payload.size() < sizeof(uint64_t)) return false;
  uint64_t wire_rev = 0;
  std::memcpy(&wire_rev, payload.data(), sizeof(wire_rev));
  if (wire_rev & kKvRevExpireFlag) {
    if (payload.size() < sizeof(uint64_t) + sizeof(int64_t)) return false;
    out->revision = wire_rev & ~kKvRevExpireFlag;
    std::memcpy(&out->expire_at_ms, payload.data() + sizeof(uint64_t), sizeof(out->expire_at_ms));
    out->value.assign(payload.data() + sizeof(uint64_t) + sizeof(int64_t),
                      payload.size() - sizeof(uint64_t) - sizeof(int64_t));
    return true;
  }
  out->revision = wire_rev;
  out->expire_at_ms = 0;
  out->value.assign(payload.data() + sizeof(uint64_t), payload.size() - sizeof(uint64_t));
  return true;
}

bool kv_record_expired(const KvRecord& rec, int64_t now_ms) {
  if (rec.expire_at_ms <= 0) return false;
  if (now_ms <= 0) now_ms = wall_ms_now_local();
  return now_ms >= rec.expire_at_ms;
}

}  // namespace openkv::storage

#if defined(OPENKV_HAS_ROCKSDB) && OPENKV_HAS_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/write_batch.h>
#endif

namespace openkv::storage {
namespace {

// E2：MVCC 物理 key 形态 user\0{L|D|W}… → 对应 CF；其余（含 meta）走 default。
enum class CfId : uint8_t { kDefault = 0, kLock = 1, kData = 2, kWrite = 3 };

CfId classify_key(std::string_view key) {
  const auto pos = key.find('\0');
  if (pos != std::string_view::npos && pos + 1 < key.size()) {
    switch (key[pos + 1]) {
      case 'L':
        return CfId::kLock;
      case 'D':
        return CfId::kData;
      case 'W':
        return CfId::kWrite;
      default:
        break;
    }
  }
  return CfId::kDefault;
}

constexpr const char* kCfLock = "lock";
constexpr const char* kCfData = "data";
constexpr const char* kCfWrite = "write";

}  // namespace

const char* KvEngine::cf_name_for_key(std::string_view key) {
  switch (classify_key(key)) {
    case CfId::kLock:
      return kCfLock;
    case CfId::kData:
      return kCfData;
    case CfId::kWrite:
      return kCfWrite;
    case CfId::kDefault:
    default:
      return "default";
  }
}

// =============================================================================
// 实现分支 A：RocksDB 后端（存储节点启动默认路径）
// =============================================================================
#if defined(OPENKV_HAS_ROCKSDB) && OPENKV_HAS_ROCKSDB

namespace {

void appendU32(std::string& out, uint32_t n) {
  out.append(reinterpret_cast<const char*>(&n), sizeof(n));
}

bool readU32(std::string_view data, size_t& i, uint32_t& n) {
  if (i + sizeof(uint32_t) > data.size()) {
    return false;
  }
  memcpy(&n, data.data() + i, sizeof(n));
  i += sizeof(uint32_t);
  return true;
}

}  // namespace

struct KvEngine::RocksHandle {
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::WriteOptions wo;
  rocksdb::ColumnFamilyHandle* default_cf{nullptr};
  rocksdb::ColumnFamilyHandle* lock_cf{nullptr};
  rocksdb::ColumnFamilyHandle* data_cf{nullptr};
  rocksdb::ColumnFamilyHandle* write_cf{nullptr};
  std::vector<rocksdb::ColumnFamilyHandle*> all;

  rocksdb::ColumnFamilyHandle* cf(CfId id) const {
    switch (id) {
      case CfId::kLock:
        return lock_cf;
      case CfId::kData:
        return data_cf;
      case CfId::kWrite:
        return write_cf;
      case CfId::kDefault:
      default:
        return default_cf;
    }
  }
};

void KvEngine::refresh_write_options() {
  rocks_->wo = rocksdb::WriteOptions();
  // kData：耐久靠 Raft log，apply 异步；仅 kAll 对 RocksDB sync。
  rocks_->wo.sync = raft::kv_apply_sync(sync_);
}

void KvEngine::set_sync_mode(raft::SyncMode m) {
  sync_ = m;
  refresh_write_options();
}

bool KvEngine::rocks_write_sync() const { return rocks_ && rocks_->wo.sync; }

void KvEngine::migrate_legacy_default_cf() {
  rocksdb::WriteBatch put_batch;
  rocksdb::WriteBatch del_batch;
  size_t n = 0;
  std::unique_ptr<rocksdb::Iterator> it(
      rocks_->db->NewIterator(rocksdb::ReadOptions(), rocks_->default_cf));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto key = it->key().ToString();
    const CfId id = classify_key(key);
    if (id == CfId::kDefault) continue;
    put_batch.Put(rocks_->cf(id), key, it->value());
    del_batch.Delete(rocks_->default_cf, key);
    ++n;
  }
  if (n == 0) return;
  auto st = rocks_->db->Write(rocks_->wo, &put_batch);
  if (!st.ok()) {
    SKV_WARN << "cf migrate put failed: " << st.ToString();
    return;
  }
  st = rocks_->db->Write(rocks_->wo, &del_batch);
  if (!st.ok()) {
    SKV_WARN << "cf migrate del failed: " << st.ToString();
    return;
  }
  SKV_INFO << "migrated " << n << " legacy keys to column families shard=" << shard_id_;
}

std::string KvEngine::db_path() const {
  if (store_level_) return data_dir_;
  return (std::filesystem::path(data_dir_) / ("shard_" + std::to_string(shard_id_))).string();
}

KvEngine::KvEngine(std::string data_dir, uint32_t shard_id, raft::SyncMode sync, bool store_level)
    : data_dir_(std::move(data_dir)),
      shard_id_(shard_id),
      store_level_(store_level),
      sync_(sync),
      rocks_(std::make_unique<RocksHandle>()) {
  if (data_dir_.empty()) {
    throw std::invalid_argument("KvEngine: empty data_dir");
  }
  const auto path = std::filesystem::path(db_path());
  std::filesystem::create_directories(path);

  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.compression = rocksdb::kNoCompression;
  options.use_fsync = (sync_ == raft::SyncMode::kAll);
  rocksdb::ColumnFamilyOptions cf_opts(options);
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, cf_opts);
  descriptors.emplace_back(kCfLock, cf_opts);
  descriptors.emplace_back(kCfData, cf_opts);
  descriptors.emplace_back(kCfWrite, cf_opts);

  // 旧库仅有 default：先列出再补齐 descriptor，Open 时 create_missing_column_families
  std::vector<std::string> existing;
  auto list_st = rocksdb::DB::ListColumnFamilies(options, path.string(), &existing);
  if (list_st.ok() && !existing.empty()) {
    // keep descriptors as the full set; create_missing handles new CFs
  }

  rocksdb::DB* raw = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  const rocksdb::Status st =
      rocksdb::DB::Open(options, path.string(), descriptors, &handles, &raw);
  if (!st.ok()) {
    throw std::runtime_error("rocksdb open failed: " + st.ToString());
  }
  if (handles.size() != 4) {
    delete raw;
    throw std::runtime_error("rocksdb open: expected 4 column families");
  }
  rocks_->db.reset(raw);
  rocks_->default_cf = handles[0];
  rocks_->lock_cf = handles[1];
  rocks_->data_cf = handles[2];
  rocks_->write_cf = handles[3];
  rocks_->all = handles;
  refresh_write_options();
  migrate_legacy_default_cf();
  SKV_INFO << "rocksdb opened shard=" << shard_id_ << " path=" << path
           << " store_level=" << (store_level_ ? 1 : 0)
           << " sync=" << raft::sync_mode_name(sync_)
           << " kv_write_sync=" << (raft::kv_apply_sync(sync_) ? 1 : 0)
           << " cfs=default,lock,data,write";
}

KvEngine::~KvEngine() {
  if (!rocks_ || !rocks_->db) return;
  for (auto* h : rocks_->all) {
    if (h) rocks_->db->DestroyColumnFamilyHandle(h);
  }
  rocks_->all.clear();
  rocks_->default_cf = rocks_->lock_cf = rocks_->data_cf = rocks_->write_cf = nullptr;
}

bool KvEngine::put(std::string_view key, std::string_view value, uint64_t revision,
                   int64_t expire_at_ms) {
  auto* cf = rocks_->cf(classify_key(key));
  return rocks_->db
      ->Put(rocks_->wo, cf, std::string(key), encode_kv_payload(revision, expire_at_ms, value))
      .ok();
}

bool KvEngine::write_batch(const std::vector<std::tuple<std::string, std::string, uint64_t>>& puts,
                           const std::vector<std::string>& dels) {
  std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>> ext;
  ext.reserve(puts.size());
  for (const auto& [k, v, rev] : puts) ext.emplace_back(k, v, rev, 0);
  return write_batch(ext, dels);
}

bool KvEngine::write_batch(
    const std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>>& puts,
    const std::vector<std::string>& dels) {
  rocksdb::WriteBatch batch;
  for (const auto& [k, v, rev, exp] : puts) {
    batch.Put(rocks_->cf(classify_key(k)), k, encode_kv_payload(rev, exp, v));
  }
  for (const auto& k : dels) {
    const CfId id = classify_key(k);
    batch.Delete(rocks_->cf(id), k);
    if (id != CfId::kDefault) batch.Delete(rocks_->default_cf, k);
  }
  return rocks_->db->Write(rocks_->wo, &batch).ok();
}

bool KvEngine::del(std::string_view key, uint64_t /*revision*/) {
  const CfId id = classify_key(key);
  auto st = rocks_->db->Delete(rocks_->wo, rocks_->cf(id), std::string(key));
  if (st.ok() && id != CfId::kDefault) {
    // 清理可能残留在 default 的旧副本
    rocks_->db->Delete(rocks_->wo, rocks_->default_cf, std::string(key));
  }
  return st.ok();
}

std::optional<KvRecord> KvEngine::get(std::string_view key, bool hide_expired) const {
  const std::string k(key);
  const CfId id = classify_key(key);
  std::string payload;
  auto st = rocks_->db->Get(rocksdb::ReadOptions(), rocks_->cf(id), k, &payload);
  if (!st.ok() && id != CfId::kDefault) {
    st = rocks_->db->Get(rocksdb::ReadOptions(), rocks_->default_cf, k, &payload);
  }
  if (!st.ok()) return std::nullopt;
  KvRecord r;
  if (!decode_kv_payload(payload, &r)) return std::nullopt;
  if (hide_expired && kv_record_expired(r)) return std::nullopt;
  return r;
}

void KvEngine::compact_for_ttl() {
  if (!rocks_ || !rocks_->db) return;
  // RocksDB 本仓库以 -fno-rtti 构建，无法挂 CompactionFilter 子类；
  // TTL 回收：扫描删除过期项，再 CompactRange 回收空间。
  std::vector<std::string> doomed;
  for_each([&](std::string_view key, const KvRecord& rec) {
    if (kv_record_expired(rec)) doomed.emplace_back(key);
    return true;
  });
  for (const auto& k : doomed) {
    del(k, 0);
  }
  if (!doomed.empty()) {
    rocksdb::CompactRangeOptions cro;
    for (auto* cf : rocks_->all) {
      rocks_->db->CompactRange(cro, cf, nullptr, nullptr);
    }
  }
}

void KvEngine::scan_prefix(
    std::string_view prefix,
    const std::function<bool(std::string_view key, const KvRecord&)>& fn) const {
  const CfId id = classify_key(prefix);
  auto* cf = rocks_->cf(id);
  std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
  const std::string pref(prefix);
  for (it->Seek(pref); it->Valid(); it->Next()) {
    const auto key = it->key().ToString();
    if (key.compare(0, pref.size(), pref) != 0) break;
    KvRecord r;
    if (!decode_kv_payload(it->value().ToString(), &r)) continue;
    if (!fn(key, r)) return;
  }
  // 兼容：prefix 指向 lock/data/write 时也扫一遍 default 旧数据
  if (id != CfId::kDefault) {
    std::unique_ptr<rocksdb::Iterator> it2(
        rocks_->db->NewIterator(rocksdb::ReadOptions(), rocks_->default_cf));
    for (it2->Seek(pref); it2->Valid(); it2->Next()) {
      const auto key = it2->key().ToString();
      if (key.compare(0, pref.size(), pref) != 0) break;
      if (classify_key(key) != id) continue;
      KvRecord r;
      if (!decode_kv_payload(it2->value().ToString(), &r)) continue;
      if (!fn(key, r)) return;
    }
  }
}

void KvEngine::for_each(const std::function<bool(std::string_view key, const KvRecord&)>& fn) const {
  for (auto* cf : rocks_->all) {
    std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      KvRecord r;
      if (!decode_kv_payload(it->value().ToString(), &r)) continue;
      if (!fn(it->key().ToString(), r)) return;
    }
  }
}

// Snapshot wire format: u32 key_len | key | u32 val_len | value_bytes (revision+user value)
// range 全空 = 全表；否则只导出 range.contains(key) 的用户 key（meta 前缀始终跳过）。
std::string KvEngine::create_snapshot(const openkv::KeyRange& range) const {
  // E3：导出前 Flush，缩小与 Checkpoint 之间的不一致窗口
  rocks_->db->FlushWAL(true);
  rocksdb::FlushOptions fo;
  fo.wait = true;
  for (auto* cf : rocks_->all) {
    rocks_->db->Flush(fo, cf);
  }
  const bool filter = !range.start_key.empty() || !range.end_key.empty();
  const std::string meta_prefix = "\xff\xffskv/";
  std::string out;
  for (auto* cf : rocks_->all) {
    std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
    if (filter && !range.start_key.empty()) {
      it->Seek(range.start_key);
    } else {
      it->SeekToFirst();
    }
    for (; it->Valid(); it->Next()) {
      const auto key = it->key().ToString();
      if (key.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
      if (filter && !range.contains(key)) {
        if (!range.end_key.empty() && key >= range.end_key) break;
        continue;
      }
      const auto val = it->value().ToString();
      appendU32(out, static_cast<uint32_t>(key.size()));
      out.append(key);
      appendU32(out, static_cast<uint32_t>(val.size()));
      out.append(val);
    }
  }
  return out;
}

size_t KvEngine::delete_range(const openkv::KeyRange& range) {
  rocksdb::WriteBatch batch;
  size_t n = 0;
  const std::string meta_prefix = "\xff\xffskv/";
  for (auto* cf : rocks_->all) {
    std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
    if (!range.start_key.empty()) {
      it->Seek(range.start_key);
    } else {
      it->SeekToFirst();
    }
    for (; it->Valid(); it->Next()) {
      const auto key = it->key().ToString();
      if (!range.end_key.empty() && key >= range.end_key) break;
      if (key.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
      if (!range.contains(key)) continue;
      batch.Delete(cf, key);
      ++n;
    }
  }
  if (n == 0) return 0;
  auto st = rocks_->db->Write(rocks_->wo, &batch);
  if (!st.ok()) {
    SKV_WARN << "delete_range failed: " << st.ToString();
    return 0;
  }
  return n;
}

void KvEngine::apply_snapshot(std::string_view data, const openkv::KeyRange& range) {
  // 共享引擎：有界 range → 只替换该区间；全空 range → 禁止整库清空（会抹掉兄弟
  // Region，Merge/Place 后 DESTROY+再追快照时常见），仅 upsert。
  const bool filter = !range.start_key.empty() || !range.end_key.empty();
  const std::string meta_prefix = "\xff\xffskv/";
  rocksdb::WriteBatch batch;
  if (!filter) {
    SKV_WARN << "apply_snapshot empty range → upsert-only (skip full clear on shared engine)";
  } else {
    for (auto* cf : rocks_->all) {
      std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
      if (!range.start_key.empty()) {
        it->Seek(range.start_key);
      } else {
        it->SeekToFirst();
      }
      for (; it->Valid(); it->Next()) {
        const auto key = it->key().ToString();
        if (!range.end_key.empty() && key >= range.end_key) break;
        if (key.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
        if (!range.contains(key)) continue;
        batch.Delete(cf, it->key());
      }
    }
  }
  size_t i = 0;
  while (i < data.size()) {
    uint32_t key_len = 0;
    uint32_t val_len = 0;
    if (!readU32(data, i, key_len) || i + key_len + sizeof(uint32_t) > data.size()) {
      break;
    }
    const std::string key(data.substr(i, key_len));
    i += key_len;
    if (!readU32(data, i, val_len) || i + val_len > data.size()) {
      break;
    }
    batch.Put(rocks_->cf(classify_key(key)), key, std::string(data.substr(i, val_len)));
    i += val_len;
  }
  rocks_->db->Write(rocks_->wo, &batch);
}

bool KvEngine::create_checkpoint(std::string_view dest_dir) const {
  const std::filesystem::path dest(dest_dir);
  try {
    // RocksDB CreateCheckpoint 要求路径不存在（由其自行创建）
    if (std::filesystem::exists(dest)) {
      if (!std::filesystem::is_directory(dest) || !std::filesystem::is_empty(dest)) {
        SKV_WARN << "checkpoint dest not empty path=" << dest;
        return false;
      }
      std::filesystem::remove(dest);
    }
    // libc++: path("foo").has_parent_path()==true 但 parent_path() 为空串，create_directories("") 会抛。
    if (dest.has_parent_path() && !dest.parent_path().empty()) {
      std::filesystem::create_directories(dest.parent_path());
    }
  } catch (const std::exception& e) {
    SKV_WARN << "checkpoint prepare: " << e.what();
    return false;
  }
  rocks_->db->FlushWAL(true);
  rocksdb::FlushOptions fo;
  fo.wait = true;
  for (auto* cf : rocks_->all) {
    auto st = rocks_->db->Flush(fo, cf);
    if (!st.ok()) {
      SKV_WARN << "checkpoint flush: " << st.ToString();
      return false;
    }
  }
  rocksdb::Checkpoint* raw = nullptr;
  auto st = rocksdb::Checkpoint::Create(rocks_->db.get(), &raw);
  if (!st.ok() || !raw) {
    SKV_WARN << "Checkpoint::Create: " << st.ToString();
    return false;
  }
  std::unique_ptr<rocksdb::Checkpoint> ckpt(raw);
  st = ckpt->CreateCheckpoint(dest.string());
  if (!st.ok()) {
    SKV_WARN << "CreateCheckpoint: " << st.ToString();
    return false;
  }
  SKV_INFO << "checkpoint ok shard=" << shard_id_ << " path=" << dest;
  return true;
}

bool KvEngine::restore_from_checkpoint(std::string_view src_dir) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.compression = rocksdb::kNoCompression;
  rocksdb::ColumnFamilyOptions cf_opts(options);
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, cf_opts);
  descriptors.emplace_back(kCfLock, cf_opts);
  descriptors.emplace_back(kCfData, cf_opts);
  descriptors.emplace_back(kCfWrite, cf_opts);

  rocksdb::DB* raw = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  auto st = rocksdb::DB::OpenForReadOnly(options, std::string(src_dir), descriptors, &handles,
                                         &raw, /*error_if_wal_file_exists=*/false);
  if (!st.ok()) {
    // 兼容仅 default 的旧 checkpoint
    handles.clear();
    st = rocksdb::DB::OpenForReadOnly(options, std::string(src_dir), &raw);
    if (!st.ok()) {
      SKV_WARN << "restore open checkpoint: " << st.ToString();
      return false;
    }
  }
  std::unique_ptr<rocksdb::DB> src(raw);

  rocksdb::WriteBatch clear;
  for (auto* cf : rocks_->all) {
    std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) clear.Delete(cf, it->key());
  }
  st = rocks_->db->Write(rocks_->wo, &clear);
  if (!st.ok()) {
    for (auto* h : handles) src->DestroyColumnFamilyHandle(h);
    SKV_WARN << "restore clear: " << st.ToString();
    return false;
  }

  rocksdb::WriteBatch put;
  size_t n = 0;
  auto copy_cf = [&](rocksdb::ColumnFamilyHandle* src_cf) {
    std::unique_ptr<rocksdb::Iterator> it(src->NewIterator(rocksdb::ReadOptions(), src_cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const auto key = it->key().ToString();
      put.Put(rocks_->cf(classify_key(key)), key, it->value());
      ++n;
    }
  };
  if (handles.size() == 4) {
    for (auto* h : handles) copy_cf(h);
  } else {
    copy_cf(src->DefaultColumnFamily());
  }
  st = rocks_->db->Write(rocks_->wo, &put);
  for (auto* h : handles) src->DestroyColumnFamilyHandle(h);
  if (!st.ok()) {
    SKV_WARN << "restore put: " << st.ToString();
    return false;
  }
  SKV_INFO << "restore_from_checkpoint shard=" << shard_id_ << " keys=" << n
           << " src=" << src_dir;
  return true;
}

namespace {
constexpr std::string_view kMetaPrefix = "\xff\xffskv/";

std::string user_of_phys(std::string_view phys) {
  if (phys.compare(0, kMetaPrefix.size(), kMetaPrefix) == 0) return {};
  const auto pos = phys.find('\0');
  return std::string(pos == std::string_view::npos ? phys : phys.substr(0, pos));
}
}  // namespace

namespace {

/// 用户 KeyRange → RocksDB Range（物理前缀；end 空用极大 sentinel）。
rocksdb::Range rocks_range_for(const openkv::KeyRange& range, std::string* start_hold,
                               std::string* limit_hold) {
  *start_hold = range.start_key;
  if (range.end_key.empty()) {
    // 半开 +∞：用高字节前缀作 exclusive limit（跳过引擎 meta `\xff\xffskv/`）。
    *limit_hold = std::string("\xff\xfe");
  } else {
    *limit_hold = range.end_key;
  }
  return rocksdb::Range(*start_hold, *limit_hold);
}

uint64_t approx_sizes_sum(rocksdb::DB* db, const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
                          const rocksdb::Range& r) {
  uint64_t total = 0;
  const auto kIncl = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES |
                     rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;
  for (auto* cf : cfs) {
    if (!cf) continue;
    uint64_t sz = 0;
    db->GetApproximateSizes(cf, &r, 1, &sz, kIncl);
    total += sz;
  }
  return total;
}

}  // namespace

size_t KvEngine::approx_bytes(const openkv::KeyRange& range) const {
  std::string start_hold, limit_hold;
  const auto r = rocks_range_for(range, &start_hold, &limit_hold);
  return static_cast<size_t>(approx_sizes_sum(rocks_->db.get(), rocks_->all, r));
}

size_t KvEngine::approx_key_count(const openkv::KeyRange& range) const {
  auto exact_unique = [&](size_t cap) -> size_t {
    std::vector<std::string> users;
    users.reserve(std::min(cap, size_t{4096}));
    for_each([&](std::string_view phys, const KvRecord&) {
      auto u = user_of_phys(phys);
      if (u.empty() || !range.contains(u)) return true;
      users.push_back(std::move(u));
      return users.size() < cap;
    });
    std::sort(users.begin(), users.end());
    users.erase(std::unique(users.begin(), users.end()), users.end());
    return users.size();
  };

  // 小库（含单测 / 早期增长）：精确唯一用户 key，避免 memtable 上 GetApproximateSizes=0。
  constexpr size_t kExactCap = 8192;
  uint64_t est_keys = 0;
  rocks_->db->GetIntProperty(rocks_->data_cf ? rocks_->data_cf : rocks_->default_cf,
                             "rocksdb.estimate-num-keys", &est_keys);
  if (est_keys == 0) rocks_->db->GetIntProperty("rocksdb.estimate-num-keys", &est_keys);
  if (est_keys < kExactCap) return exact_unique(kExactCap);

  const bool full = range.start_key.empty() && range.end_key.empty();
  if (full) return static_cast<size_t>(est_keys);

  std::string start_hold, limit_hold, full_start, full_limit;
  const auto rr = rocks_range_for(range, &start_hold, &limit_hold);
  openkv::KeyRange whole;
  const auto wr = rocks_range_for(whole, &full_start, &full_limit);
  const uint64_t range_sz = approx_sizes_sum(rocks_->db.get(), rocks_->all, rr);
  const uint64_t whole_sz = approx_sizes_sum(rocks_->db.get(), rocks_->all, wr);
  if (whole_sz == 0) return exact_unique(4096);
  return static_cast<size_t>((est_keys * range_sz) / whole_sz);
}

std::string KvEngine::pick_split_key(const openkv::KeyRange& range) const {
  // 采样用户 key（上限 512），取中位；避免全表排序。
  constexpr size_t kMaxSample = 512;
  std::vector<std::string> users;
  users.reserve(kMaxSample);
  size_t seen = 0;
  for_each([&](std::string_view phys, const KvRecord&) {
    auto u = user_of_phys(phys);
    if (u.empty() || !range.contains(u)) return true;
    ++seen;
    // 蓄水池：均匀保留最多 kMaxSample 个
    if (users.size() < kMaxSample) {
      users.push_back(std::move(u));
    } else {
      const size_t j = seen == 0 ? 0 : (seen - 1) % kMaxSample;
      if (j < users.size()) users[j] = std::move(u);
    }
    return true;
  });
  if (users.size() < 2) return {};
  std::sort(users.begin(), users.end());
  users.erase(std::unique(users.begin(), users.end()), users.end());
  if (users.size() < 2) return {};
  const std::string& mid = users[users.size() / 2];
  if (!range.can_split_at(mid)) return {};
  return mid;
}

size_t KvEngine::migrate_ge(KvEngine& dst, std::string_view split_key) {
  const std::string meta_prefix = "\xff\xffskv/";
  const std::string sk(split_key);
  // 单 Store 共享同一 RocksDB：物理搬迁无意义（KeyRange 隔离）；只统计右片 key 数。
  if (this == &dst || (rocks_ && dst.rocks_ && rocks_->db == dst.rocks_->db)) {
    size_t n = 0;
    for (auto* cf : rocks_->all) {
      std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
      for (it->Seek(sk); it->Valid(); it->Next()) {
        const auto key = it->key().ToString();
        if (key.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
        ++n;
      }
    }
    return n;
  }
  rocksdb::WriteBatch put_batch;
  rocksdb::WriteBatch del_batch;
  size_t moved = 0;
  for (auto* cf : rocks_->all) {
    std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions(), cf));
    for (it->Seek(sk); it->Valid(); it->Next()) {
      const auto key = it->key().ToString();
      if (key.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
      const auto val = it->value().ToString();
      KvRecord r;
      if (!decode_kv_payload(val, &r)) continue;
      const CfId id = classify_key(key);
      put_batch.Put(dst.rocks_->cf(id), key, val);
      del_batch.Delete(cf, key);
      ++moved;
    }
  }
  if (moved == 0) return 0;
  auto st = dst.rocks_->db->Write(dst.rocks_->wo, &put_batch);
  if (!st.ok()) {
    SKV_WARN << "migrate_ge dst WriteBatch failed: " << st.ToString();
    return 0;
  }
  st = rocks_->db->Write(rocks_->wo, &del_batch);
  if (!st.ok()) {
    SKV_WARN << "migrate_ge src delete batch failed: " << st.ToString();
  }
  return moved;
}

size_t KvEngine::migrate_all(KvEngine& dst) {
  return migrate_ge(dst, std::string());
}

#else
// =============================================================================
// 实现分支 B：内存后端（默认 stub 构建）
// 行为与 RocksDB 分支在 API 上对齐，但快照编码故意用「可读 revision 文本」，
// 便于单测与调试；两端快照格式不互通（同进程内一致即可）。
// =============================================================================

std::string KvEngine::db_path() const {
  if (store_level_) return data_dir_;
  return (std::filesystem::path(data_dir_) / ("shard_" + std::to_string(shard_id_))).string();
}

// ---------------------------------------------------------------------------
// 构造：仍创建目录，保持与 RocksDB 路径布局一致（便于日后切换后端）
// ---------------------------------------------------------------------------
KvEngine::KvEngine(std::string data_dir, uint32_t shard_id, raft::SyncMode sync, bool store_level)
    : data_dir_(std::move(data_dir)), shard_id_(shard_id), store_level_(store_level), sync_(sync) {
  if (data_dir_.empty()) {
    throw std::invalid_argument("KvEngine: empty data_dir");
  }
  std::filesystem::create_directories(db_path());
  SKV_WARN << "RocksDB not linked; using in-memory engine for shard=" << shard_id_
           << " store_level=" << (store_level_ ? 1 : 0)
           << " sync=" << raft::sync_mode_name(sync_);
}

KvEngine::~KvEngine() = default;

void KvEngine::set_sync_mode(raft::SyncMode m) { sync_ = m; }

bool KvEngine::rocks_write_sync() const { return false; }

// ---------------------------------------------------------------------------
// put：覆盖写；持锁，保证与 get/del/snapshot 互斥
// ---------------------------------------------------------------------------
bool KvEngine::put(std::string_view key, std::string_view value, uint64_t revision,
                   int64_t expire_at_ms) {
  std::lock_guard<std::mutex> lock(mu_);
  mem_[std::string(key)] = KvRecord{std::string(value), revision, expire_at_ms};
  return true;
}

bool KvEngine::write_batch(const std::vector<std::tuple<std::string, std::string, uint64_t>>& puts,
                           const std::vector<std::string>& dels) {
  std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>> ext;
  ext.reserve(puts.size());
  for (const auto& [k, v, rev] : puts) ext.emplace_back(k, v, rev, 0);
  return write_batch(ext, dels);
}

bool KvEngine::write_batch(
    const std::vector<std::tuple<std::string, std::string, uint64_t, int64_t>>& puts,
    const std::vector<std::string>& dels) {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto& [k, v, rev, exp] : puts) {
    mem_[k] = KvRecord{v, rev, exp};
  }
  for (const auto& k : dels) mem_.erase(k);
  return true;
}

// ---------------------------------------------------------------------------
// del：返回是否确实删除了已有 key（erase 计数 > 0）
// ---------------------------------------------------------------------------
bool KvEngine::del(std::string_view key, uint64_t /*revision*/) {
  std::lock_guard<std::mutex> lock(mu_);
  return mem_.erase(std::string(key)) > 0;
}

// ---------------------------------------------------------------------------
// get：拷贝返回 KvRecord；调用方拿到的是快照副本，与后续写互不影响
// ---------------------------------------------------------------------------
std::optional<KvRecord> KvEngine::get(std::string_view key, bool hide_expired) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = mem_.find(std::string(key));
  if (it == mem_.end()) {
    return std::nullopt;
  }
  if (hide_expired && kv_record_expired(it->second)) return std::nullopt;
  return it->second;
}

void KvEngine::compact_for_ttl() {
  std::lock_guard<std::mutex> lock(mu_);
  const int64_t now = wall_ms_now_local();
  for (auto it = mem_.begin(); it != mem_.end();) {
    if (kv_record_expired(it->second, now)) {
      it = mem_.erase(it);
    } else {
      ++it;
    }
  }
}

void KvEngine::scan_prefix(
    std::string_view prefix,
    const std::function<bool(std::string_view key, const KvRecord&)>& fn) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::pair<std::string, KvRecord>> items;
  items.reserve(mem_.size());
  for (const auto& [k, v] : mem_) {
    if (k.compare(0, prefix.size(), prefix) == 0) {
      items.emplace_back(k, v);
    }
  }
  std::sort(items.begin(), items.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  for (const auto& [k, v] : items) {
    if (!fn(k, v)) break;
  }
}

void KvEngine::for_each(const std::function<bool(std::string_view key, const KvRecord&)>& fn) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::pair<std::string, KvRecord>> items(mem_.begin(), mem_.end());
  std::sort(items.begin(), items.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  for (const auto& [k, v] : items) {
    if (!fn(k, v)) break;
  }
}

bool KvEngine::create_checkpoint(std::string_view dest_dir) const {
  try {
    std::filesystem::create_directories(dest_dir);
    auto snap = create_snapshot();
    std::ofstream out(std::filesystem::path(dest_dir) / "mem.snapshot", std::ios::binary);
    out.write(snap.data(), static_cast<std::streamsize>(snap.size()));
    return out.good();
  } catch (...) {
    return false;
  }
}

bool KvEngine::restore_from_checkpoint(std::string_view src_dir) {
  try {
    std::ifstream in(std::filesystem::path(src_dir) / "mem.snapshot", std::ios::binary);
    if (!in) return false;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    apply_snapshot(data);
    return true;
  } catch (...) {
    return false;
  }
}

// ---------------------------------------------------------------------------
// create_snapshot（内存格式）：
//   key \0 revision_decimal \0 value \0  （重复）
//
// 与 RocksDB 分支不同：revision 用十进制字符串，方便肉眼查看 dump。
// value 仍可为任意字节（含 '\0' 时会截断——demo 限制；生产请用长度前缀）。
// ---------------------------------------------------------------------------
std::string KvEngine::create_snapshot(const openkv::KeyRange& range) const {
  std::lock_guard<std::mutex> lock(mu_);
  const bool filter = !range.start_key.empty() || !range.end_key.empty();
  const std::string meta_prefix = "\xff\xffskv/";
  std::ostringstream oss;
  for (const auto& [k, v] : mem_) {
    if (k.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
    if (filter && !range.contains(k)) continue;
    oss << k << '\0' << v.revision << '\0' << v.value << '\0';
  }
  return oss.str();
}

size_t KvEngine::delete_range(const openkv::KeyRange& range) {
  std::lock_guard<std::mutex> lock(mu_);
  const std::string meta_prefix = "\xff\xffskv/";
  size_t n = 0;
  for (auto it = mem_.begin(); it != mem_.end();) {
    if (it->first.compare(0, meta_prefix.size(), meta_prefix) == 0) {
      ++it;
      continue;
    }
    if (!range.contains(it->first)) {
      ++it;
      continue;
    }
    it = mem_.erase(it);
    ++n;
  }
  return n;
}

// ---------------------------------------------------------------------------
// apply_snapshot：有界 range 只替换该区间；全空 → upsert-only（共享引擎安全）。
// 字段顺序：key | revision | value（三段 '\0' 分隔）
// ---------------------------------------------------------------------------
void KvEngine::apply_snapshot(std::string_view data, const openkv::KeyRange& range) {
  std::lock_guard<std::mutex> lock(mu_);
  const bool filter = !range.start_key.empty() || !range.end_key.empty();
  const std::string meta_prefix = "\xff\xffskv/";
  if (!filter) {
    SKV_WARN << "apply_snapshot(mem) empty range → upsert-only";
  } else {
    for (auto it = mem_.begin(); it != mem_.end();) {
      if (it->first.compare(0, meta_prefix.size(), meta_prefix) == 0) {
        ++it;
        continue;
      }
      if (!range.contains(it->first)) {
        ++it;
        continue;
      }
      it = mem_.erase(it);
    }
  }
  size_t i = 0;
  while (i < data.size()) {
    auto a = data.find('\0', i);       // key 结束
    if (a == std::string_view::npos) {
      break;
    }
    auto b = data.find('\0', a + 1);   // revision 结束
    if (b == std::string_view::npos) {
      break;
    }
    auto c = data.find('\0', b + 1);   // value 结束
    if (c == std::string_view::npos) {
      break;
    }
    KvRecord r;
    // revision 字段是十进制文本
    r.revision = std::stoull(std::string(data.substr(a + 1, b - a - 1)));
    r.value = std::string(data.substr(b + 1, c - b - 1));
    mem_[std::string(data.substr(i, a - i))] = std::move(r);
    i = c + 1;
  }
}

size_t KvEngine::approx_key_count(const openkv::KeyRange& range) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> users;
  users.reserve(mem_.size());
  for (const auto& [k, _] : mem_) {
    const auto pos = k.find('\0');
    std::string u = pos == std::string::npos ? k : k.substr(0, pos);
    if (u.compare(0, 8, "\xff\xffskv/") == 0) continue;
    if (!range.contains(u)) continue;
    users.push_back(std::move(u));
  }
  std::sort(users.begin(), users.end());
  users.erase(std::unique(users.begin(), users.end()), users.end());
  return users.size();
}

size_t KvEngine::approx_bytes(const openkv::KeyRange& range) const {
  std::lock_guard<std::mutex> lock(mu_);
  size_t n = 0;
  for (const auto& [k, r] : mem_) {
    const auto pos = k.find('\0');
    std::string_view u = pos == std::string::npos ? std::string_view(k)
                                                  : std::string_view(k).substr(0, pos);
    if (u.compare(0, 8, "\xff\xffskv/") == 0) continue;
    if (!range.contains(u)) continue;
    n += k.size() + r.value.size() + sizeof(r.revision);
  }
  return n;
}

std::string KvEngine::pick_split_key(const openkv::KeyRange& range) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (mem_.size() < 2) return {};
  std::vector<std::string> users;
  users.reserve(mem_.size());
  for (const auto& [k, _] : mem_) {
    const auto pos = k.find('\0');
    std::string u = pos == std::string::npos ? k : k.substr(0, pos);
    if (u.compare(0, 8, "\xff\xffskv/") == 0) continue;
    if (!range.contains(u)) continue;
    users.push_back(std::move(u));
  }
  std::sort(users.begin(), users.end());
  users.erase(std::unique(users.begin(), users.end()), users.end());
  if (users.size() < 2) return {};
  const std::string& mid = users[users.size() / 2];
  if (!range.can_split_at(mid)) return {};
  return mid;
}

size_t KvEngine::migrate_ge(KvEngine& dst, std::string_view split_key) {
  const std::string meta_prefix = "\xff\xffskv/";
  if (this == &dst) {
    size_t n = 0;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [k, _] : mem_) {
      if (k.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
      if (k >= split_key) ++n;
    }
    return n;
  }
  // 先收集再写入 dst，最后再从 src 删除，避免删后写失败丢数据。
  std::vector<std::pair<std::string, KvRecord>> moved;
  {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [k, r] : mem_) {
      if (k.compare(0, meta_prefix.size(), meta_prefix) == 0) continue;
      if (k >= split_key) moved.emplace_back(k, r);
    }
  }
  for (const auto& [k, r] : moved) {
    dst.put(k, r.value, r.revision, r.expire_at_ms);
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [k, _] : moved) mem_.erase(k);
  }
  return moved.size();
}

size_t KvEngine::migrate_all(KvEngine& dst) {
  return migrate_ge(dst, std::string());
}

#endif  // OPENKV_HAS_ROCKSDB

// =============================================================================
// ShardEngineManager：单 Store 一份 RocksDB（data_dir/kv）；Region 靠 KeyRange。
// =============================================================================

ShardEngineManager::ShardEngineManager(std::string data_dir, raft::SyncMode sync)
    : data_dir_(std::move(data_dir)), sync_(sync) {}

void ShardEngineManager::ensure_shared_locked() {
  if (shared_) return;
  const auto kv_root = (std::filesystem::path(data_dir_) / "kv").string();
  shared_ = std::make_shared<KvEngine>(kv_root, /*shard_id=*/0, sync_, /*store_level=*/true);
  SKV_INFO << "store-level KvEngine ready path=" << kv_root;
}

void ShardEngineManager::migrate_legacy_shards_if_needed() {
  std::lock_guard<std::mutex> lock(mu_);
  ensure_shared_locked();
  namespace fs = std::filesystem;
  if (!fs::exists(data_dir_) || !fs::is_directory(data_dir_)) return;
  size_t migrated_dirs = 0;
  size_t migrated_keys = 0;
  for (const auto& ent : fs::directory_iterator(data_dir_)) {
    if (!ent.is_directory()) continue;
    const auto name = ent.path().filename().string();
    if (name.rfind("shard_", 0) != 0) continue;
    uint32_t sid = 0;
    try {
      sid = static_cast<uint32_t>(std::stoul(name.substr(6)));
    } catch (...) {
      continue;
    }
    try {
      KvEngine legacy(data_dir_, sid, sync_, /*store_level=*/false);
      legacy.for_each([&](std::string_view key, const KvRecord& rec) {
        if (!shared_->get(key)) {
          shared_->put(key, rec.value, rec.revision);
          ++migrated_keys;
        }
        return true;
      });
      const auto bak = ent.path().string() + ".migrated";
      std::error_code ec;
      fs::rename(ent.path(), bak, ec);
      if (ec) {
        SKV_WARN << "migrate rename failed " << ent.path() << " -> " << bak
                 << " err=" << ec.message();
      } else {
        ++migrated_dirs;
        SKV_INFO << "migrated legacy " << name << " -> kv keys~" << migrated_keys;
      }
    } catch (const std::exception& e) {
      SKV_WARN << "migrate skip " << name << ": " << e.what();
    }
  }
  if (migrated_dirs > 0) {
    SKV_INFO << "legacy shard migrate done dirs=" << migrated_dirs << " keys=" << migrated_keys;
  }
}

KvEngine& ShardEngineManager::get_or_create(uint32_t shard_id) {
  std::lock_guard<std::mutex> lock(mu_);
  ensure_shared_locked();
  engines_[shard_id] = shared_;
  return *shared_;
}

KvEngine* ShardEngineManager::get(uint32_t shard_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = engines_.find(shard_id);
  if (it != engines_.end() && it->second) return it->second.get();
  if (shared_) return shared_.get();
  return nullptr;
}

bool ShardEngineManager::share_with(uint32_t shard_id, uint32_t source_shard) {
  std::lock_guard<std::mutex> lock(mu_);
  ensure_shared_locked();
  (void)source_shard;
  engines_[shard_id] = shared_;
  return true;
}

bool ShardEngineManager::shares_engine(uint32_t a, uint32_t b) const {
  std::lock_guard<std::mutex> lock(mu_);
  (void)a;
  (void)b;
  return true;  // 单 Store：任意 Region 同引擎
}

size_t ShardEngineManager::release(uint32_t shard_id) {
  std::lock_guard<std::mutex> lock(mu_);
  engines_.erase(shard_id);
  // 单 Store：永不因单个 Region DESTROY 删掉共享 RocksDB
  return shared_ ? shared_.use_count() : 0;
}

}  // namespace openkv::storage
