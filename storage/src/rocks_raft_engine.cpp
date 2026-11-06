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

// @file rocks_raft_engine.cpp
// RocksDB 后端 RaftEngine（WriteBatch 原子写 state+log）。

#include "opskv/storage/rocks_raft_engine.hpp"

#include "opskv/logging.hpp"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <vector>

namespace opskv::storage {
namespace fs = std::filesystem;

namespace {

void put_be64(std::string* out, uint64_t v) {
  char b[8];
  for (int i = 7; i >= 0; --i) {
    b[i] = static_cast<char>(v & 0xff);
    v >>= 8;
  }
  out->append(b, 8);
}

}  // namespace

struct RocksRaftEngine::RocksHandle {
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::WriteOptions wo;
};

std::string RocksRaftEngine::state_key(uint64_t group_id) {
  std::string k;
  k.push_back('\x01');
  put_be64(&k, group_id);
  return k;
}

std::string RocksRaftEngine::log_key(uint64_t group_id, uint64_t index) {
  std::string k;
  k.push_back('\x02');
  put_be64(&k, group_id);
  put_be64(&k, index);
  return k;
}

std::string RocksRaftEngine::log_prefix(uint64_t group_id) {
  std::string k;
  k.push_back('\x02');
  put_be64(&k, group_id);
  return k;
}

class RocksLogBatch final : public raft::RaftLogBatch {
 public:
  void append(uint64_t raft_group_id, std::vector<raft::LogEntry> entries) override {
    for (auto& e : entries) {
      ops_.push_back(Op{Op::kPutLog, raft_group_id, e.index, raft::encode_log_entry(e), {}});
    }
  }
  void cut_logs(uint64_t raft_group_id, uint64_t from, uint64_t to) override {
    if (to <= from) return;
    ops_.push_back(Op{Op::kCut, raft_group_id, from, {}, to});
  }
  void put_raft_state(uint64_t raft_group_id, const raft::RaftLocalState& state) override {
    ops_.push_back(Op{Op::kPutState, raft_group_id, 0, raft::encode_raft_local_state(state), {}});
  }
  bool empty() const override { return ops_.empty(); }

  struct Op {
    enum Kind { kPutLog, kPutState, kCut } kind;
    uint64_t group{0};
    uint64_t index{0};   // put log index / cut from
    std::string value;   // encoded
    uint64_t cut_to{0};  // cut end
  };
  std::vector<Op> ops_;
};

RocksRaftEngine::RocksRaftEngine(std::string db_path, raft::SyncMode sync)
    : path_(std::move(db_path)), sync_(sync) {
  fs::create_directories(path_);
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.compression = rocksdb::kNoCompression;
  rocksdb::DB* raw = nullptr;
  auto st = rocksdb::DB::Open(options, path_, &raw);
  if (!st.ok()) {
    throw std::runtime_error(std::string("RocksRaftEngine open failed: ") + st.ToString());
  }
  rocks_ = std::make_unique<RocksHandle>();
  rocks_->db.reset(raw);
  set_sync_mode(sync_);
  SKV_INFO << "rocks raft engine opened path=" << path_ << " sync=" << raft::sync_mode_name(sync_);
}

RocksRaftEngine::~RocksRaftEngine() = default;

void RocksRaftEngine::set_sync_mode(raft::SyncMode m) {
  std::lock_guard<std::mutex> lock(mu_);
  sync_ = m;
  if (!rocks_) return;
  rocks_->wo = rocksdb::WriteOptions();
  rocks_->wo.sync = (m != raft::SyncMode::kNone);
  rocks_->wo.disableWAL = false;
}

std::unique_ptr<raft::RaftLogBatch> RocksRaftEngine::make_log_batch(size_t) {
  return std::make_unique<RocksLogBatch>();
}

size_t RocksRaftEngine::consume(raft::RaftLogBatch& batch, bool sync) {
  auto* rb = dynamic_cast<RocksLogBatch*>(&batch);
  if (!rb || rb->empty() || !rocks_) return 0;

  rocksdb::WriteBatch wb;
  size_t bytes = 0;
  for (const auto& op : rb->ops_) {
    switch (op.kind) {
      case RocksLogBatch::Op::kPutState: {
        auto key = state_key(op.group);
        wb.Put(key, op.value);
        bytes += key.size() + op.value.size();
        break;
      }
      case RocksLogBatch::Op::kPutLog: {
        auto key = log_key(op.group, op.index);
        wb.Put(key, op.value);
        bytes += key.size() + op.value.size();
        break;
      }
      case RocksLogBatch::Op::kCut: {
        for (uint64_t i = op.index; i < op.cut_to; ++i) {
          wb.Delete(log_key(op.group, i));
          bytes += 16;
        }
        break;
      }
    }
  }

  rocksdb::WriteOptions wo;
  {
    std::lock_guard<std::mutex> lock(mu_);
    wo = rocks_->wo;
  }
  if (sync && sync_ == raft::SyncMode::kNone) {
    // 调用方要求 sync，即使引擎默认 none
    wo.sync = true;
  } else if (!sync) {
    wo.sync = false;
  }

  auto st = rocks_->db->Write(wo, &wb);
  if (!st.ok()) {
    SKV_WARN << "rocks raft consume failed: " << st.ToString();
    return 0;
  }
  rb->ops_.clear();
  return bytes;
}

std::optional<raft::RaftLocalState> RocksRaftEngine::get_raft_state(uint64_t raft_group_id) const {
  if (!rocks_) return std::nullopt;
  std::string val;
  auto st = rocks_->db->Get(rocksdb::ReadOptions(), state_key(raft_group_id), &val);
  if (!st.ok()) return std::nullopt;
  raft::RaftLocalState out;
  if (!raft::decode_raft_local_state(val, &out)) return std::nullopt;
  return out;
}

std::optional<raft::LogEntry> RocksRaftEngine::get_entry(uint64_t raft_group_id,
                                                        uint64_t index) const {
  if (!rocks_) return std::nullopt;
  std::string val;
  auto st = rocks_->db->Get(rocksdb::ReadOptions(), log_key(raft_group_id, index), &val);
  if (!st.ok()) return std::nullopt;
  raft::LogEntry e;
  if (!raft::decode_log_entry(val, &e)) return std::nullopt;
  return e;
}

size_t RocksRaftEngine::fetch_entries_to(uint64_t raft_group_id, uint64_t begin, uint64_t end,
                                         size_t max_size, std::vector<raft::LogEntry>* to) const {
  if (!to || !rocks_ || end <= begin) return 0;
  std::string prefix = log_prefix(raft_group_id);
  std::string start = log_key(raft_group_id, begin);
  std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions()));
  size_t n = 0;
  size_t bytes = 0;
  for (it->Seek(start); it->Valid(); it->Next()) {
    auto k = it->key();
    if (k.size() < prefix.size() ||
        std::memcmp(k.data(), prefix.data(), prefix.size()) != 0) {
      break;
    }
    // parse index from key
    if (k.size() != 1 + 8 + 8) continue;
    uint64_t idx = 0;
    for (int i = 0; i < 8; ++i) idx = (idx << 8) | static_cast<uint8_t>(k[1 + 8 + i]);
    if (idx >= end) break;
    if (idx < begin) continue;
    raft::LogEntry e;
    if (!raft::decode_log_entry(it->value().ToString(), &e)) continue;
    if (bytes + e.data.size() > max_size && n > 0) break;
    to->push_back(std::move(e));
    bytes += to->back().data.size();
    ++n;
  }
  return n;
}

size_t RocksRaftEngine::gc(uint64_t raft_group_id, uint64_t from, uint64_t to) {
  auto batch = make_log_batch();
  batch->cut_logs(raft_group_id, from, to);
  return consume(*batch, sync_ != raft::SyncMode::kNone);
}

void RocksRaftEngine::clean(uint64_t raft_group_id) {
  if (!rocks_) return;
  rocksdb::WriteBatch wb;
  wb.Delete(state_key(raft_group_id));
  std::string prefix = log_prefix(raft_group_id);
  std::unique_ptr<rocksdb::Iterator> it(rocks_->db->NewIterator(rocksdb::ReadOptions()));
  for (it->Seek(prefix); it->Valid(); it->Next()) {
    auto k = it->key();
    if (k.size() < prefix.size() ||
        std::memcmp(k.data(), prefix.data(), prefix.size()) != 0) {
      break;
    }
    wb.Delete(k);
  }
  rocksdb::WriteOptions wo;
  {
    std::lock_guard<std::mutex> lock(mu_);
    wo = rocks_->wo;
  }
  auto st = rocks_->db->Write(wo, &wb);
  if (!st.ok()) {
    SKV_WARN << "rocks raft clean failed group=" << raft_group_id << " " << st.ToString();
  }
}

}  // namespace opskv::storage
