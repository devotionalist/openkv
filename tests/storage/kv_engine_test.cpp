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

// @file kv_engine_test.cpp
// 测试 KvEngine put/get/del、快照往返与 ShardEngineManager 多分片。

#include "opskv/storage/kv_engine.hpp"
#include "opskv/region.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::storage::KvEngine;
using opskv::storage::ShardEngineManager;

class KvEngineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("skv_kv_" + std::to_string(::getpid()) + "_" +
                                        std::to_string(counter_++));
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }
  fs::path dir_;
  static inline int counter_{0};
};

TEST_F(KvEngineTest, PutGetDelete) {
  KvEngine eng(dir_.string(), 0);
  EXPECT_TRUE(eng.put("k", "v", 1));
  auto r = eng.get("k");
  ASSERT_TRUE(r);
  EXPECT_EQ(r->value, "v");
  EXPECT_EQ(r->revision, 1u);
  EXPECT_TRUE(eng.del("k", 2));
  EXPECT_FALSE(eng.get("k"));
}

TEST_F(KvEngineTest, MissingKey) {
  KvEngine eng(dir_.string(), 1);
  EXPECT_FALSE(eng.get("nope"));
}

TEST_F(KvEngineTest, SnapshotRoundTrip) {
  KvEngine eng(dir_.string(), 2);
  eng.put("a", "1", 10);
  eng.put("b", "2", 11);
  auto snap = eng.create_snapshot();
  EXPECT_FALSE(snap.empty());

  KvEngine eng2((dir_ / "copy").string(), 3);
  eng2.apply_snapshot(snap);
  auto a = eng2.get("a");
  auto b = eng2.get("b");
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_EQ(a->value, "1");
  EXPECT_EQ(b->value, "2");
}

// 共享引擎：按 Region 区间 apply 不得抹掉兄弟 Region 的 key（it-scale-live aaa 丢失根因）。
TEST_F(KvEngineTest, SnapshotRangePreservesSibling) {
  KvEngine eng(dir_.string(), 4);
  ASSERT_TRUE(eng.put("aaa", "1", 1));
  ASSERT_TRUE(eng.put("zzz", "2", 2));
  opskv::KeyRange left;
  left.end_key = "m";  // [-, m)
  auto snap_left = eng.create_snapshot(left);
  EXPECT_FALSE(snap_left.empty());

  KvEngine shared((dir_ / "shared").string(), 5);
  ASSERT_TRUE(shared.put("aaa", "stale", 9));
  ASSERT_TRUE(shared.put("zzz", "keep", 9));
  shared.apply_snapshot(snap_left, left);

  auto a = shared.get("aaa");
  auto z = shared.get("zzz");
  ASSERT_TRUE(a);
  ASSERT_TRUE(z);
  EXPECT_EQ(a->value, "1");
  EXPECT_EQ(z->value, "keep");
}

TEST_F(KvEngineTest, ShardManager) {
  ShardEngineManager mgr(dir_.string());
  EXPECT_EQ(mgr.get(0), nullptr);
  auto& e0 = mgr.get_or_create(0);
  auto& e1 = mgr.get_or_create(1);
  EXPECT_EQ(mgr.get(0), &e0);
  // 单 Store：任意 Region 同一 RocksDB
  EXPECT_EQ(&e0, &e1);
  e0.put("x", "y", 1);
  EXPECT_TRUE(e0.get("x"));
  EXPECT_TRUE(e1.get("x"));
  EXPECT_TRUE(mgr.shares_engine(0, 1));
  EXPECT_TRUE(mgr.share_with(2, 0));
}

TEST_F(KvEngineTest, CheckpointRoundTrip) {
  const auto ckpt = dir_ / "ckpt";
  {
    KvEngine eng(dir_.string(), 0);
    ASSERT_TRUE(eng.put("a", "1", 1));
    ASSERT_TRUE(eng.put(std::string("u") + '\0' + 'L', "lk", 2));
    ASSERT_TRUE(eng.create_checkpoint(ckpt.string()));
  }
  ASSERT_TRUE(fs::exists(ckpt));
  KvEngine eng2((dir_ / "restore").string(), 1);
  ASSERT_TRUE(eng2.put("junk", "x", 9));
  ASSERT_TRUE(eng2.restore_from_checkpoint(ckpt.string()));
  ASSERT_TRUE(eng2.get("a").has_value());
  EXPECT_EQ(eng2.get("a")->value, "1");
  EXPECT_TRUE(eng2.get(std::string("u") + '\0' + 'L').has_value());
  EXPECT_FALSE(eng2.get("junk").has_value());
}

TEST_F(KvEngineTest, ColumnFamilyRouting) {
  EXPECT_STREQ(KvEngine::cf_name_for_key("\xff\xffskv/epoch"), "default");
  EXPECT_STREQ(KvEngine::cf_name_for_key(std::string("pk") + '\0' + 'L'), "lock");
  EXPECT_STREQ(KvEngine::cf_name_for_key(std::string("pk") + '\0' + 'D' + '\0'), "data");
  EXPECT_STREQ(KvEngine::cf_name_for_key(std::string("pk") + '\0' + 'W' + '\0'), "write");

  const std::string lock_k = std::string("user") + '\0' + 'L';
  const std::string data_k = std::string("user") + '\0' + 'D' + std::string(8, '\0');
  {
    KvEngine eng(dir_.string(), 0);
    ASSERT_TRUE(eng.put(lock_k, "lk", 1));
    ASSERT_TRUE(eng.put(data_k, "dv", 2));
    ASSERT_TRUE(eng.put("plain", "p", 3));
    EXPECT_EQ(eng.get(lock_k)->value, "lk");
    EXPECT_EQ(eng.get(data_k)->value, "dv");
    EXPECT_EQ(eng.get("plain")->value, "p");
  }
  // 关闭后重开：CF 与数据仍在
  KvEngine eng2(dir_.string(), 0);
  ASSERT_TRUE(eng2.get(lock_k).has_value());
  ASSERT_TRUE(eng2.get(data_k).has_value());
  EXPECT_EQ(eng2.get("plain")->value, "p");
}

TEST_F(KvEngineTest, SyncModeAligned) {
  EXPECT_EQ(opskv::raft::parse_sync_mode("none"), opskv::raft::SyncMode::kNone);
  EXPECT_EQ(opskv::raft::parse_sync_mode("all"), opskv::raft::SyncMode::kAll);
  EXPECT_EQ(opskv::raft::parse_sync_mode("bogus"), opskv::raft::SyncMode::kData);

  KvEngine eng(dir_.string(), 0, opskv::raft::SyncMode::kNone);
  EXPECT_EQ(eng.sync_mode(), opskv::raft::SyncMode::kNone);
  ASSERT_TRUE(eng.put("k", "v", 1));
  eng.set_sync_mode(opskv::raft::SyncMode::kAll);
  EXPECT_EQ(eng.sync_mode(), opskv::raft::SyncMode::kAll);
  ASSERT_TRUE(eng.put("k2", "v2", 2));
  EXPECT_TRUE(eng.get("k2").has_value());

  ShardEngineManager mgr(dir_.string(), opskv::raft::SyncMode::kData);
  EXPECT_EQ(mgr.sync_mode(), opskv::raft::SyncMode::kData);
  EXPECT_EQ(mgr.get_or_create(7).sync_mode(), opskv::raft::SyncMode::kData);
}
