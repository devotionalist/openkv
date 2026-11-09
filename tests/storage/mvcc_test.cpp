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

#include "openkv/storage/kv_engine.hpp"
#include "openkv/storage/mvcc.hpp"
#include "openkv/storage/store_service.hpp"

#include <gtest/gtest.h>

#include <filesystem>

using openkv::storage::KvEngine;
using openkv::storage::LockInfo;
using openkv::storage::MvccOp;
using openkv::storage::MvccStore;
using openkv::storage::WriteInfo;
using openkv::storage::decode_prewrite_value;
using openkv::storage::encode_prewrite_value;

TEST(MvccTest, LockRoundTrip) {
  LockInfo in;
  in.start_ts = 42;
  in.op = MvccOp::kPut;
  in.primary = "pk";
  in.value = "v1";
  auto bytes = openkv::storage::encode_lock(in);
  LockInfo out;
  ASSERT_TRUE(openkv::storage::decode_lock(bytes, &out));
  EXPECT_EQ(out.start_ts, 42u);
  EXPECT_EQ(out.op, MvccOp::kPut);
  EXPECT_EQ(out.primary, "pk");
  EXPECT_EQ(out.value, "v1");
}

TEST(MvccTest, PrewriteValueCodec) {
  auto v = encode_prewrite_value(7, 0, "p", "data");
  uint64_t ts = 0;
  uint8_t op = 0;
  std::string primary, value;
  ASSERT_TRUE(decode_prewrite_value(v, &ts, &op, &primary, &value));
  EXPECT_EQ(ts, 7u);
  EXPECT_EQ(op, 0);
  EXPECT_EQ(primary, "p");
  EXPECT_EQ(value, "data");
}

TEST(MvccTest, TxnGetVisibility) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_test";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 99);
  MvccStore mvcc(eng);

  LockInfo li;
  li.start_ts = 10;
  li.op = MvccOp::kPut;
  li.primary = "a";
  li.value = "hello";
  mvcc.put_lock("a", li, 1);
  mvcc.put_data("a", 10, "hello", 1);
  WriteInfo wi{10, MvccOp::kPut};
  mvcc.put_write("a", 20, wi, 2);
  mvcc.clear_lock("a", 2);

  bool locked = false;
  auto v1 = mvcc.txn_get("a", 15, &locked);
  EXPECT_FALSE(locked);
  EXPECT_FALSE(v1.has_value());  // commit_ts=20 > start_ts=15

  auto v2 = mvcc.txn_get("a", 25, &locked);
  EXPECT_FALSE(locked);
  ASSERT_TRUE(v2.has_value());
  EXPECT_EQ(*v2, "hello");
}

TEST(MvccTest, WriteConflict) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_wc";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 98);
  MvccStore mvcc(eng);
  WriteInfo wi{5, MvccOp::kPut};
  mvcc.put_write("k", 100, wi, 1);
  EXPECT_TRUE(mvcc.has_write_conflict("k", 50));
  EXPECT_FALSE(mvcc.has_write_conflict("k", 100));
  EXPECT_FALSE(mvcc.has_write_conflict("k", 200));
}

TEST(MvccTest, CheckTxnStatusAndPessimistic) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_cts";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 97);
  MvccStore mvcc(eng);

  LockInfo li;
  li.start_ts = 10;
  li.op = MvccOp::kPessimistic;
  li.primary = "pk";
  li.ttl_ms = 1;
  li.lock_wall_ms = openkv::storage::wall_ms_now() - 100;
  mvcc.put_lock("pk", li, 1);

  auto st = mvcc.check_txn_status("pk", 10);
  EXPECT_EQ(st.status, openkv::storage::TxnStatus::kLocked);
  EXPECT_TRUE(st.lock_expired);

  mvcc.clear_lock("pk", 2);
  WriteInfo wi{10, MvccOp::kPut};
  mvcc.put_data("pk", 10, "v", 2);
  mvcc.put_write("pk", 20, wi, 3);
  st = mvcc.check_txn_status("pk", 10);
  EXPECT_EQ(st.status, openkv::storage::TxnStatus::kCommitted);
  EXPECT_EQ(st.commit_ts, 20u);

  st = mvcc.check_txn_status("missing", 99);
  EXPECT_EQ(st.status, openkv::storage::TxnStatus::kRolledBack);
}

TEST(MvccTest, GcKeepsLatest) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_gc";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 96);
  MvccStore mvcc(eng);
  mvcc.put_data("g", 1, "old", 1);
  mvcc.put_write("g", 10, WriteInfo{1, MvccOp::kPut}, 1);
  mvcc.put_data("g", 2, "new", 2);
  mvcc.put_write("g", 20, WriteInfo{2, MvccOp::kPut}, 2);
  EXPECT_EQ(mvcc.gc_below(UINT64_MAX), 1u);
  bool locked = false;
  auto v = mvcc.txn_get("g", 100, &locked);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "new");
}

TEST(MvccTest, GcKeepsFloorBelowSafe) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_gc_floor";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 96);
  MvccStore mvcc(eng);
  // commit 10 / 20 / 30；safe=25 → 删 10，留 20(floor) 与 30
  mvcc.put_data("g", 1, "v10", 1);
  mvcc.put_write("g", 10, WriteInfo{1, MvccOp::kPut}, 1);
  mvcc.put_data("g", 2, "v20", 2);
  mvcc.put_write("g", 20, WriteInfo{2, MvccOp::kPut}, 2);
  mvcc.put_data("g", 3, "v30", 3);
  mvcc.put_write("g", 30, WriteInfo{3, MvccOp::kPut}, 3);
  EXPECT_EQ(mvcc.gc_below(25), 1u);
  bool locked = false;
  auto at22 = mvcc.txn_get("g", 22, &locked);
  ASSERT_TRUE(at22.has_value());
  EXPECT_EQ(*at22, "v20");
  auto at40 = mvcc.txn_get("g", 40, &locked);
  ASSERT_TRUE(at40.has_value());
  EXPECT_EQ(*at40, "v30");
}

TEST(MvccTest, TxnTtlRemainingPersistAndMissing) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_ttl_api";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 94);
  MvccStore mvcc(eng);
  bool locked = false;

  auto miss = mvcc.txn_ttl("k", 100, &locked);
  ASSERT_TRUE(miss.has_value());
  EXPECT_EQ(*miss, -2);
  EXPECT_FALSE(locked);

  const int64_t exp = openkv::storage::wall_ms_now() + 5000;
  mvcc.put_data("k", 1, "v", 1, exp);
  mvcc.put_write("k", 10, WriteInfo{1, MvccOp::kPut, exp}, 10);
  auto rem = mvcc.txn_ttl("k", 100, &locked);
  ASSERT_TRUE(rem.has_value());
  EXPECT_GT(*rem, 0);
  EXPECT_LE(*rem, 5000);

  mvcc.put_data("k", 2, "v2", 2, 0);
  mvcc.put_write("k", 20, WriteInfo{2, MvccOp::kPut, 0}, 20);
  auto perm = mvcc.txn_ttl("k", 100, &locked);
  ASSERT_TRUE(perm.has_value());
  EXPECT_EQ(*perm, -1);

  const int64_t past = openkv::storage::wall_ms_now() - 10;
  mvcc.put_data("k", 3, "gone", 3, past);
  mvcc.put_write("k", 30, WriteInfo{3, MvccOp::kPut, past}, 30);
  auto expired = mvcc.txn_ttl("k", 100, &locked);
  ASSERT_TRUE(expired.has_value());
  EXPECT_EQ(*expired, -2);
}

TEST(MvccTest, TxnScanPrefixVisibilityAndPage) {
  auto dir = std::filesystem::temp_directory_path() / "skv_mvcc_scan";
  std::filesystem::remove_all(dir);
  KvEngine eng(dir.string(), 95);
  MvccStore mvcc(eng);

  auto put = [&](std::string_view k, std::string_view v, uint64_t start, uint64_t commit) {
    mvcc.put_data(k, start, v, commit);
    mvcc.put_write(k, commit, WriteInfo{start, MvccOp::kPut}, commit);
  };
  put("user:a", "1", 1, 10);
  put("user:b", "2", 2, 20);
  put("user:c", "3", 3, 30);
  put("other:x", "9", 4, 40);
  // delete user:b
  mvcc.put_write("user:b", 50, WriteInfo{5, MvccOp::kDelete}, 50);

  auto all = mvcc.txn_scan("user:", "user;", "", /*start_ts=*/100, 10, false);
  ASSERT_EQ(all.pairs.size(), 2u);
  EXPECT_EQ(all.pairs[0].key, "user:a");
  EXPECT_EQ(all.pairs[0].value, "1");
  EXPECT_EQ(all.pairs[1].key, "user:c");
  EXPECT_EQ(all.pairs[1].value, "3");
  EXPECT_FALSE(all.truncated);

  auto page1 = mvcc.txn_scan("user:", "user;", "", 100, 1, false);
  ASSERT_EQ(page1.pairs.size(), 1u);
  EXPECT_EQ(page1.pairs[0].key, "user:a");
  EXPECT_TRUE(page1.truncated);
  EXPECT_EQ(page1.next_key, "user:a");

  auto page2 = mvcc.txn_scan("user:", "user;", page1.next_key, 100, 10, false);
  ASSERT_EQ(page2.pairs.size(), 1u);
  EXPECT_EQ(page2.pairs[0].key, "user:c");
  EXPECT_FALSE(page2.truncated);

  // start_ts=15 → 只见 user:a（commit 10）；user:c commit 30 不可见
  auto snap = mvcc.txn_scan("user:", "user;", "", 15, 10, true);
  ASSERT_EQ(snap.pairs.size(), 1u);
  EXPECT_EQ(snap.pairs[0].key, "user:a");
  EXPECT_TRUE(snap.pairs[0].value.empty());
}
