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

#include "opskv/storage/store_service.hpp"
#include "opskv/storage/storage_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::NodeConfig;
using opskv::storage::CmdOp;
using opskv::storage::Command;
using opskv::storage::StorageNode;
using opskv::storage::StoreFacade;
using opskv::storage::decode_command;
using opskv::storage::encode_command;
using opskv::storage::region_epoch_storage_key;

TEST(StoreAdminRaftTest, AdminCommandRoundTrip) {
  Command c;
  c.op = CmdOp::kAdminSplit;
  c.key = "m";
  uint32_t right = 7;
  c.value.assign(reinterpret_cast<const char*>(&right), sizeof(right));
  auto bin = encode_command(c);
  Command out;
  ASSERT_TRUE(decode_command(bin, &out));
  EXPECT_EQ(out.op, CmdOp::kAdminSplit);
  EXPECT_EQ(out.key, "m");
  uint32_t r2 = 0;
  std::memcpy(&r2, out.value.data(), sizeof(r2));
  EXPECT_EQ(r2, 7u);
}

TEST(StoreAdminRaftTest, EpochPersistsAcrossRestart) {
  auto dir = fs::temp_directory_path() / ("skv_epoch_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  {
    NodeConfig cfg;
    cfg.node_id = "storage-test";
    cfg.listen_addr = "127.0.0.1:0";
    cfg.data_dir = dir.string();
    cfg.bootstrap_regions = {0};
    cfg.raft_election_timeout_ms = 200;
    cfg.raft_heartbeat_ms = 50;
    StorageNode node(cfg);
    node.start();
    for (int i = 0; i < 80; ++i) {
      if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(node.multi_raft().get(0)->is_leader());
    auto resp = node.admin_set_epoch(0, 42);
    ASSERT_EQ(resp.rfind("OK", 0), 0u) << resp;
    EXPECT_EQ(node.region_epoch(0), 42u);
    auto* eng = node.engines().get(0);
    ASSERT_NE(eng, nullptr);
    auto rec = eng->get(region_epoch_storage_key(0));
    ASSERT_TRUE(rec.has_value());
    node.stop();
  }
  {
    NodeConfig cfg;
    cfg.node_id = "storage-test";
    cfg.listen_addr = "127.0.0.1:0";
    cfg.data_dir = dir.string();
    cfg.bootstrap_regions = {0};
    cfg.raft_election_timeout_ms = 200;
    cfg.raft_heartbeat_ms = 50;
    StorageNode node(cfg);
    node.start();
    for (int i = 0; i < 80; ++i) {
      if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(node.region_epoch(0), 42u);
    auto* eng = node.engines().get(0);
    ASSERT_NE(eng, nullptr);
    auto rec = eng->get(region_epoch_storage_key(0));
    ASSERT_TRUE(rec.has_value());
    node.stop();
  }
  fs::remove_all(dir);
}

TEST(StoreAdminRaftTest, TxnGetLinearizableSmoke) {
  auto dir = fs::temp_directory_path() / ("skv_txnget_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  NodeConfig cfg;
  cfg.node_id = "storage-test";
  cfg.listen_addr = "127.0.0.1:0";
  cfg.data_dir = dir.string();
  cfg.bootstrap_regions = {0};
  cfg.raft_election_timeout_ms = 200;
  cfg.raft_heartbeat_ms = 50;
  StorageNode node(cfg);
  StoreFacade facade(node);
  node.start();
  for (int i = 0; i < 80; ++i) {
    if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  auto r = facade.txn_get(0, "k", 100, /*linearizable=*/true);
  EXPECT_TRUE(r.is_leader);
  EXPECT_FALSE(r.locked);
  node.stop();
  fs::remove_all(dir);
}

TEST(StoreAdminRaftTest, ApplyConflictVisibleToClient) {
  auto dir = fs::temp_directory_path() / ("skv_applyerr_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  NodeConfig cfg;
  cfg.node_id = "storage-test";
  cfg.listen_addr = "127.0.0.1:0";
  cfg.data_dir = dir.string();
  cfg.bootstrap_regions = {0};
  cfg.raft_election_timeout_ms = 200;
  cfg.raft_heartbeat_ms = 50;
  StorageNode node(cfg);
  StoreFacade facade(node);
  node.start();
  for (int i = 0; i < 80; ++i) {
    if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(node.multi_raft().get(0)->is_leader());

  auto pw = facade.prewrite(0, "k", 10, "k", /*mvcc_op=*/0, "v1");
  ASSERT_TRUE(pw.ok) << pw.error;

  // 绕过门面预检：直接 propose 冲突 prewrite，apply 应回传 locked
  Command cmd;
  cmd.op = CmdOp::kPrewrite;
  cmd.key = "k";
  cmd.value = opskv::storage::encode_prewrite_value(20, 0, "k", "v2");
  auto r = facade.propose(0, cmd);
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error, "locked");

  // Commit 无锁 → lock_not_found
  Command c2;
  c2.op = CmdOp::kCommit;
  c2.key = "missing";
  c2.value = opskv::storage::encode_commit_value(1, 2);
  auto r2 = facade.propose(0, c2);
  EXPECT_FALSE(r2.ok);
  EXPECT_EQ(r2.error, "lock_not_found");

  node.stop();
  fs::remove_all(dir);
}

TEST(StoreAdminRaftTest, SplitSeedsRightWithLeftPeers) {
  auto dir = fs::temp_directory_path() / ("skv_splitseed_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  NodeConfig cfg;
  cfg.node_id = "storage-test";
  cfg.listen_addr = "127.0.0.1:0";
  cfg.data_dir = dir.string();
  cfg.bootstrap_regions = {0};
  cfg.raft_election_timeout_ms = 200;
  cfg.raft_heartbeat_ms = 50;
  StorageNode node(cfg);
  StoreFacade facade(node);
  node.start();
  for (int i = 0; i < 80; ++i) {
    if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(node.multi_raft().get(0)->is_leader());

  Command put;
  put.op = CmdOp::kPut;
  put.key = "a";
  put.value = "1";
  ASSERT_TRUE(facade.propose(0, put).ok);
  put.key = "m";
  put.value = "2";
  ASSERT_TRUE(facade.propose(0, put).ok);
  put.key = "z";
  put.value = "3";
  ASSERT_TRUE(facade.propose(0, put).ok);

  auto left_peers = node.resolve_region_peers(0);
  ASSERT_FALSE(left_peers.empty());

  auto resp = node.admin_split(0, 1, "m");
  ASSERT_EQ(resp.rfind("OK", 0), 0u) << resp;

  auto* right = node.multi_raft().get(1);
  ASSERT_NE(right, nullptr);
  EXPECT_EQ(right->peers().size(), left_peers.size());
  // 左右 epoch 对齐抬高；同机共享引擎，KV 不搬迁
  EXPECT_EQ(node.region_epoch(1), node.region_epoch(0));
  EXPECT_GT(node.region_epoch(0), 1u);
  EXPECT_TRUE(node.engines().shares_engine(0, 1));
  EXPECT_EQ(node.region_range(0).end_key, "m");
  EXPECT_EQ(node.region_range(1).start_key, "m");

  auto* leng = node.engines().get(0);
  auto* reng = node.engines().get(1);
  ASSERT_NE(leng, nullptr);
  ASSERT_NE(reng, nullptr);
  EXPECT_EQ(leng, reng);
  EXPECT_TRUE(leng->get("a").has_value());
  EXPECT_TRUE(leng->get("m").has_value());  // 仍在共享 DB
  EXPECT_TRUE(leng->get("z").has_value());
  EXPECT_TRUE(node.key_in_region(0, "a"));
  EXPECT_FALSE(node.key_in_region(0, "m"));
  EXPECT_TRUE(node.key_in_region(1, "m"));
  EXPECT_FALSE(node.key_in_region(1, "a"));

  node.stop();
  fs::remove_all(dir);
}

TEST(StoreAdminRaftTest, ProposeIdempotentByRequestId) {
  auto dir = fs::temp_directory_path() / ("skv_reqid_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  NodeConfig cfg;
  cfg.node_id = "storage-test";
  cfg.listen_addr = "127.0.0.1:0";
  cfg.data_dir = dir.string();
  cfg.bootstrap_regions = {0};
  cfg.raft_election_timeout_ms = 200;
  cfg.raft_heartbeat_ms = 50;
  StorageNode node(cfg);
  StoreFacade facade(node);
  node.start();
  for (int i = 0; i < 80; ++i) {
    if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(node.multi_raft().get(0)->is_leader());

  Command cmd;
  cmd.op = CmdOp::kPut;
  cmd.key = "k";
  cmd.value = "v1";
  cmd.request_id = 1001;
  auto r1 = facade.propose(0, cmd);
  ASSERT_TRUE(r1.ok) << r1.error;
  const uint64_t rev = r1.revision;

  cmd.value = "v2";  // 重试应忽略新 value，返回首次 revision
  auto r2 = facade.propose(0, cmd);
  ASSERT_TRUE(r2.ok) << r2.error;
  EXPECT_EQ(r2.revision, rev);

  auto g = facade.get(0, "k", true);
  ASSERT_TRUE(g.found);
  EXPECT_EQ(g.value, "v1");

  node.stop();
  fs::remove_all(dir);
}

TEST(StoreAdminRaftTest, MigrateGeAtomicBatches) {
  auto dir = fs::temp_directory_path() / ("skv_mig_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  opskv::storage::ShardEngineManager mgr(dir.string());
  auto& left = mgr.get_or_create(0);
  auto& right = mgr.get_or_create(1);
  left.put("a", "1", 1);
  left.put("m", "2", 2);
  left.put("z", "3", 3);
  const auto meta = region_epoch_storage_key(0);
  left.put(meta, std::string(8, '\0'), 0);
  size_t n = left.migrate_ge(right, "m");
  EXPECT_EQ(n, 2u);
  // 单 Store：left/right 同引擎，migrate_ge 只统计不删；KeyRange 负责隔离。
  if (&left == &right || mgr.shares_engine(0, 1)) {
    EXPECT_TRUE(left.get("m").has_value());
    EXPECT_TRUE(left.get("z").has_value());
    EXPECT_TRUE(left.get("a").has_value());
    EXPECT_EQ(right.get("m")->value, "2");
  } else {
    EXPECT_FALSE(left.get("m").has_value());
    EXPECT_FALSE(left.get("z").has_value());
    EXPECT_TRUE(left.get("a").has_value());
    EXPECT_TRUE(left.get(meta).has_value());
    EXPECT_TRUE(right.get("m").has_value());
    EXPECT_TRUE(right.get("z").has_value());
    EXPECT_EQ(right.get("m")->value, "2");
  }
  fs::remove_all(dir);
}
