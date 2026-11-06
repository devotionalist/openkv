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

#include "opskv/storage/kv_engine.hpp"
#include "opskv/storage/store_service.hpp"
#include "opskv/storage/storage_node.hpp"
#include "opskv/storage/watch_log.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using opskv::NodeConfig;
using opskv::storage::CmdOp;
using opskv::storage::Command;
using opskv::storage::KvEngine;
using opskv::storage::StorageNode;
using opskv::storage::StoreFacade;
using opskv::storage::WatchLog;
using opskv::storage::WatchRecord;

TEST(WatchLogTest, EncodeRoundTripAndKeyOrder) {
  auto p = WatchLog::encode_payload(false, "k", "v");
  bool del = true;
  std::string k, v;
  ASSERT_TRUE(WatchLog::decode_payload(p, &del, &k, &v));
  EXPECT_FALSE(del);
  EXPECT_EQ(k, "k");
  EXPECT_EQ(v, "v");
  EXPECT_LT(WatchLog::make_key(1), WatchLog::make_key(2));
  uint64_t rev = 0;
  ASSERT_TRUE(WatchLog::parse_key(WatchLog::make_key(42), &rev));
  EXPECT_EQ(rev, 42u);
}

TEST(WatchLogTest, PersistAcrossReopen) {
  auto dir =
      std::filesystem::temp_directory_path() / ("skv_wlog_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    KvEngine eng(dir.string(), 0);
    WatchLog::append(eng, WatchRecord{0, 5, false, "a", "1"}, 8);
    WatchLog::append(eng, WatchRecord{0, 7, true, "a", {}}, 8);
  }
  {
    KvEngine eng(dir.string(), 0);
    auto evs = WatchLog::poll(eng, 0, 4, 10);
    ASSERT_EQ(evs.size(), 2u);
    EXPECT_EQ(evs[0].revision, 5u);
    EXPECT_EQ(evs[0].key, "a");
    EXPECT_EQ(evs[0].value, "1");
    EXPECT_EQ(evs[1].revision, 7u);
    EXPECT_TRUE(evs[1].deleted);
  }
  std::filesystem::remove_all(dir);
}

TEST(WatchLogTest, TrimCapacity) {
  auto dir =
      std::filesystem::temp_directory_path() / ("skv_wtrim_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  KvEngine eng(dir.string(), 0);
  for (uint64_t i = 1; i <= 5; ++i) {
    WatchLog::append(eng, WatchRecord{0, i, false, "k", "v"}, 3);
  }
  auto evs = WatchLog::poll(eng, 0, 0, 10);
  ASSERT_EQ(evs.size(), 3u);
  EXPECT_EQ(evs[0].revision, 3u);
  EXPECT_EQ(evs[2].revision, 5u);
  std::filesystem::remove_all(dir);
}

TEST(WatchLogTest, CommitRevisionIsRaftIndex) {
  auto dir =
      std::filesystem::temp_directory_path() / ("skv_wtxn_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  NodeConfig cfg;
  cfg.node_id = "storage-test";
  cfg.listen_addr = "127.0.0.1:0";
  cfg.data_dir = dir.string();
  cfg.bootstrap_regions = {0};
  cfg.raft_election_timeout_ms = 200;
  cfg.raft_heartbeat_ms = 50;
  uint64_t put_rev = 0;
  {
    StorageNode node(cfg);
    StoreFacade facade(node);
    node.start();
    for (int i = 0; i < 80; ++i) {
      if (auto* g = node.multi_raft().get(0); g && g->is_leader()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    Command put;
    put.op = CmdOp::kPut;
    put.key = "wk";
    put.value = "wv";
    auto pr = facade.propose(0, put);
    ASSERT_TRUE(pr.ok);
    put_rev = pr.revision;
    auto evs = WatchLog::poll(node.engines().get_or_create(0), 0, put_rev - 1, 10);
    ASSERT_FALSE(evs.empty());
    EXPECT_EQ(evs.back().revision, put_rev);
    EXPECT_EQ(evs.back().key, "wk");
    node.stop();
  }
  // 进程级重启：引擎锁释放后再打开
  {
    StorageNode node2(cfg);
    node2.start();
    auto evs2 = WatchLog::poll(node2.engines().get_or_create(0), 0, 0, 10);
    bool found = false;
    for (const auto& e : evs2) {
      if (e.key == "wk" && e.revision == put_rev) found = true;
    }
    EXPECT_TRUE(found);
    node2.stop();
  }
  std::filesystem::remove_all(dir);
}
