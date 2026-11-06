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

// @file store_facade_test.cpp
// 集成测试 StorageNode + StoreFacade：Raft 选主后 propose/get 端到端。

#include "opskv/storage/storage_node.hpp"
#include "opskv/storage/store_service.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::NodeConfig;
using opskv::storage::CmdOp;
using opskv::storage::Command;
using opskv::storage::StorageNode;
using opskv::storage::StoreFacade;

class StoreFacadeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("skv_facade_" + std::to_string(::getpid()));
    fs::remove_all(dir_);
    NodeConfig cfg;
    cfg.node_id = "storage-test";
    cfg.listen_addr = "127.0.0.1:0";
    cfg.data_dir = dir_.string();
    cfg.shard_count = 2;
    cfg.bootstrap_regions = {0, 1};
    cfg.raft_election_timeout_ms = 200;
    cfg.raft_heartbeat_ms = 50;
    node_ = std::make_unique<StorageNode>(cfg);
    facade_ = std::make_unique<StoreFacade>(*node_);
    node_->start();
    wait_leader(0);
  }

  void TearDown() override {
    if (node_) {
      node_->stop();
    }
    fs::remove_all(dir_);
  }

  void wait_leader(uint32_t shard) {
    for (int i = 0; i < 80; ++i) {
      if (auto* g = node_->multi_raft().get(shard); g && g->is_leader()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    FAIL() << "shard " << shard << " no leader";
  }

  fs::path dir_;
  std::unique_ptr<StorageNode> node_;
  std::unique_ptr<StoreFacade> facade_;
};

TEST_F(StoreFacadeTest, PutGet) {
  Command cmd{CmdOp::kPut, "foo", "bar", 0};
  auto pr = facade_->propose(0, cmd);
  ASSERT_TRUE(pr.ok) << pr.error;
  EXPECT_GT(pr.revision, 0u);

  auto gr = facade_->get(0, "foo", true);
  EXPECT_TRUE(gr.found);
  EXPECT_EQ(gr.value, "bar");
  EXPECT_TRUE(gr.is_leader);
}

TEST_F(StoreFacadeTest, Delete) {
  facade_->propose(0, {CmdOp::kPut, "d", "1", 0});
  auto pr = facade_->propose(0, {CmdOp::kDelete, "d", "", 0});
  ASSERT_TRUE(pr.ok) << pr.error;
  auto gr = facade_->get(0, "d", true);
  EXPECT_FALSE(gr.found);
}

TEST_F(StoreFacadeTest, IndependentShards) {
  wait_leader(1);
  // 单 Store 共享引擎：同 key 跨 Region 会覆盖；用不同 key 验证两 Region 均可写读。
  ASSERT_TRUE(facade_->propose(0, {CmdOp::kPut, "k0", "s0", 0}).ok);
  ASSERT_TRUE(facade_->propose(1, {CmdOp::kPut, "k1", "s1", 0}).ok);
  EXPECT_EQ(facade_->get(0, "k0", true).value, "s0");
  EXPECT_EQ(facade_->get(1, "k1", true).value, "s1");
  EXPECT_TRUE(node_->engines().shares_engine(0, 1));
}
