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

// @file config_test.cpp
// 测试 load_node_config / load_bootstrap：key=value 解析、临时文件与默认值。

#include "opskv/config.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <filesystem>
#include <unistd.h>

namespace fs = std::filesystem;

class ConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("skv_cfg_" + std::to_string(::getpid()));
    fs::create_directories(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  fs::path write_file(const std::string& name, const std::string& body) {
    auto p = dir_ / name;
    std::ofstream out(p);
    out << body;
    return p;
  }

  fs::path dir_;
};

TEST_F(ConfigTest, LoadNodeConfig) {
  auto p = write_file("node.conf", R"(
# comment
node_id=storage-9
listen_addr=0.0.0.0:9199
advertise_addr=storage-9.storage-headless.opskv.svc.cluster.local:9199
meta_addr=127.0.0.1:8500
data_dir=./data/x
shard_count=32
raft_election_timeout_ms=1500
raft_heartbeat_ms=120
persist_sync=all
)");
  auto c = opskv::load_node_config(p.string());
  EXPECT_EQ(c.node_id, "storage-9");
  EXPECT_EQ(c.listen_addr, "0.0.0.0:9199");
  EXPECT_EQ(c.advertise_addr, "storage-9.storage-headless.opskv.svc.cluster.local:9199");
  EXPECT_EQ(c.shard_count, 32u);
  EXPECT_EQ(c.raft_election_timeout_ms, 1500u);
  EXPECT_EQ(c.raft_heartbeat_ms, 120u);
  EXPECT_EQ(c.persist_sync, "all");
}

TEST_F(ConfigTest, LoadBootstrapPeers) {
  auto p = write_file("boot.conf",
                      "meta_addr=0.0.0.0:8500\n"
                      "shard_count=8\n"
                      "replica_factor=3\n"
                      "storage_peers=s0@127.0.0.1:9100,s1@127.0.0.1:9101\n"
                      "compute_peers=c0@127.0.0.1:19000\n");
  auto b = opskv::load_bootstrap(p.string());
  EXPECT_EQ(b.shard_count, 8u);
  ASSERT_EQ(b.storage_peers.size(), 2u);
  EXPECT_EQ(b.storage_peers[0].node_id, "s0");
  EXPECT_EQ(b.storage_peers[0].address, "127.0.0.1:9100");
  ASSERT_EQ(b.compute_peers.size(), 1u);
  EXPECT_EQ(b.compute_peers[0].node_id, "c0");
}

TEST_F(ConfigTest, MissingFileThrows) {
  EXPECT_THROW(opskv::load_node_config((dir_ / "nope.conf").string()), std::runtime_error);
}
