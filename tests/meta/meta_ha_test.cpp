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

// @file meta_ha_test.cpp
// Meta 单节点 Raft（rf=1）与命令编解码；多进程 HA 由故障演练覆盖。

#include "openkv/meta/meta_fsm.hpp"
#include "openkv/meta/meta_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using openkv::ClusterBootstrap;
using openkv::meta::MetaCommand;
using openkv::meta::MetaOp;
using openkv::meta::MetaNode;
using openkv::meta::NodeInfo;
using openkv::meta::NodeRole;
using openkv::meta::apply_meta_command;
using openkv::meta::decode_meta_command;
using openkv::meta::encode_meta_command;

TEST(MetaFsmTest, RoundTripRegister) {
  MetaCommand c;
  c.op = MetaOp::kRegister;
  c.node.node_id = "s9";
  c.node.address = "127.0.0.1:9199";
  c.node.role = NodeRole::kStorage;
  auto bytes = encode_meta_command(c);
  MetaCommand out;
  ASSERT_TRUE(decode_meta_command(bytes, &out));
  EXPECT_EQ(out.op, MetaOp::kRegister);
  EXPECT_EQ(out.node.node_id, "s9");
  EXPECT_EQ(out.node.address, "127.0.0.1:9199");
}

TEST(MetaHaTest, SingleReplicaLeaderRegister) {
  ClusterBootstrap boot;
  boot.node_id = "meta-test";
  boot.listen_addr = "127.0.0.1:18550";
  boot.shard_count = 4;
  boot.replica_factor = 3;
  boot.storage_peers = {{"s0", "127.0.0.1:9100"}};
  // 空 meta_peers → 单节点 Raft
  MetaNode srv(boot);
  ASSERT_NO_THROW(srv.start());
  for (int i = 0; i < 40; ++i) {
    if (srv.is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(srv.is_leader());

  MetaCommand reg;
  reg.op = MetaOp::kRegister;
  reg.node.node_id = "s-new";
  reg.node.address = "127.0.0.1:9111";
  reg.node.role = NodeRole::kStorage;
  std::string err, redir;
  ASSERT_TRUE(srv.replicate(reg, &err, &redir)) << err;
  auto n = srv.state().get_node("s-new");
  ASSERT_TRUE(n.has_value());
  EXPECT_EQ(n->address, "127.0.0.1:9111");

  auto rt = srv.routing_table(&err, &redir);
  EXPECT_EQ(rt.shard_count, 1u);  // Range：初始 1 个 Region
  EXPECT_EQ(rt.regions.size(), 1u);

  // A3：GET_ROUTE 不要求 Leader 标记（routing_table 已放宽）
  auto line = srv.handle_line("GET_ROUTE");
  EXPECT_EQ(line.rfind("OK ROUTE", 0), 0u);

  // A2：WATCH_ROUTE 立即返回（since < ver）
  auto watch = srv.handle_line("WATCH_ROUTE 0 200");
  EXPECT_EQ(watch.rfind("OK ROUTE", 0), 0u);

  // O2：METRICS
  auto metrics = srv.handle_line("METRICS");
  EXPECT_NE(metrics.find("skv_meta_route_version"), std::string::npos);
  EXPECT_NE(metrics.find("END"), std::string::npos);

  srv.stop();
}

TEST(MetaHaTest, FsmSnapSurvivesRestart) {
  namespace fs = std::filesystem;
  const auto dir = fs::temp_directory_path() /
                   ("skv_meta_fsm_" + std::to_string(::getpid()) + "_" +
                    std::to_string(
                        std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::remove_all(dir);
  fs::create_directories(dir);

  ClusterBootstrap boot;
  boot.node_id = "meta-fsm";
  boot.listen_addr = "127.0.0.1:18551";
  boot.data_dir = dir.string();
  boot.raft_compact_threshold = 64;
  boot.replica_factor = 1;
  boot.storage_peers = {{"s0", "127.0.0.1:9100"}};

  uint64_t route_ver = 0;
  size_t n_regions = 0;
  {
    MetaNode srv(boot);
    ASSERT_NO_THROW(srv.start());
    for (int i = 0; i < 40; ++i) {
      if (srv.is_leader()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(srv.is_leader());

    auto split = srv.handle_line("SPLIT_REGION 0 6d");  // hex 'm'
    ASSERT_EQ(split.rfind("OK SPLIT", 0), 0u) << split;

    // 推进 apply 并触发定期 FSM snap（每 32 条）
    for (int i = 0; i < 40; ++i) {
      auto ts = srv.handle_line("GET_TS 8");
      ASSERT_EQ(ts.rfind("OK TS", 0), 0u) << ts;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::string err, redir;
    auto rt = srv.routing_table(&err, &redir);
    n_regions = rt.regions.size();
    route_ver = rt.version;
    EXPECT_GE(n_regions, 2u);
    EXPECT_GT(route_ver, 1u);
    srv.stop();
  }

  // 同 data_dir 再起：应恢复 split 后的路由，而非仅 bootstrap region-0
  {
    MetaNode srv2(boot);
    ASSERT_NO_THROW(srv2.start());
    for (int i = 0; i < 40; ++i) {
      if (srv2.is_leader()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(srv2.is_leader());
    std::string err, redir;
    auto rt = srv2.routing_table(&err, &redir);
    EXPECT_EQ(rt.regions.size(), n_regions) << "FSM snap/replay must keep split regions";
    EXPECT_GE(rt.version, route_ver);
    auto line = srv2.handle_line("GET_ROUTE");
    EXPECT_EQ(line.rfind("OK ROUTE", 0), 0u);
    srv2.stop();
  }
  fs::remove_all(dir);
}
