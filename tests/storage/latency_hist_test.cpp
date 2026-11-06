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

#include "opskv/storage/latency_hist.hpp"
#include "opskv/storage/store_service.hpp"
#include "opskv/storage/storage_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using opskv::NodeConfig;
using opskv::storage::CmdOp;
using opskv::storage::Command;
using opskv::storage::LatencyHistogram;
using opskv::storage::RegionMetrics;
using opskv::storage::StorageNode;
using opskv::storage::StoreFacade;

TEST(LatencyHistTest, QuantilesAndPrometheus) {
  LatencyHistogram h;
  for (int i = 0; i < 90; ++i) h.observe(200);   // <=250
  for (int i = 0; i < 9; ++i) h.observe(8000);    // <=10000
  h.observe(200000);                              // <=250000
  EXPECT_EQ(h.count(), 100u);
  EXPECT_EQ(h.quantile_us(0.50), 250u);
  EXPECT_GE(h.quantile_us(0.99), 10000u);
  auto prom = h.encode_prometheus("skv_t", "region=\"0\"");
  EXPECT_NE(prom.find("skv_t_bucket{region=\"0\",le=\"250\"}"), std::string::npos);
  EXPECT_NE(prom.find("skv_t_count{region=\"0\"} 100"), std::string::npos);
}

TEST(LatencyHistTest, RegionMetricsProposeGet) {
  RegionMetrics m;
  m.observe_propose(0, 1000);
  m.observe_get(0, 100);
  m.observe_propose(1, 5000);
  auto c0 = m.region_compact(0);
  EXPECT_NE(c0.find("propose_count=1"), std::string::npos);
  EXPECT_NE(c0.find("get_count=1"), std::string::npos);
  auto text = m.prometheus_text();
  EXPECT_NE(text.find("skv_propose_latency_us_count{region=\"0\"} 1"), std::string::npos);
  EXPECT_NE(text.find("skv_get_latency_us_count{region=\"0\"} 1"), std::string::npos);
  EXPECT_NE(text.find("region=\"1\""), std::string::npos);
}

TEST(LatencyHistTest, FacadeRecordsPropose) {
  auto dir =
      std::filesystem::temp_directory_path() / ("skv_lat_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
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
  Command cmd;
  cmd.op = CmdOp::kPut;
  cmd.key = "k";
  cmd.value = "v";
  ASSERT_TRUE(facade.propose(0, cmd).ok);
  auto compact = facade.metrics().region_compact(0);
  EXPECT_NE(compact.find("propose_count=1"), std::string::npos);
  node.stop();
  std::filesystem::remove_all(dir);
}
