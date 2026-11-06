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

// @file raft_persist_test.cpp
// Raft 持久化装载与宕机恢复（单节点）。

#include "opskv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::raft::RaftNode;
using opskv::raft::TransportSend;

TEST(RaftPersistTest, SaveAndRecoverApplied) {
  auto dir = fs::temp_directory_path() / ("skv_raft_persist_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);

  std::vector<std::pair<uint64_t, std::string>> applied;
  auto on_apply = [&](uint64_t idx, const std::string& data) {
    applied.emplace_back(idx, data);
  };
  TransportSend noop = [](const std::string&, const std::string&, const std::string&) {};

  {
    RaftNode::Options opts;
    opts.shard_id = 0;
    opts.self_id = "n0";
    opts.peers = {{"n0", "127.0.0.1:1", false}};
    opts.election_timeout_ms = 200;
    opts.persist_dir = dir.string();
    opts.compact_threshold = 0;
    RaftNode node(opts, on_apply, noop);
    for (int i = 0; i < 30 && !node.is_leader(); ++i) {
      node.tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(node.is_leader());
    uint64_t idx = 0;
    ASSERT_TRUE(node.propose("hello", &idx));
    for (int i = 0; i < 20; ++i) {
      if (node.soft_state().last_applied >= idx) break;
      node.tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GE(node.soft_state().last_applied, idx);
    EXPECT_FALSE(applied.empty());
  }

  applied.clear();
  {
    RaftNode::Options opts;
    opts.shard_id = 0;
    opts.self_id = "n0";
    opts.peers = {{"n0", "127.0.0.1:1", false}};
    opts.election_timeout_ms = 200;
    opts.persist_dir = dir.string();
    RaftNode node(opts, on_apply, noop);
    auto st = node.soft_state();
    EXPECT_GE(st.commit_index, 1u);
    EXPECT_GE(st.last_applied, 1u);
    // 已 apply 的不会再次回调；recover_apply 仅补缺口
    node.recover_apply();
  }

  fs::remove_all(dir);
}

TEST(RaftPersistTest, InstallSnapshotCompactsAndApplies) {
  std::string applied_snap;
  uint64_t snap_idx = 0;
  auto applier = [&](uint64_t last_idx, uint64_t /*term*/, std::string_view data) {
    snap_idx = last_idx;
    applied_snap.assign(data);
    return true;
  };
  TransportSend noop = [](const std::string&, const std::string&, const std::string&) {};

  RaftNode::Options opts;
  opts.shard_id = 0;
  opts.self_id = "n1";
  opts.peers = {{"n0", "127.0.0.1:1", false}, {"n1", "127.0.0.1:2", false}};
  opts.election_timeout_ms = 500;
  opts.compact_threshold = 0;

  RaftNode node(opts, [](uint64_t, const std::string&) {}, noop, nullptr, applier);

  bool ok = false;
  node.on_install_snapshot("n0", /*term=*/1, /*last_idx=*/50, /*last_term=*/1, "kv-blob", &ok);
  EXPECT_TRUE(ok);
  EXPECT_EQ(snap_idx, 50u);
  EXPECT_EQ(applied_snap, "kv-blob");
  auto st = node.soft_state();
  EXPECT_EQ(st.commit_index, 50u);
  EXPECT_EQ(st.last_applied, 50u);
  EXPECT_EQ(st.term, 1u);
}
