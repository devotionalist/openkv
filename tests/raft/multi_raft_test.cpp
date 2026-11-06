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

// @file multi_raft_test.cpp
// 测试 MultiRaft 懒创建分片 group、tick 后选主与 leader_shards 列表。

#include "opskv/raft/multi_raft.hpp"

#include <gtest/gtest.h>

#include <memory>

using opskv::raft::MultiRaft;
using opskv::raft::RaftNode;

TEST(MultiRaftTest, GetOrCreateAndLeaders) {
  MultiRaft mr([](uint32_t shard) {
    RaftNode::Options opts;
    opts.shard_id = shard;
    opts.self_id = "n0";
    opts.peers = {{"n0", "127.0.0.1:1", false}};
    opts.election_timeout_ms = 100;
    opts.heartbeat_ms = 50;
    return std::make_shared<RaftNode>(
        opts, [](uint64_t, const std::string&) {},
        [](const std::string&, const std::string&, const std::string&) {});
  });

  EXPECT_EQ(mr.get(0), nullptr);
  auto& g0 = mr.get_or_create(0);
  auto& g1 = mr.get_or_create(1);
  EXPECT_EQ(mr.get(0), &g0);
  EXPECT_NE(&g0, &g1);

  for (int i = 0; i < 40; ++i) {
    mr.tick_all();
  }
  auto leaders = mr.leader_shards();
  EXPECT_EQ(leaders.size(), 2u);
  auto all = mr.all_shards();
  EXPECT_EQ(all.size(), 2u);
}
