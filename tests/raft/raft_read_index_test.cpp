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

#include "opskv/raft/raft_frame.hpp"
#include "opskv/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using opskv::raft::RaftNode;
using opskv::raft::decode_frame_header;
using opskv::raft::encode_frame;
using opskv::raft::FrameType;
using opskv::raft::is_binary_frame;

TEST(RaftFrameTest, EncodeDecodeRoundTrip) {
  auto wire = encode_frame(FrameType::kAppendEntries, 7, "RAFT_AE 0 n0 1");
  ASSERT_TRUE(is_binary_frame(wire));
  opskv::raft::DecodedFrame fr;
  size_t total = 0;
  ASSERT_TRUE(decode_frame_header(wire, &fr, &total));
  EXPECT_EQ(total, wire.size());
  EXPECT_EQ(fr.shard, 7u);
  EXPECT_EQ(fr.payload, "RAFT_AE 0 n0 1");
}

TEST(RaftReadIndexTest, LeaseReadOnSingleNode) {
  RaftNode::Options opts;
  opts.self_id = "n0";
  opts.peers = {{"n0", "1", false}};
  opts.enable_lease_read = true;
  opts.leader_lease_ms = 1000;
  opts.enable_async_apply = false;
  opts.coalesce_replication = false;
  auto node = std::make_unique<RaftNode>(std::move(opts), [](auto&&...) {}, [](auto&&...) {});
  for (int i = 0; i < 40; ++i) {
    node->tick();
    if (node->is_leader()) break;
  }
  ASSERT_TRUE(node->is_leader());
  uint64_t ci = 0;
  ASSERT_TRUE(node->read_index(&ci));
  EXPECT_GE(ci, 0u);
  auto st = node->soft_state();
  EXPECT_GE(st.election_count, 1u);
}

TEST(RaftReadIndexTest, SoftStateMetrics) {
  RaftNode::Options opts;
  opts.self_id = "n0";
  opts.peers = {{"n0", "1", false}};
  opts.enable_async_apply = false;
  auto node = std::make_unique<RaftNode>(std::move(opts), [](auto&&...) {}, [](auto&&...) {});
  for (int i = 0; i < 40; ++i) node->tick();
  auto st = node->soft_state();
  EXPECT_TRUE(node->is_leader());
  EXPECT_GE(st.election_count, 1u);
  EXPECT_EQ(st.replication_lag, 0u);
}
