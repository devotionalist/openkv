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

// @file raft_fault_inject_test.cpp
// 出站抖动注入：丢包/延迟规则（不启真实 gRPC）。

#include "openkv/raft/util/raft_fault_inject.hpp"

#include <gtest/gtest.h>

#include <cstdlib>

using openkv::raft::RaftOutboundFault;

TEST(RaftFaultInjectTest, DefaultDisabled) {
  RaftOutboundFault f;
  EXPECT_FALSE(f.enabled());
  EXPECT_FALSE(f.should_drop("AppendEntries", 0));
  EXPECT_EQ(f.delay_ms_for("AppendEntries"), 0);
}

TEST(RaftFaultInjectTest, DropAeOnly) {
  RaftOutboundFault f;
  f.drop_ae_pct = 30;
  EXPECT_TRUE(f.enabled());
  EXPECT_TRUE(f.should_drop("AppendEntries", 0));
  EXPECT_TRUE(f.should_drop("AppendEntries", 29));
  EXPECT_FALSE(f.should_drop("AppendEntries", 30));
  EXPECT_FALSE(f.should_drop("RequestVote", 0));
}

TEST(RaftFaultInjectTest, DropRaftCoversAll) {
  RaftOutboundFault f;
  f.drop_raft_pct = 50;
  EXPECT_TRUE(f.should_drop("RequestVote", 49));
  EXPECT_FALSE(f.should_drop("RequestVote", 50));
  EXPECT_TRUE(f.should_drop("AppendEntries", 10));
  EXPECT_TRUE(f.should_drop("InstallSnapshot", 0));
}

TEST(RaftFaultInjectTest, DelayTakesMaxOfAeAndRaft) {
  RaftOutboundFault f;
  f.delay_ae_ms = 20;
  f.delay_raft_ms = 5;
  EXPECT_EQ(f.delay_ms_for("AppendEntries"), 20);
  EXPECT_EQ(f.delay_ms_for("RequestVote"), 5);
  f.delay_raft_ms = 40;
  EXPECT_EQ(f.delay_ms_for("AppendEntries"), 40);
}

TEST(RaftFaultInjectTest, FromEnvReadsAndClamps) {
  setenv("SKV_FAULT_DROP_AE_PCT", "25", 1);
  setenv("SKV_FAULT_DELAY_AE_MS", "15", 1);
  setenv("SKV_FAULT_DROP_RAFT_PCT", "0", 1);
  setenv("SKV_FAULT_DELAY_RAFT_MS", "0", 1);
  auto f = RaftOutboundFault::from_env();
  EXPECT_EQ(f.drop_ae_pct, 25);
  EXPECT_EQ(f.delay_ae_ms, 15);
  EXPECT_EQ(f.drop_raft_pct, 0);
  EXPECT_EQ(f.delay_raft_ms, 0);

  setenv("SKV_FAULT_DROP_AE_PCT", "999", 1);
  setenv("SKV_FAULT_DELAY_RAFT_MS", "-1", 1);
  f = RaftOutboundFault::from_env();
  EXPECT_EQ(f.drop_ae_pct, 100);
  EXPECT_EQ(f.delay_raft_ms, 0);

  unsetenv("SKV_FAULT_DROP_AE_PCT");
  unsetenv("SKV_FAULT_DELAY_AE_MS");
  unsetenv("SKV_FAULT_DROP_RAFT_PCT");
  unsetenv("SKV_FAULT_DELAY_RAFT_MS");
}
