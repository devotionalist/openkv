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

// @file raft_log_test.cpp
// 测试 RaftLog 追加、冲突截断、match、compact_to 等日志语义。

#include "opskv/raft/raft_log.hpp"

#include <gtest/gtest.h>

using opskv::raft::LogEntry;
using opskv::raft::RaftLog;

TEST(RaftLogTest, AppendAndQuery) {
  RaftLog log;
  EXPECT_EQ(log.last_index(), 0u);
  auto i1 = log.append(1, "a");
  auto i2 = log.append(1, "b");
  EXPECT_EQ(i1, 1u);
  EXPECT_EQ(i2, 2u);
  EXPECT_EQ(log.last_index(), 2u);
  EXPECT_EQ(log.last_term(), 1u);
  auto e = log.entry(2);
  ASSERT_TRUE(e);
  EXPECT_EQ(e->data, "b");
}

TEST(RaftLogTest, AppendEntriesConflictTruncate) {
  RaftLog log;
  log.append(1, "a");
  log.append(1, "b");
  log.append(1, "c");

  std::vector<LogEntry> newer{{2, 2, "B2"}, {2, 3, "C2"}};
  EXPECT_TRUE(log.append_entries(1, 1, newer));
  EXPECT_EQ(log.last_index(), 3u);
  EXPECT_EQ(log.entry(2)->term, 2u);
  EXPECT_EQ(log.entry(2)->data, "B2");
}

TEST(RaftLogTest, AppendEntriesRejectBadPrev) {
  RaftLog log;
  log.append(1, "a");
  EXPECT_FALSE(log.append_entries(1, 9, {}));
}

TEST(RaftLogTest, CompactTo) {
  RaftLog log;
  log.append(1, "a");
  log.append(1, "b");
  log.append(2, "c");
  log.compact_to(2, 1);
  EXPECT_EQ(log.first_index(), 3u);
  EXPECT_EQ(log.last_index(), 3u);
  EXPECT_FALSE(log.entry(1).has_value());
  EXPECT_TRUE(log.entry(3).has_value());
}

TEST(RaftLogTest, EntriesFromLimit) {
  RaftLog log;
  for (int i = 0; i < 10; ++i) {
    log.append(1, std::to_string(i));
  }
  auto ents = log.entries_from(5, 3);
  ASSERT_EQ(ents.size(), 3u);
  EXPECT_EQ(ents[0].index, 5u);
  EXPECT_EQ(ents[2].index, 7u);
}
