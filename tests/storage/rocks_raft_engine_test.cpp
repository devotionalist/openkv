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

// @file rocks_raft_engine_test.cpp
// RocksRaftEngine：WriteBatch 原子写 state+log。

#include "opskv/storage/rocks_raft_engine.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::raft::LogEntry;
using opskv::raft::RaftLocalState;
using opskv::raft::SyncMode;
using opskv::storage::RocksRaftEngine;

TEST(RocksRaftEngineTest, ConsumeAndFetch) {
  auto dir = fs::temp_directory_path() / ("skv_rocks_raft_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  {
    RocksRaftEngine eng(dir.string(), SyncMode::kNone);
    auto batch = eng.make_log_batch();
    batch->append(3, {{2, 10, "hello"}, {2, 11, "world"}});
    RaftLocalState st;
    st.term = 2;
    st.commit_index = 11;
    st.first_index = 10;
    st.last_index = 11;
    batch->put_raft_state(3, st);
    ASSERT_GT(eng.consume(*batch, true), 0u);

    auto got = eng.get_raft_state(3);
    ASSERT_TRUE(got);
    EXPECT_EQ(got->term, 2u);
    auto e = eng.get_entry(3, 10);
    ASSERT_TRUE(e);
    EXPECT_EQ(e->data, "hello");

    std::vector<LogEntry> ents;
    EXPECT_EQ(eng.fetch_entries_to(3, 10, 12, SIZE_MAX, &ents), 2u);
  }
  {
    RocksRaftEngine eng(dir.string(), SyncMode::kNone);
    auto got = eng.get_raft_state(3);
    ASSERT_TRUE(got);
    EXPECT_EQ(got->last_index, 11u);
  }
  fs::remove_all(dir);
}
