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

// @file raft_engine_test.cpp
// FileRaftEngine：LogBatch 原子写 + 恢复。

#include "opskv/raft/file_raft_engine.hpp"
#include "opskv/raft/raft_engine.hpp"
#include "opskv/raft/raft_log.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <unistd.h>

namespace fs = std::filesystem;
using opskv::raft::FileRaftEngine;
using opskv::raft::LogEntry;
using opskv::raft::PersistedHardState;
using opskv::raft::RaftLocalState;
using opskv::raft::SyncMode;

TEST(RaftEngineTest, FileLogBatchRoundTrip) {
  auto dir = fs::temp_directory_path() / ("skv_raft_eng_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  {
    FileRaftEngine eng(dir.string(), SyncMode::kNone);
    auto batch = eng.make_log_batch();
    std::vector<LogEntry> ents = {{1, 1, "a"}, {1, 2, "b"}};
    batch->append(7, ents);
    RaftLocalState st;
    st.term = 1;
    st.voted_for = "n0";
    st.commit_index = 2;
    st.last_applied = 2;
    st.first_index = 1;
    st.last_index = 2;
    batch->put_raft_state(7, st);
    ASSERT_GT(eng.consume(*batch, false), 0u);

    auto got = eng.get_raft_state(7);
    ASSERT_TRUE(got);
    EXPECT_EQ(got->term, 1u);
    EXPECT_EQ(got->voted_for, "n0");
    EXPECT_EQ(got->last_index, 2u);

    std::vector<LogEntry> loaded;
    EXPECT_EQ(eng.fetch_entries_to(7, 1, 3, SIZE_MAX, &loaded), 2u);
    ASSERT_EQ(loaded.size(), 2u);
    EXPECT_EQ(loaded[0].data, "a");
    EXPECT_EQ(loaded[1].data, "b");
  }
  {
    FileRaftEngine eng(dir.string(), SyncMode::kNone);
    PersistedHardState hs;
    std::vector<LogEntry> ents;
    ASSERT_TRUE(load_group_from_engine(eng, 7, &hs, &ents));
    EXPECT_EQ(hs.commit_index, 2u);
    EXPECT_EQ(ents.size(), 2u);
  }
  fs::remove_all(dir);
}

TEST(RaftEngineTest, EntryCrcRoundTrip) {
  LogEntry e{9, 42, "payload-with-crc"};
  auto blob = opskv::raft::encode_log_entry(e);
  ASSERT_TRUE(blob.substr(0, 4) == "RLE2");
  LogEntry out;
  ASSERT_TRUE(opskv::raft::decode_log_entry(blob, &out));
  EXPECT_EQ(out.term, 9u);
  EXPECT_EQ(out.index, 42u);
  EXPECT_EQ(out.data, "payload-with-crc");
  // 翻坏 CRC
  blob.back() = static_cast<char>(blob.back() ^ 0x7f);
  EXPECT_FALSE(opskv::raft::decode_log_entry(blob, &out));
}

TEST(RaftEngineTest, FilePureAppend) {
  auto dir = fs::temp_directory_path() / ("skv_raft_app_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  FileRaftEngine eng(dir.string(), SyncMode::kNone);
  {
    auto batch = eng.make_log_batch();
    batch->append(1, {{1, 1, "a"}});
    RaftLocalState st;
    st.term = 1;
    st.first_index = 1;
    st.last_index = 1;
    batch->put_raft_state(1, st);
    eng.consume(*batch, false);
  }
  {
    auto batch = eng.make_log_batch();
    batch->append(1, {{1, 2, "b"}, {1, 3, "c"}});
    RaftLocalState st;
    st.term = 1;
    st.first_index = 1;
    st.last_index = 3;
    batch->put_raft_state(1, st);
    eng.consume(*batch, false);
  }
  std::vector<LogEntry> loaded;
  EXPECT_EQ(eng.fetch_entries_to(1, 1, 4, SIZE_MAX, &loaded), 3u);
  EXPECT_EQ(loaded[2].data, "c");
  fs::remove_all(dir);
}

TEST(RaftEngineTest, CutAndGc) {
  auto dir = fs::temp_directory_path() / ("skv_raft_gc_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  FileRaftEngine eng(dir.string(), SyncMode::kNone);
  {
    auto batch = eng.make_log_batch();
    batch->append(1, {{1, 1, "x"}, {1, 2, "y"}, {1, 3, "z"}});
    RaftLocalState st;
    st.first_index = 1;
    st.last_index = 3;
    st.term = 1;
    batch->put_raft_state(1, st);
    eng.consume(*batch, false);
  }
  eng.gc(1, 1, 3);  // drop 1,2 keep 3
  std::vector<LogEntry> left;
  eng.fetch_entries_to(1, 1, 10, SIZE_MAX, &left);
  ASSERT_EQ(left.size(), 1u);
  EXPECT_EQ(left[0].index, 3u);
  eng.clean(1);
  EXPECT_FALSE(eng.get_raft_state(1).has_value());
  fs::remove_all(dir);
}
