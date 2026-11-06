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

#include "opskv/raft/snap_stream.hpp"

#include <gtest/gtest.h>

using opskv::raft::SnapStreamBuffer;
using opskv::raft::snap_from_hex;
using opskv::raft::snap_to_hex;

TEST(SnapStreamTest, HexRoundTrip) {
  const std::string raw("hello\0world", 11);
  auto hex = snap_to_hex(raw);
  std::string back;
  ASSERT_TRUE(snap_from_hex(hex, &back));
  EXPECT_EQ(back, raw);
}

TEST(SnapStreamTest, AssembleChunks) {
  SnapStreamBuffer buf;
  SnapStreamBuffer::Key key{"n0", 1, 50};
  const std::string payload(100, 'x');
  bool complete = false;
  std::string out;
  uint64_t term = 0, last_term = 0;

  ASSERT_TRUE(buf.append(key, 3, 2, 0, payload.size(), false, payload.substr(0, 40), &complete, &out,
                         &term, &last_term));
  EXPECT_FALSE(complete);

  ASSERT_TRUE(buf.append(key, 3, 2, 40, payload.size(), false, payload.substr(40, 40), &complete,
                         &out, &term, &last_term));
  EXPECT_FALSE(complete);

  ASSERT_TRUE(buf.append(key, 3, 2, 80, payload.size(), true, payload.substr(80), &complete, &out,
                         &term, &last_term));
  EXPECT_TRUE(complete);
  EXPECT_EQ(out, payload);
  EXPECT_EQ(term, 3u);
  EXPECT_EQ(last_term, 2u);
}

TEST(SnapStreamTest, EmptySnapshot) {
  SnapStreamBuffer buf;
  SnapStreamBuffer::Key key{"n0", 0, 1};
  bool complete = false;
  std::string out;
  uint64_t term = 0, last_term = 0;
  ASSERT_TRUE(buf.append(key, 1, 1, 0, 0, true, "", &complete, &out, &term, &last_term));
  EXPECT_TRUE(complete);
  EXPECT_TRUE(out.empty());
}

TEST(SnapStreamTest, RejectOutOfOrder) {
  SnapStreamBuffer buf;
  SnapStreamBuffer::Key key{"n0", 1, 10};
  bool complete = false;
  std::string out;
  uint64_t term = 0, last_term = 0;
  ASSERT_TRUE(buf.append(key, 1, 1, 0, 10, false, "aaaaa", &complete, &out, &term, &last_term));
  EXPECT_FALSE(buf.append(key, 1, 1, 0, 10, false, "bbbbb", &complete, &out, &term, &last_term));
}
