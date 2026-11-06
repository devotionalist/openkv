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

#include "opskv/region.hpp"

#include <gtest/gtest.h>

using opskv::KeyRange;
using opskv::RegionMeta;
using opskv::locate_region;
using opskv::sort_regions;

TEST(RegionTest, ContainsAndInf) {
  KeyRange all;
  EXPECT_TRUE(all.contains(""));
  EXPECT_TRUE(all.contains("zzz"));

  KeyRange mid;
  mid.start_key = "a";
  mid.end_key = "m";
  EXPECT_FALSE(mid.contains(""));
  EXPECT_TRUE(mid.contains("a"));
  EXPECT_TRUE(mid.contains("b"));
  EXPECT_FALSE(mid.contains("m"));
}

TEST(RegionTest, LocateSorted) {
  std::vector<RegionMeta> rs = {{0, {"", "m"}}, {1, {"m", ""}}};
  sort_regions(rs);
  EXPECT_EQ(*locate_region(rs, "a"), 0u);
  EXPECT_EQ(*locate_region(rs, "m"), 1u);
  EXPECT_EQ(*locate_region(rs, "z"), 1u);
}

TEST(RegionTest, CanSplitAt) {
  KeyRange r;
  r.start_key = "a";
  r.end_key = "z";
  EXPECT_TRUE(r.can_split_at("m"));
  EXPECT_FALSE(r.can_split_at("a"));
  EXPECT_FALSE(r.can_split_at("z"));
  EXPECT_FALSE(r.can_split_at(""));
}
