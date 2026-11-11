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

// @file ds_encode_test.cpp
// DS 编解码：与 Go dsencode 对齐。

#include "openkv/compute/ds_encode.hpp"

#include <gtest/gtest.h>

using namespace openkv::compute::ds;

TEST(DsEncodeTest, MetaKeyPrefix) {
  const std::string ukey = "user:1";
  auto mk = meta_key(ukey);
  ASSERT_GE(mk.size(), ukey.size());
  EXPECT_EQ(mk.compare(0, ukey.size(), ukey), 0);
  EXPECT_NE(mk, ukey);
}

TEST(DsEncodeTest, HashRoundTrip) {
  Hash h{{"a", "1"}, {"b", "2"}};
  auto blob = encode_hash(h);
  auto got = decode_hash(blob);
  EXPECT_EQ(got["a"], "1");
  EXPECT_EQ(got["b"], "2");
}

TEST(DsEncodeTest, WrongType) {
  auto blob = encode_hash({{"f", "v"}});
  EXPECT_THROW(decode_set(blob), WrongTypeError);
}

TEST(DsEncodeTest, SetListZSet) {
  Set s{"x", "y"};
  auto sg = decode_set(encode_set(s));
  EXPECT_EQ(sg.size(), 2u);

  List l{"a", "b", "c"};
  auto lg = decode_list(encode_list(l));
  ASSERT_EQ(lg.size(), 3u);
  EXPECT_EQ(lg[1], "b");

  auto rng = normalize_range(3, 0, -1);
  ASSERT_TRUE(rng);
  EXPECT_EQ(rng->first, 0u);
  EXPECT_EQ(rng->second, 3u);

  ZSet z{{"m1", 1.5}, {"m0", 0.5}};
  auto zg = decode_zset(encode_zset(z));
  EXPECT_DOUBLE_EQ(zg["m1"], 1.5);
  auto ents = z_sorted(zg);
  ASSERT_EQ(ents.size(), 2u);
  EXPECT_EQ(ents[0].member, "m0");
  EXPECT_EQ(ents[1].member, "m1");
}
