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

// @file store_codec_test.cpp
// 测试 Command encode/decode 对 Put/Delete 及非法 payload 的处理。

#include "opskv/storage/store_service.hpp"

#include <gtest/gtest.h>

using opskv::storage::CmdOp;
using opskv::storage::Command;
using opskv::storage::decode_command;
using opskv::storage::encode_command;

TEST(StoreCodecTest, PutRoundTrip) {
  Command in{CmdOp::kPut, "hello", "world", 1234};
  auto bytes = encode_command(in);
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_EQ(out.op, CmdOp::kPut);
  EXPECT_EQ(out.key, "hello");
  EXPECT_EQ(out.value, "world");
  EXPECT_EQ(out.ttl_ms, 1234);
}

TEST(StoreCodecTest, DeleteRoundTrip) {
  Command in{CmdOp::kDelete, "k", "", 0};
  auto bytes = encode_command(in);
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_EQ(out.op, CmdOp::kDelete);
  EXPECT_EQ(out.key, "k");
  EXPECT_TRUE(out.value.empty());
}

TEST(StoreCodecTest, BinarySafe) {
  Command in{CmdOp::kPut, std::string("\0\1\2", 3), std::string("\xff\x00", 2), 0};
  auto bytes = encode_command(in);
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_EQ(out.key, in.key);
  EXPECT_EQ(out.value, in.value);
}

TEST(StoreCodecTest, RejectTruncated) {
  Command out;
  EXPECT_FALSE(decode_command("x", &out));
  EXPECT_FALSE(decode_command("", &out));
}

TEST(StoreCodecTest, RequestIdRoundTrip) {
  Command in{CmdOp::kPut, "k", "v", 0, 0xabcdef0123456789ULL};
  auto bytes = encode_command(in);
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_EQ(out.request_id, 0xabcdef0123456789ULL);
  // 无 request_id 的旧格式仍可解
  Command legacy{CmdOp::kPut, "k", "v", 0};
  auto old = encode_command(legacy);
  Command out2;
  ASSERT_TRUE(decode_command(old, &out2));
  EXPECT_EQ(out2.request_id, 0u);
}
