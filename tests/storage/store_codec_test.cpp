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

#include "openkv/storage/kv_engine.hpp"
#include "openkv/storage/store_service.hpp"

#include <cstring>
#include <gtest/gtest.h>

using openkv::storage::CmdOp;
using openkv::storage::Command;
using openkv::storage::decode_command;
using openkv::storage::encode_command;

TEST(StoreCodecTest, OnePcValueRoundTrip) {
  using openkv::storage::decode_onepc_value;
  using openkv::storage::encode_onepc_value;
  auto bytes = encode_onepc_value(100, 200, 0, "pri", "val");
  uint64_t start = 0, commit = 0;
  uint8_t op = 9;
  std::string primary, value;
  ASSERT_TRUE(decode_onepc_value(bytes, &start, &commit, &op, &primary, &value));
  EXPECT_EQ(start, 100u);
  EXPECT_EQ(commit, 200u);
  EXPECT_EQ(op, 0);
  EXPECT_EQ(primary, "pri");
  EXPECT_EQ(value, "val");
}

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

TEST(StoreCodecTest, LegacyRequestIdTrailerIgnored) {
  // 旧 8 字节 request_id 尾：解码忽略，不设 cas。
  auto bytes = encode_command(Command{CmdOp::kPut, "k", "v", 0});
  const uint64_t old_rid = 0xabcdef0123456789ULL;
  bytes.append(reinterpret_cast<const char*>(&old_rid), sizeof(old_rid));
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_EQ(out.op, CmdOp::kPut);
  EXPECT_EQ(out.key, "k");
  EXPECT_FALSE(out.cas);
  EXPECT_EQ(out.expected_revision, 0u);
}

TEST(StoreCodecTest, CasTrailerRoundTrip) {
  Command in{CmdOp::kPut, "k", "v", 0};
  in.cas = true;
  in.expected_revision = 42;
  auto bytes = encode_command(in);
  Command out;
  ASSERT_TRUE(decode_command(bytes, &out));
  EXPECT_TRUE(out.cas);
  EXPECT_EQ(out.expected_revision, 42u);

  // 旧 17 字节尾：request_id(8)|cas(1)|expected_revision(8)
  std::string old = encode_command(Command{CmdOp::kPut, "k", "v", 0});
  const uint64_t rid = 7;
  const uint64_t rev = 42;
  old.append(reinterpret_cast<const char*>(&rid), sizeof(rid));
  old.push_back(1);
  old.append(reinterpret_cast<const char*>(&rev), sizeof(rev));
  Command out2;
  ASSERT_TRUE(decode_command(old, &out2));
  EXPECT_TRUE(out2.cas);
  EXPECT_EQ(out2.expected_revision, 42u);
}

TEST(StoreCodecTest, KvPayloadExpireRoundTripAndLegacy) {
  using openkv::storage::KvRecord;
  using openkv::storage::decode_kv_payload;
  using openkv::storage::encode_kv_payload;
  using openkv::storage::kv_record_expired;

  auto neu = encode_kv_payload(9, 1234567890, "hello");
  KvRecord r;
  ASSERT_TRUE(decode_kv_payload(neu, &r));
  EXPECT_EQ(r.revision, 9u);
  EXPECT_EQ(r.expire_at_ms, 1234567890);
  EXPECT_EQ(r.value, "hello");
  EXPECT_TRUE(kv_record_expired(r, 1234567890));
  EXPECT_FALSE(kv_record_expired(r, 1234567889));

  // 旧格式：仅 revision|value
  std::string legacy(sizeof(uint64_t) + 3, '\0');
  uint64_t rev = 3;
  std::memcpy(legacy.data(), &rev, sizeof(rev));
  legacy[8] = 'a';
  legacy[9] = 'b';
  legacy[10] = 'c';
  KvRecord old;
  ASSERT_TRUE(decode_kv_payload(legacy, &old));
  EXPECT_EQ(old.revision, 3u);
  EXPECT_EQ(old.expire_at_ms, 0);
  EXPECT_EQ(old.value, "abc");
  EXPECT_FALSE(kv_record_expired(old, 999999999999LL));
}
