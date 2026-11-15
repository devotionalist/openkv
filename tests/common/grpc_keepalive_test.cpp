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

// @file grpc_keepalive_test.cpp
// 统一 keepalive 辅助：可构造 channel args / 挂到 ServerBuilder。

#include "openkv/grpc_keepalive.hpp"

#include <gtest/gtest.h>

TEST(GrpcKeepaliveTest, ClientArgsDefaults) {
  EXPECT_EQ(openkv::kGrpcKeepaliveTimeMs, 10000);
  EXPECT_EQ(openkv::kGrpcKeepaliveTimeoutMs, 3000);
  EXPECT_NO_THROW({
    auto args = openkv::grpc_client_channel_args();
    (void)args;
    auto args2 = openkv::grpc_client_channel_args(64 << 20);
    (void)args2;
  });
}

TEST(GrpcKeepaliveTest, InsecureChannelConstructible) {
  // 不要求对端在线；只验证 CreateCustomChannel + keepalive args 不抛。
  EXPECT_NO_THROW({
    auto ch = openkv::grpc_insecure_channel("127.0.0.1:1");
    ASSERT_NE(ch, nullptr);
  });
}

TEST(GrpcKeepaliveTest, ApplyServerKeepaliveNullSafe) {
  EXPECT_NO_THROW(openkv::apply_server_keepalive(nullptr));
  grpc::ServerBuilder builder;
  EXPECT_NO_THROW(openkv::apply_server_keepalive(&builder));
}
