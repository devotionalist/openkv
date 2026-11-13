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

// @file grpc_shutdown_test.cpp
// SKV_GRPC_SHUTDOWN_MS 解析；graceful_grpc_shutdown(nullptr) 不崩。

#include "openkv/grpc_shutdown.hpp"
#include "openkv/protocol.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

namespace {

class EnvGuard {
 public:
  explicit EnvGuard(const char* key) : key_(key) {
    if (const char* v = std::getenv(key)) {
      had_ = true;
      old_ = v;
    }
  }
  ~EnvGuard() {
    if (had_) {
      setenv(key_, old_.c_str(), 1);
    } else {
      unsetenv(key_);
    }
  }
  EnvGuard(const EnvGuard&) = delete;
  EnvGuard& operator=(const EnvGuard&) = delete;

 private:
  const char* key_;
  bool had_{false};
  std::string old_;
};

}  // namespace

TEST(GrpcShutdownTest, GraceMsDefaultWhenUnset) {
  EnvGuard g(openkv::proto::env::kGrpcShutdownMs);
  unsetenv(openkv::proto::env::kGrpcShutdownMs);
  EXPECT_EQ(openkv::grpc_shutdown_grace_ms(15000), 15000);
  EXPECT_EQ(openkv::grpc_shutdown_grace_ms(3000), 3000);
}

TEST(GrpcShutdownTest, GraceMsFromEnv) {
  EnvGuard g(openkv::proto::env::kGrpcShutdownMs);
  setenv(openkv::proto::env::kGrpcShutdownMs, "8000", 1);
  EXPECT_EQ(openkv::grpc_shutdown_grace_ms(), 8000);
  setenv(openkv::proto::env::kGrpcShutdownMs, "0", 1);
  EXPECT_EQ(openkv::grpc_shutdown_grace_ms(), 0);
}

TEST(GrpcShutdownTest, GraceMsRejectsGarbage) {
  EnvGuard g(openkv::proto::env::kGrpcShutdownMs);
  setenv(openkv::proto::env::kGrpcShutdownMs, "nope", 1);
  EXPECT_EQ(openkv::grpc_shutdown_grace_ms(12000), 12000);
}

TEST(GrpcShutdownTest, NullServerNoop) {
  EXPECT_NO_THROW(openkv::graceful_grpc_shutdown(static_cast<grpc::Server*>(nullptr), 0));
}
