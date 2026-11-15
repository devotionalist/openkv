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

#include "openkv/compute/gateway_server.hpp"

#include <gtest/gtest.h>

using openkv::compute::store_skip_err;

TEST(StoreSkipErrTest, MatchesDialFailures) {
  EXPECT_TRUE(store_skip_err("connection refused"));
  EXPECT_TRUE(store_skip_err("connection reset by peer"));
  EXPECT_TRUE(store_skip_err("transport: Error while dialing 127.0.0.1:9101"));
  EXPECT_TRUE(store_skip_err("Unavailable: failed to connect"));
  EXPECT_TRUE(store_skip_err("failed to connect to all addresses"));
  EXPECT_TRUE(store_skip_err("no_region for key"));
  EXPECT_TRUE(store_skip_err("Deadline Exceeded"));
  EXPECT_TRUE(store_skip_err("context deadline exceeded"));
  EXPECT_TRUE(store_skip_err("Socket closed"));
}

TEST(StoreSkipErrTest, RejectsBusinessErrors) {
  EXPECT_FALSE(store_skip_err("not_leader redirect=127.0.0.1:9100"));
  EXPECT_FALSE(store_skip_err("locked primary_lock=…"));
  EXPECT_FALSE(store_skip_err("write_conflict"));
  EXPECT_FALSE(store_skip_err(""));
}
