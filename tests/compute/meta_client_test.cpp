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

// @file meta_client_test.cpp
// parse_redirect / TSO 默认配置 / ActiveTs（队列语义见 ts_range_queue_test）。

#include "openkv/compute/active_ts.hpp"
#include "openkv/compute/grpc_channel.hpp"
#include "openkv/compute/tso.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <thread>

using openkv::compute::ActiveTsTracker;
using openkv::compute::parse_redirect;
using openkv::compute::TSOracle;

TEST(GrpcUtilTest, ParseRedirect) {
  EXPECT_EQ(parse_redirect("ERR not_leader redirect=127.0.0.1:8500"), "127.0.0.1:8500");
  EXPECT_EQ(parse_redirect("OK"), "");
}

TEST(TsoTest, DefaultBatchAndWatermark) {
  TSOracle o({}, 0, 0);
  EXPECT_EQ(o.batch_size(), openkv::compute::kDefaultTsoBatch);
  EXPECT_EQ(o.low_watermark(), openkv::compute::kDefaultTsoLowWatermark);
}

TEST(ActiveTsTest, MinAndExpire) {
  ActiveTsTracker tr(std::chrono::milliseconds(50));
  EXPECT_EQ(tr.min(), std::numeric_limits<uint64_t>::max());
  tr.add(100);
  tr.add(50);
  EXPECT_EQ(tr.min(), 50u);
  tr.remove(50);
  EXPECT_EQ(tr.min(), 100u);
  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  EXPECT_EQ(tr.min(), std::numeric_limits<uint64_t>::max());
}
