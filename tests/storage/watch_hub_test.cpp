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

#include "opskv/storage/watch_hub.hpp"

#include <gtest/gtest.h>

using opskv::storage::WatchHub;
using opskv::storage::WatchRecord;

TEST(WatchHubTest, PollFiltersAndOrders) {
  WatchHub hub(8);
  hub.push(WatchRecord{1, 10, false, "a", "1"});
  hub.push(WatchRecord{2, 11, false, "b", "2"});
  hub.push(WatchRecord{1, 12, true, "a", {}});
  auto all = hub.poll(1, 9, 10);
  ASSERT_EQ(all.size(), 2u);
  EXPECT_EQ(all[0].revision, 10u);
  EXPECT_EQ(all[1].revision, 12u);
  EXPECT_TRUE(all[1].deleted);
  auto empty = hub.poll(1, 12, 10);
  EXPECT_TRUE(empty.empty());
}
