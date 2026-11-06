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

// @file scheduler_cop_test.cpp
// Scheduler latch + Coprocessor + 单 Store 引擎烟雾。

#include "opskv/storage/coprocessor.hpp"
#include "opskv/storage/kv_engine.hpp"
#include "opskv/storage/scheduler.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

TEST(SchedulerTest, LatchSerializesConflict) {
  opskv::storage::Scheduler sched;
  std::atomic<int> inflight{0};
  std::atomic<int> max_in{0};
  sched.set_write_fn([&](uint32_t, std::string, uint64_t*, std::string*) {
    int v = ++inflight;
    int m = max_in.load();
    while (v > m && !max_in.compare_exchange_weak(m, v)) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    --inflight;
    return true;
  });
  std::thread t1([&] {
    uint64_t i = 0;
    sched.schedule_write(0, "k", "a", &i, nullptr);
  });
  std::thread t2([&] {
    uint64_t i = 0;
    sched.schedule_write(0, "k", "b", &i, nullptr);
  });
  t1.join();
  t2.join();
  EXPECT_EQ(max_in.load(), 1);
}

TEST(CoprocessorTest, PrefixScanInRange) {
  auto dir = fs::temp_directory_path() / ("skv_cop_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);
  opskv::storage::ShardEngineManager mgr(dir.string());
  auto& eng = mgr.get_or_create(0);
  eng.put("a1", "v1", 1);
  eng.put("a2", "v2", 2);
  eng.put("b1", "v3", 3);
  opskv::storage::Coprocessor cop(&eng);
  opskv::storage::CoprocessorRequest req;
  req.start_key = "a";
  req.end_key = "b";
  req.limit = 10;
  opskv::KeyRange range;  // full
  auto resp = cop.execute(req, range);
  EXPECT_EQ(resp.pairs.size(), 2u);
  EXPECT_EQ(resp.matched, 2u);
  fs::remove_all(dir);
}

TEST(CoprocessorTest, KeyPrefixKeysOnlyAndCount) {
  auto dir = fs::temp_directory_path() / ("skv_cop2_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);
  opskv::storage::ShardEngineManager mgr(dir.string());
  auto& eng = mgr.get_or_create(0);
  eng.put("user:1", "alice", 1);
  eng.put("user:2", "bob", 2);
  eng.put("ord:1", "x", 3);
  opskv::storage::Coprocessor cop(&eng);
  opskv::KeyRange range;

  opskv::storage::CoprocessorRequest keys;
  keys.key_prefix = "user:";
  keys.keys_only = true;
  keys.limit = 10;
  auto kr = cop.execute(keys, range);
  ASSERT_EQ(kr.pairs.size(), 2u);
  EXPECT_TRUE(kr.pairs[0].value.empty());
  EXPECT_EQ(kr.matched, 2u);

  opskv::storage::CoprocessorRequest cnt;
  cnt.key_prefix = "user:";
  cnt.count_only = true;
  cnt.limit = 10;
  auto cr = cop.execute(cnt, range);
  EXPECT_TRUE(cr.pairs.empty());
  EXPECT_EQ(cr.matched, 2u);

  opskv::storage::CoprocessorRequest vp;
  vp.value_prefix = "a";
  vp.limit = 10;
  auto vr = cop.execute(vp, range);
  EXPECT_EQ(vr.matched, 1u);
  fs::remove_all(dir);
}

TEST(ShardEngineManagerTest, SingleStoreShared) {
  auto dir = fs::temp_directory_path() / ("skv_ss_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);
  opskv::storage::ShardEngineManager mgr(dir.string());
  auto& a = mgr.get_or_create(1);
  auto& b = mgr.get_or_create(2);
  EXPECT_EQ(&a, &b);
  EXPECT_TRUE(fs::exists(dir / "kv"));
  fs::remove_all(dir);
}
