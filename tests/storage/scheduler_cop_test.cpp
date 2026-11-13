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

#include "openkv/storage/coprocessor.hpp"
#include "openkv/storage/kv_engine.hpp"
#include "openkv/storage/scheduler.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <future>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

TEST(SchedulerTest, LatchSerializesConflict) {
  openkv::storage::Scheduler sched;
  std::atomic<int> inflight{0};
  std::atomic<int> max_in{0};
  auto run = [&] {
    std::promise<uint64_t> p;
    auto f = p.get_future();
    sched.lock_key_async("k", [&p](uint64_t tok) { p.set_value(tok); });
    const uint64_t tok = f.get();
    int v = ++inflight;
    int m = max_in.load();
    while (v > m && !max_in.compare_exchange_weak(m, v)) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    --inflight;
    sched.unlock(tok);
  };
  std::thread t1(run);
  std::thread t2(run);
  t1.join();
  t2.join();
  EXPECT_EQ(max_in.load(), 1);
}

TEST(SchedulerTest, LatchHoldUntilUnlock) {
  openkv::storage::Scheduler sched;
  std::promise<uint64_t> first;
  auto first_fut = first.get_future();
  sched.lock_key_async("k", [&first](uint64_t tok) { first.set_value(tok); });
  const uint64_t tok = first_fut.get();
  ASSERT_NE(tok, 0u);

  std::atomic<bool> second_done{false};
  std::promise<uint64_t> second;
  auto second_fut = second.get_future();
  sched.lock_key_async("k", [&second](uint64_t t) { second.set_value(t); });
  std::thread t2([&] {
    const uint64_t tok2 = second_fut.get();
    sched.unlock(tok2);
    second_done.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  EXPECT_FALSE(second_done.load());
  sched.unlock(tok);
  t2.join();
  EXPECT_TRUE(second_done.load());
}

TEST(SchedulerTest, LatchAsyncDoesNotBlockCaller) {
  openkv::storage::Scheduler sched;
  std::atomic<int> granted{0};
  std::atomic<uint64_t> first_tok{0};
  std::atomic<uint64_t> second_tok{0};

  sched.lock_key_async("k", [&](uint64_t tok) {
    first_tok.store(tok);
    ++granted;
  });
  ASSERT_NE(first_tok.load(), 0u);
  EXPECT_EQ(granted.load(), 1);

  std::atomic<bool> second_scheduled{false};
  sched.lock_key_async("k", [&](uint64_t tok) {
    second_tok.store(tok);
    ++granted;
  });
  second_scheduled.store(true);
  // 冲突时立即返回，不 cv.wait；回调等 unlock 后触发。
  EXPECT_TRUE(second_scheduled.load());
  EXPECT_EQ(granted.load(), 1);
  EXPECT_EQ(second_tok.load(), 0u);

  sched.unlock(first_tok.load());
  EXPECT_EQ(granted.load(), 2);
  EXPECT_NE(second_tok.load(), 0u);
  sched.unlock(second_tok.load());
}

TEST(CoprocessorTest, PrefixScanInRange) {
  auto dir = fs::temp_directory_path() / ("skv_cop_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);
  openkv::storage::ShardEngineManager mgr(dir.string());
  auto& eng = mgr.get_or_create(0);
  eng.put("a1", "v1", 1);
  eng.put("a2", "v2", 2);
  eng.put("b1", "v3", 3);
  openkv::storage::Coprocessor cop(&eng);
  openkv::storage::CoprocessorRequest req;
  req.start_key = "a";
  req.end_key = "b";
  req.limit = 10;
  openkv::KeyRange range;  // full
  auto resp = cop.execute(req, range);
  EXPECT_EQ(resp.pairs.size(), 2u);
  EXPECT_EQ(resp.matched, 2u);
  fs::remove_all(dir);
}

TEST(CoprocessorTest, KeyPrefixKeysOnlyAndCount) {
  auto dir = fs::temp_directory_path() / ("skv_cop2_" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir);
  openkv::storage::ShardEngineManager mgr(dir.string());
  auto& eng = mgr.get_or_create(0);
  eng.put("user:1", "alice", 1);
  eng.put("user:2", "bob", 2);
  eng.put("ord:1", "x", 3);
  openkv::storage::Coprocessor cop(&eng);
  openkv::KeyRange range;

  openkv::storage::CoprocessorRequest keys;
  keys.key_prefix = "user:";
  keys.keys_only = true;
  keys.limit = 10;
  auto kr = cop.execute(keys, range);
  ASSERT_EQ(kr.pairs.size(), 2u);
  EXPECT_TRUE(kr.pairs[0].value.empty());
  EXPECT_EQ(kr.matched, 2u);

  openkv::storage::CoprocessorRequest cnt;
  cnt.key_prefix = "user:";
  cnt.count_only = true;
  cnt.limit = 10;
  auto cr = cop.execute(cnt, range);
  EXPECT_TRUE(cr.pairs.empty());
  EXPECT_EQ(cr.matched, 2u);

  openkv::storage::CoprocessorRequest vp;
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
  openkv::storage::ShardEngineManager mgr(dir.string());
  auto& a = mgr.get_or_create(1);
  auto& b = mgr.get_or_create(2);
  EXPECT_EQ(&a, &b);
  EXPECT_TRUE(fs::exists(dir / "kv"));
  fs::remove_all(dir);
}
