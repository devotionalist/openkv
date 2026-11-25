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

// @file index_waiter_test.cpp
// IndexWaiter：即时唤醒 / 超时 / 异步回调。

#include "openkv/raftstore/index_waiter.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

TEST(IndexWaiterTest, InvokeWakesWaiter) {
  openkv::raftstore::IndexWaiter q;
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.invoke(7, "");
  });
  const auto t0 = std::chrono::steady_clock::now();
  auto out = q.wait(7, std::chrono::milliseconds(2000));
  const auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
  EXPECT_LT(ms, 500);
}

TEST(IndexWaiterTest, InvokeWithError) {
  openkv::raftstore::IndexWaiter q;
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    q.invoke(3, "cas_failed current_revision=9");
  });
  auto out = q.wait(3, std::chrono::milliseconds(1000));
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_EQ(out.error, "cas_failed current_revision=9");
}

TEST(IndexWaiterTest, Timeout) {
  openkv::raftstore::IndexWaiter q;
  auto out = q.wait(99, std::chrono::milliseconds(40));
  EXPECT_TRUE(out.timed_out);
  EXPECT_GE(q.timeout_total(), 1u);
}

TEST(IndexWaiterTest, InvokeBeforeWait) {
  openkv::raftstore::IndexWaiter q;
  q.invoke(1, "");
  auto out = q.wait(1, std::chrono::milliseconds(100));
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
}

TEST(IndexWaiterTest, WaitAsyncInvoke) {
  openkv::raftstore::IndexWaiter q;
  std::promise<openkv::raftstore::IndexWaiter::WaitOutcome> done;
  q.wait_async(
      42, std::chrono::milliseconds(2000),
      [&](openkv::raftstore::IndexWaiter::WaitOutcome out) { done.set_value(std::move(out)); });
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    q.invoke(42, "");
  });
  auto out = done.get_future().get();
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
}

TEST(IndexWaiterTest, WaitAsyncTimeout) {
  openkv::raftstore::IndexWaiter q;
  std::promise<openkv::raftstore::IndexWaiter::WaitOutcome> done;
  q.wait_async(
      77, std::chrono::milliseconds(40),
      [&](openkv::raftstore::IndexWaiter::WaitOutcome out) { done.set_value(std::move(out)); });
  auto out = done.get_future().get();
  EXPECT_TRUE(out.timed_out);
  EXPECT_GE(q.timeout_total(), 1u);
}

TEST(IndexWaiterTest, WaitAsyncAlreadyDone) {
  openkv::raftstore::IndexWaiter q;
  q.invoke(5, "cas_failed");
  std::promise<openkv::raftstore::IndexWaiter::WaitOutcome> done;
  q.wait_async(
      5, std::chrono::milliseconds(1000),
      [&](openkv::raftstore::IndexWaiter::WaitOutcome out) { done.set_value(std::move(out)); });
  auto out = done.get_future().get();
  EXPECT_FALSE(out.timed_out);
  EXPECT_EQ(out.error, "cas_failed");
}

TEST(IndexWaiterTest, ConcurrentWaitInvokeNoTimeout) {
  openkv::raftstore::IndexWaiter q;
  constexpr int kN = 64;
  std::vector<std::thread> waiters;
  std::vector<std::thread> invokers;
  std::atomic<int> timeouts{0};
  waiters.reserve(kN);
  invokers.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    waiters.emplace_back([&q, &timeouts, i] {
      auto out = q.wait(static_cast<uint64_t>(i + 1), std::chrono::milliseconds(2000));
      if (out.timed_out) {
        timeouts.fetch_add(1);
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  for (int i = 0; i < kN; ++i) {
    invokers.emplace_back([&q, i] { q.invoke(static_cast<uint64_t>(i + 1), ""); });
  }
  for (auto& t : waiters) {
    t.join();
  }
  for (auto& t : invokers) {
    t.join();
  }
  EXPECT_EQ(timeouts.load(), 0);
}
