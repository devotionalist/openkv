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

// @file proposal_queue_test.cpp
// W1：ProposalQueue 即时唤醒 / 超时（TiKV-style apply callback）。

#include "openkv/storage/proposal_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

TEST(ProposalQueueTest, InvokeWakesWaiter) {
  openkv::storage::ProposalQueue q;
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.invoke(7, "");
  });
  const auto t0 = std::chrono::steady_clock::now();
  auto out = q.wait(7, std::chrono::milliseconds(2000));
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
  EXPECT_LT(ms, 500);  // 不应被 5ms×N 空转拖到秒级
}

TEST(ProposalQueueTest, InvokeWithError) {
  openkv::storage::ProposalQueue q;
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    q.invoke(3, "cas_failed current_revision=9");
  });
  auto out = q.wait(3, std::chrono::milliseconds(1000));
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_EQ(out.error, "cas_failed current_revision=9");
}

TEST(ProposalQueueTest, Timeout) {
  openkv::storage::ProposalQueue q;
  auto out = q.wait(99, std::chrono::milliseconds(40));
  EXPECT_TRUE(out.timed_out);
  EXPECT_GE(q.timeout_total(), 1u);
}

TEST(ProposalQueueTest, InvokeBeforeWait) {
  openkv::storage::ProposalQueue q;
  q.invoke(1, "");
  auto out = q.wait(1, std::chrono::milliseconds(100));
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
}

TEST(ProposalQueueTest, WaitAsyncInvoke) {
  openkv::storage::ProposalQueue q;
  std::promise<openkv::storage::ProposalQueue::WaitOutcome> done;
  q.wait_async(42, std::chrono::milliseconds(2000),
               [&](openkv::storage::ProposalQueue::WaitOutcome out) { done.set_value(std::move(out)); });
  std::thread pub([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    q.invoke(42, "");
  });
  auto out = done.get_future().get();
  pub.join();
  EXPECT_FALSE(out.timed_out);
  EXPECT_TRUE(out.error.empty());
}

TEST(ProposalQueueTest, WaitAsyncTimeout) {
  openkv::storage::ProposalQueue q;
  std::promise<openkv::storage::ProposalQueue::WaitOutcome> done;
  q.wait_async(77, std::chrono::milliseconds(40),
               [&](openkv::storage::ProposalQueue::WaitOutcome out) { done.set_value(std::move(out)); });
  auto out = done.get_future().get();
  EXPECT_TRUE(out.timed_out);
  EXPECT_GE(q.timeout_total(), 1u);
}

TEST(ProposalQueueTest, WaitAsyncAlreadyDone) {
  openkv::storage::ProposalQueue q;
  q.invoke(5, "cas_failed");
  std::promise<openkv::storage::ProposalQueue::WaitOutcome> done;
  q.wait_async(5, std::chrono::milliseconds(1000),
               [&](openkv::storage::ProposalQueue::WaitOutcome out) { done.set_value(std::move(out)); });
  auto out = done.get_future().get();
  EXPECT_FALSE(out.timed_out);
  EXPECT_EQ(out.error, "cas_failed");
}

TEST(ProposalQueueTest, ConcurrentWaitInvokeNoTimeout) {
  openkv::storage::ProposalQueue q;
  constexpr int kN = 64;
  std::vector<std::thread> waiters;
  std::vector<std::thread> invokers;
  std::atomic<int> timeouts{0};
  waiters.reserve(kN);
  invokers.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    waiters.emplace_back([&q, &timeouts, i] {
      auto out = q.wait(static_cast<uint64_t>(i + 1), std::chrono::milliseconds(2000));
      if (out.timed_out) timeouts.fetch_add(1);
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  for (int i = 0; i < kN; ++i) {
    invokers.emplace_back([&q, i] { q.invoke(static_cast<uint64_t>(i + 1), ""); });
  }
  for (auto& t : waiters) t.join();
  for (auto& t : invokers) t.join();
  EXPECT_EQ(timeouts.load(), 0);
}
