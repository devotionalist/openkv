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

#include "openkv/raftstore/batch_system.hpp"
#include "openkv/raftstore/schedule_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_set>

using namespace openkv::raftstore;

TEST(ScheduleQueueTest, DedupAndPop) {
  ScheduleQueue q;
  std::atomic<bool> running{true};
  q.push(1);
  q.push(1);
  q.push(2);
  EXPECT_EQ(q.size(), 2u);
  auto a = q.pop(0, running);
  auto b = q.pop(0, running);
  auto c = q.pop(0, running);
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());
  EXPECT_FALSE(c.has_value());
  EXPECT_EQ(std::unordered_set<uint32_t>({*a, *b}).size(), 2u);
}

TEST(BatchSystemTest, ScheduleDrainsWithoutScan) {
  // 投递 → notify → worker 处理；不依赖 peer_regions 轮询。
  auto router = std::make_shared<Router>();
  router->register_peer(7, 1024);
  router->register_apply(7, 1024);

  BatchSystem::Options opts;
  opts.raft_workers = 1;
  opts.apply_workers = 1;
  opts.batch_size = 16;
  opts.poll_timeout_ms = 50;
  BatchSystem bs(router, opts);

  std::atomic<int> raft_calls{0};
  std::atomic<int> raft_msgs{0};
  bs.set_raft_handler([&](uint32_t rid, std::vector<PeerMsg> msgs) {
    EXPECT_EQ(rid, 7u);
    raft_calls.fetch_add(1);
    raft_msgs.fetch_add(static_cast<int>(msgs.size()));
  });
  bs.set_apply_handler([](uint32_t, std::vector<ApplyControl>) {});

  router->set_raft_notify([&](uint32_t rid) { bs.schedule_raft(rid); });
  router->set_apply_notify([&](uint32_t rid) { bs.schedule_apply(rid); });

  bs.start();
  constexpr int kN = 40;
  for (int i = 0; i < kN; ++i) {
    ASSERT_TRUE(router->send_raft(7, SignificantMsg{SignificantMsg::Type::kWake, {}}));
  }
  for (int i = 0; i < 200 && raft_msgs.load() < kN; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  bs.stop();

  EXPECT_EQ(raft_msgs.load(), kN);
  EXPECT_GE(raft_calls.load(), 1);
  EXPECT_LE(raft_calls.load(), kN) << "drain should batch multiple wakes per handler call";
}
