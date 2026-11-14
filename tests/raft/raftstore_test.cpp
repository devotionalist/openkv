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

// @file raftstore_test.cpp
// Store / Router / Hibernate / Apply 队列 QoS 烟雾。

#include "openkv/raft/file_raft_engine.hpp"
#include "openkv/raftstore/snap_worker.hpp"
#include "openkv/raftstore/store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <thread>

using openkv::raft::FileRaftEngine;
using openkv::raft::RaftNode;
using openkv::raft::Ready;
using openkv::raft::SyncMode;
using openkv::raftstore::SnapTask;
using openkv::raftstore::SnapWorker;
using openkv::raftstore::Store;

TEST(RaftstoreTest, StoreTickElectsLeader) {
  Store::Options opts;
  opts.hibernate_after_ms = 0;
  opts.batch.raft_workers = 1;
  opts.batch.apply_workers = 1;
  opts.batch.poll_timeout_ms = 20;
  Store store(opts, [](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}};
    o.election_timeout_ms = 100;
    o.heartbeat_ms = 40;
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {},
                                      [](auto&&...) {});
  });
  store.start();
  store.get_or_create(1);
  for (int i = 0; i < 80 && !store.get(1)->is_leader(); ++i) {
    store.schedule_tick();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(store.get(1)->is_leader());
  store.stop();
}

TEST(RaftstoreTest, HibernateSkipsTickUntilWake) {
  Store::Options opts;
  opts.hibernate_after_ms = 1;
  opts.batch.raft_workers = 1;
  opts.batch.apply_workers = 1;
  opts.batch.poll_timeout_ms = 20;
  Store store(opts, [](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}, {"n1", "b", false}};
    o.election_timeout_ms = 5000;
    o.heartbeat_ms = 1000;
    o.check_quorum = false;
    o.enable_pre_vote = false;
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {},
                                      [](auto&&...) {});
  });
  store.start();
  auto& n = store.get_or_create(9);
  // 非 Leader，立刻变冷
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  store.schedule_tick();  // 应进入 hibernate
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  store.schedule_tick();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_FALSE(n.is_leader());
  store.mark_hot(9);
  store.schedule_tick();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  store.stop();
}

TEST(RaftstoreTest, RequestReadyCoalescesByCount) {
  // request_ready 按 ready_coalesce_n 攒批后再 drive_ready。
  namespace fs = std::filesystem;
  const auto dir = fs::temp_directory_path() /
                   ("okv_req_ready_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<FileRaftEngine>(dir.string(), SyncMode::kNone);

  std::atomic<int> persists{0};
  std::atomic<size_t> max_entries{0};

  Store::Options opts;
  opts.hibernate_after_ms = 0;
  opts.batch.raft_workers = 1;
  opts.batch.apply_workers = 1;
  opts.batch.poll_timeout_ms = 20;
  opts.batch.batch_size = 64;
  opts.ready_coalesce_n = 4;
  opts.ready_coalesce_ms = 200;  // 主要靠条数触发，ms 作兜底

  Store store(opts, [engine](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}};
    o.election_timeout_ms = 80;
    o.heartbeat_ms = 30;
    o.external_io = true;
    o.raft_engine = engine;
    o.raft_group_id = shard;
    o.persist_sync = SyncMode::kNone;
    o.check_quorum = false;
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  });
  store.set_persist_fn([&](uint32_t rid, const Ready& rd) {
    ++persists;
    const auto nent = rd.entries.size();
    if (nent > max_entries.load()) max_entries.store(nent);
    if (auto* n = store.get(rid)) n->persist_ready(rd);
  });
  store.set_apply_fn([](uint32_t, uint64_t, const std::string&) {});
  store.start();
  auto& n = store.get_or_create(1);
  for (int i = 0; i < 80 && !n.is_leader(); ++i) {
    store.schedule_tick();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(n.is_leader());
  persists.store(0);
  max_entries.store(0);

  constexpr int kN = 8;
  for (int i = 0; i < kN; ++i) {
    ASSERT_TRUE(n.propose("r" + std::to_string(i)));
    store.request_ready(1);
  }
  for (int i = 0; i < 100; ++i) {
    if (persists.load() >= 2 && max_entries.load() >= 4) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  store.stop();
  fs::remove_all(dir);

  EXPECT_GE(persists.load(), 1);
  EXPECT_LE(persists.load(), (kN / 4) + 2) << "count coalesce should flush about every 4 proposes";
  EXPECT_GE(max_entries.load(), 4u) << "one Ready should carry coalesce_n entries";
}

TEST(RaftstoreTest, DriveReadyCoalescesPersist) {
  // 同批多个 drive_ready 应合并为更少 persist；persist 睡眠期间 propose 可继续入队。
  namespace fs = std::filesystem;
  const auto dir = fs::temp_directory_path() /
                   ("okv_ready_coalesce_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<FileRaftEngine>(dir.string(), SyncMode::kNone);

  std::atomic<int> persists{0};
  std::atomic<size_t> max_entries{0};

  Store::Options opts;
  opts.hibernate_after_ms = 0;
  opts.batch.raft_workers = 1;
  opts.batch.apply_workers = 1;
  opts.batch.poll_timeout_ms = 20;
  opts.batch.batch_size = 64;

  Store store(opts, [engine](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}};
    o.election_timeout_ms = 80;
    o.heartbeat_ms = 30;
    o.external_io = true;
    o.raft_engine = engine;
    o.raft_group_id = shard;
    o.persist_sync = SyncMode::kNone;
    o.check_quorum = false;
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  });
  store.set_persist_fn([&](uint32_t rid, const Ready& rd) {
    ++persists;
    const auto nent = rd.entries.size();
    if (nent > max_entries.load()) max_entries.store(nent);
    // 模拟 fsync 占住 raft worker，让后续 Wake 堆积后合并。
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    if (auto* n = store.get(rid)) n->persist_ready(rd);
  });
  store.set_apply_fn([](uint32_t, uint64_t, const std::string&) {});
  store.start();
  auto& n = store.get_or_create(1);
  for (int i = 0; i < 80 && !n.is_leader(); ++i) {
    store.schedule_tick();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(n.is_leader());
  persists.store(0);
  max_entries.store(0);

  constexpr int kN = 8;
  for (int i = 0; i < kN; ++i) {
    ASSERT_TRUE(n.propose("p" + std::to_string(i)));
    store.drive_ready(1);
  }
  // 等到至少一次合并 Ready，或已刷完一段时间。
  for (int i = 0; i < 200; ++i) {
    if (max_entries.load() >= 2 || (persists.load() >= 1 && i > 40)) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  store.stop();
  fs::remove_all(dir);

  EXPECT_GE(persists.load(), 1);
  EXPECT_LT(persists.load(), kN) << "group commit should coalesce Wake into fewer persists";
  EXPECT_GE(max_entries.load(), 2u) << "at least one Ready should carry multiple entries";
}

TEST(RaftstoreTest, PersistPipelineUnblocksRaftWorker) {
  // persist_fn 阻塞时，同 Store 上另一 Region 的 Tick 仍应能推进（IO 不占 raft worker）。
  namespace fs = std::filesystem;
  const auto dir = fs::temp_directory_path() /
                   ("okv_persist_pipe_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(dir);
  auto engine = std::make_shared<FileRaftEngine>(dir.string(), SyncMode::kNone);

  std::atomic<bool> persist_entered{false};
  std::atomic<bool> release_persist{false};

  Store::Options opts;
  opts.hibernate_after_ms = 0;
  opts.batch.raft_workers = 1;  // 故意单 worker，突出跨 Region 重叠
  opts.batch.apply_workers = 1;
  opts.batch.poll_timeout_ms = 20;
  opts.persist_workers = 2;
  opts.ready_coalesce_n = 1;
  opts.ready_coalesce_ms = 0;

  Store store(opts, [engine](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}};
    o.election_timeout_ms = 80;
    o.heartbeat_ms = 30;
    o.external_io = true;
    o.raft_engine = engine;
    o.raft_group_id = shard;
    o.persist_sync = SyncMode::kNone;
    o.check_quorum = false;
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {}, [](auto&&...) {});
  });
  store.set_persist_fn([&](uint32_t rid, const Ready& rd) {
    if (rid == 1) {
      persist_entered.store(true);
      while (!release_persist.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    if (auto* n = store.get(rid)) n->persist_ready(rd);
  });
  store.set_apply_fn([](uint32_t, uint64_t, const std::string&) {});
  store.start();

  auto& n1 = store.get_or_create(1);
  auto& n2 = store.get_or_create(2);
  for (int i = 0; i < 80 && (!n1.is_leader() || !n2.is_leader()); ++i) {
    store.schedule_tick();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(n1.is_leader());
  ASSERT_TRUE(n2.is_leader());

  ASSERT_TRUE(n1.propose("block"));
  store.drive_ready(1);
  for (int i = 0; i < 200 && !persist_entered.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(persist_entered.load()) << "persist IO should start";

  // Region1 落盘卡住期间，Region2 应仍能选主后的 propose/Ready。
  ASSERT_TRUE(n2.propose("other"));
  store.drive_ready(2);
  bool n2_applied = false;
  for (int i = 0; i < 100; ++i) {
    if (n2.soft_state().commit_index >= 1) {
      n2_applied = true;
      break;
    }
    store.schedule_tick();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  release_persist.store(true);
  store.stop();
  fs::remove_all(dir);
  EXPECT_TRUE(n2_applied) << "region2 should progress while region1 persist blocked";
}

TEST(RaftstoreTest, ApplyQosFlag) {
  Store::Options opts;
  opts.batch.max_apply_queue_depth = 1;
  opts.batch.raft_workers = 1;
  opts.batch.apply_workers = 1;
  Store store(opts, [](uint32_t shard) {
    RaftNode::Options o;
    o.shard_id = shard;
    o.self_id = "n0";
    o.peers = {{"n0", "a", false}};
    return std::make_shared<RaftNode>(o, [](uint64_t, const std::string&) {},
                                      [](auto&&...) {});
  });
  store.start();
  store.get_or_create(3);
  EXPECT_FALSE(store.apply_queue_overloaded(3));
  store.stop();
}

TEST(SnapWorkerTest, CoalesceSameRegion) {
  std::atomic<int> sends{0};
  SnapWorker::Options opts;
  opts.coalesce_region = true;
  opts.queue_capacity = 8;
  SnapWorker w(
      opts, [](uint32_t, openkv::raft::SnapshotMeta*) { return std::string("blob"); },
      [&](uint32_t, const openkv::raft::SnapshotMeta&, const std::string&) { ++sends; });
  w.start();
  SnapTask a;
  a.region_id = 7;
  a.data = "old";
  SnapTask b;
  b.region_id = 7;
  b.data = "new";
  EXPECT_TRUE(w.enqueue(std::move(a)));
  EXPECT_TRUE(w.enqueue(std::move(b)));
  for (int i = 0; i < 100 && sends.load() < 1; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  w.stop();
  EXPECT_EQ(sends.load(), 1);
  auto st = w.stats();
  EXPECT_GE(st.coalesced, 1u);
  EXPECT_EQ(st.sent, 1u);
}

TEST(SnapWorkerTest, RetryThenSucceed) {
  std::atomic<int> calls{0};
  SnapWorker::Options opts;
  opts.max_retries = 2;
  opts.coalesce_region = false;
  SnapWorker w(
      opts, [](uint32_t, openkv::raft::SnapshotMeta*) { return std::string("x"); },
      [&](uint32_t, const openkv::raft::SnapshotMeta&, const std::string&) {
        if (++calls <= 2) throw std::runtime_error("transient");
      });
  w.start();
  SnapTask t;
  t.region_id = 3;
  EXPECT_TRUE(w.enqueue(std::move(t)));
  for (int i = 0; i < 200 && w.stats().sent < 1; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  w.stop();
  EXPECT_EQ(w.stats().sent, 1u);
  EXPECT_GE(w.stats().retried, 1u);
  EXPECT_GE(calls.load(), 3);
}
