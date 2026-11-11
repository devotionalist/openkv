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

// @file store_facade_test.cpp
// 集成测试 StorageNode + StoreFacade：Raft 选主后 propose/get 端到端。

#include "openkv/storage/mvcc.hpp"
#include "openkv/storage/storage_node.hpp"
#include "openkv/storage/store_service.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using openkv::NodeConfig;
using openkv::storage::CmdOp;
using openkv::storage::Command;
using openkv::storage::StorageNode;
using openkv::storage::StoreFacade;

class StoreFacadeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / ("skv_facade_" + std::to_string(::getpid()));
    fs::remove_all(dir_);
    NodeConfig cfg;
    cfg.node_id = "storage-test";
    cfg.listen_addr = "127.0.0.1:0";
    cfg.data_dir = dir_.string();
    cfg.shard_count = 2;
    cfg.bootstrap_regions = {0, 1};
    cfg.raft_election_timeout_ms = 200;
    cfg.raft_heartbeat_ms = 50;
    node_ = std::make_unique<StorageNode>(cfg);
    facade_ = std::make_unique<StoreFacade>(*node_);
    node_->start();
    wait_leader(0);
  }

  void TearDown() override {
    if (node_) {
      node_->stop();
    }
    fs::remove_all(dir_);
  }

  void wait_leader(uint32_t shard) {
    for (int i = 0; i < 80; ++i) {
      if (auto* g = node_->multi_raft().get(shard); g && g->is_leader()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    FAIL() << "shard " << shard << " no leader";
  }

  fs::path dir_;
  std::unique_ptr<StorageNode> node_;
  std::unique_ptr<StoreFacade> facade_;
};

TEST_F(StoreFacadeTest, PutGet) {
  Command cmd{CmdOp::kPut, "foo", "bar", 0};
  auto pr = facade_->propose(0, cmd);
  ASSERT_TRUE(pr.ok) << pr.error;
  EXPECT_GT(pr.revision, 0u);

  auto gr = facade_->get(0, "foo", true);
  EXPECT_TRUE(gr.found);
  EXPECT_EQ(gr.value, "bar");
  EXPECT_TRUE(gr.is_leader);
}

TEST_F(StoreFacadeTest, Delete) {
  facade_->propose(0, {CmdOp::kPut, "d", "1", 0});
  auto pr = facade_->propose(0, {CmdOp::kDelete, "d", "", 0});
  ASSERT_TRUE(pr.ok) << pr.error;
  auto gr = facade_->get(0, "d", true);
  EXPECT_FALSE(gr.found);
}

TEST_F(StoreFacadeTest, IndependentShards) {
  wait_leader(1);
  // 单 Store 共享引擎：同 key 跨 Region 会覆盖；用不同 key 验证两 Region 均可写读。
  ASSERT_TRUE(facade_->propose(0, {CmdOp::kPut, "k0", "s0", 0}).ok);
  ASSERT_TRUE(facade_->propose(1, {CmdOp::kPut, "k1", "s1", 0}).ok);
  EXPECT_EQ(facade_->get(0, "k0", true).value, "s0");
  EXPECT_EQ(facade_->get(1, "k1", true).value, "s1");
  EXPECT_TRUE(node_->engines().shares_engine(0, 1));
}

TEST_F(StoreFacadeTest, ApplyEntriesBatchesPutAndOnePc) {
  using openkv::raft::LogEntry;
  using openkv::storage::encode_command;
  using openkv::storage::encode_onepc_value;
  using openkv::storage::MvccOp;
  using openkv::storage::MvccStore;

  std::vector<LogEntry> ents;
  for (int i = 0; i < 6; ++i) {
    Command cmd;
    cmd.op = CmdOp::kPut;
    cmd.key = "batch-put-" + std::to_string(i);
    cmd.value = "pv" + std::to_string(i);
    LogEntry e;
    e.index = 2000 + static_cast<uint64_t>(i);
    e.term = 1;
    e.data = encode_command(cmd);
    ents.push_back(std::move(e));
  }
  for (int i = 0; i < 4; ++i) {
    Command cmd;
    cmd.op = CmdOp::kOnePc;
    cmd.key = "batch-1pc-" + std::to_string(i);
    const uint64_t start = 500 + static_cast<uint64_t>(i) * 2;
    cmd.value = encode_onepc_value(start, start + 1, static_cast<uint8_t>(MvccOp::kPut), cmd.key,
                                   "ov" + std::to_string(i));
    LogEntry e;
    e.index = 3000 + static_cast<uint64_t>(i);
    e.term = 1;
    e.data = encode_command(cmd);
    ents.push_back(std::move(e));
  }
  node_->on_apply_entries(0, ents);

  for (int i = 0; i < 6; ++i) {
    auto gr = facade_->get(0, "batch-put-" + std::to_string(i), false);
    ASSERT_TRUE(gr.found) << i;
    EXPECT_EQ(gr.value, "pv" + std::to_string(i));
  }
  auto* eng = node_->engines().get(0);
  ASSERT_NE(eng, nullptr);
  MvccStore mvcc(*eng);
  for (int i = 0; i < 4; ++i) {
    bool locked = false;
    auto vis = mvcc.txn_get("batch-1pc-" + std::to_string(i), ~uint64_t{0}, &locked);
    ASSERT_TRUE(vis.has_value()) << i;
    EXPECT_EQ(*vis, "ov" + std::to_string(i));
    EXPECT_FALSE(locked);
  }
}

TEST_F(StoreFacadeTest, PlainCasCreateOnlyAndMatch) {
  Command create;
  create.op = CmdOp::kPut;
  create.key = "cas-k";
  create.value = "v1";
  create.cas = true;
  create.expected_revision = 0;
  auto c1 = facade_->propose(0, create);
  ASSERT_TRUE(c1.ok) << c1.error;

  auto again = facade_->propose(0, create);
  ASSERT_FALSE(again.ok);
  EXPECT_TRUE(again.cas_failed) << again.error;
  EXPECT_EQ(again.current_revision, c1.revision);

  auto gr = facade_->get(0, "cas-k", true);
  ASSERT_TRUE(gr.found);
  Command upd;
  upd.op = CmdOp::kPut;
  upd.key = "cas-k";
  upd.value = "v2";
  upd.cas = true;
  upd.expected_revision = gr.revision;
  auto c2 = facade_->propose(0, upd);
  ASSERT_TRUE(c2.ok) << c2.error;
  EXPECT_GT(c2.revision, gr.revision);

  upd.expected_revision = gr.revision;  // stale
  auto stale = facade_->propose(0, upd);
  ASSERT_FALSE(stale.ok);
  EXPECT_TRUE(stale.cas_failed);
  EXPECT_EQ(stale.current_revision, c2.revision);
}

TEST_F(StoreFacadeTest, OnePcPutGetAndCas) {
  auto r = facade_->one_pc(0, "1pc-k", 10, 11, "1pc-k", 0, "v1", 0, false, 0);
  ASSERT_TRUE(r.ok) << r.error;
  EXPECT_EQ(r.revision, 11u);
  auto tg = facade_->txn_get(0, "1pc-k", ~uint64_t{0}, true);
  ASSERT_TRUE(tg.found);
  EXPECT_EQ(tg.value, "v1");
  EXPECT_EQ(tg.revision, 11u);
  // 无残留 lock
  auto lk = facade_->get_lock(0, "1pc-k");
  EXPECT_FALSE(lk.found);

  auto create = facade_->one_pc(0, "1pc-new", 20, 21, "1pc-new", 0, "x", 0, true, 0);
  ASSERT_TRUE(create.ok) << create.error;
  auto stale = facade_->one_pc(0, "1pc-k", 30, 31, "1pc-k", 0, "v2", 0, true, 10);
  ASSERT_FALSE(stale.ok);
  EXPECT_TRUE(stale.cas_failed);
  auto ok = facade_->one_pc(0, "1pc-k", 30, 31, "1pc-k", 0, "v2", 0, true, 11);
  ASSERT_TRUE(ok.ok) << ok.error;
  auto tg2 = facade_->txn_get(0, "1pc-k", ~uint64_t{0}, true);
  ASSERT_TRUE(tg2.found);
  EXPECT_EQ(tg2.value, "v2");

  auto del = facade_->one_pc(0, "1pc-k", 40, 41, "1pc-k", 1, {}, 0, false, 0);
  ASSERT_TRUE(del.ok) << del.error;
  auto gone = facade_->txn_get(0, "1pc-k", ~uint64_t{0}, true);
  EXPECT_FALSE(gone.found);
}

TEST_F(StoreFacadeTest, MvccCasViaPrewrite) {
  // seed via prewrite+commit
  auto pw = facade_->prewrite(0, "m-cas", 100, "m-cas", 0, "a", 0, false, 0);
  ASSERT_TRUE(pw.ok) << pw.error;
  auto cm = facade_->commit(0, "m-cas", 100, 200, 0);
  ASSERT_TRUE(cm.ok) << cm.error;

  auto tg = facade_->txn_get(0, "m-cas", ~uint64_t{0}, true);
  ASSERT_TRUE(tg.found);
  EXPECT_EQ(tg.revision, 200u);

  auto bad = facade_->prewrite(0, "m-cas", 300, "m-cas", 0, "b", 0, true, 199);
  ASSERT_FALSE(bad.ok);
  EXPECT_TRUE(bad.cas_failed);

  auto good = facade_->prewrite(0, "m-cas", 300, "m-cas", 0, "b", 0, true, 200);
  ASSERT_TRUE(good.ok) << good.error;
  ASSERT_TRUE(facade_->commit(0, "m-cas", 300, 400, 0).ok);

  auto create = facade_->prewrite(0, "m-new", 500, "m-new", 0, "x", 0, true, 0);
  ASSERT_TRUE(create.ok) << create.error;
}

TEST_F(StoreFacadeTest, PlainTtlLazyExpireAndCompact) {
  using openkv::storage::stamp_expire_at_ms;
  Command cmd;
  cmd.op = CmdOp::kPut;
  cmd.key = "ttl-plain";
  cmd.value = "soon";
  cmd.ttl_ms = stamp_expire_at_ms(500);
  auto pr = facade_->propose(0, cmd);
  ASSERT_TRUE(pr.ok) << pr.error;

  auto hit = facade_->get(0, "ttl-plain", true);
  ASSERT_TRUE(hit.found) << "redirect=" << hit.redirect_to << " rev=" << hit.revision;
  EXPECT_EQ(hit.value, "soon");
  auto* eng = node_->engines().get(0);
  ASSERT_NE(eng, nullptr);
  auto raw0 = eng->get("ttl-plain", false);
  ASSERT_TRUE(raw0) << "missing raw after put";
  EXPECT_GT(raw0->expire_at_ms, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  auto miss = facade_->get(0, "ttl-plain", true);
  EXPECT_FALSE(miss.found);

  eng->compact_for_ttl();
  EXPECT_FALSE(eng->get("ttl-plain", /*hide_expired=*/false).has_value());
}

TEST_F(StoreFacadeTest, MvccTtlViaPrewrite) {
  using openkv::storage::stamp_expire_at_ms;
  // 寿命须覆盖 fixture 选主耗时；过短会导致 commit 后立刻不可见（假失败）
  const int64_t exp = stamp_expire_at_ms(5000);
  auto pw = facade_->prewrite(0, "ttl-m", 1000, "ttl-m", 0, "v", 0, false, 0, exp);
  ASSERT_TRUE(pw.ok) << pw.error;
  ASSERT_TRUE(facade_->commit(0, "ttl-m", 1000, 1100, 0).ok);

  auto tg = facade_->txn_get(0, "ttl-m", ~uint64_t{0}, true);
  ASSERT_TRUE(tg.found) << tg.error;
  EXPECT_EQ(tg.value, "v");

  // 直接把 expire 打到过去：避免再 sleep 数秒
  auto* eng = facade_->node().engines().get(0);
  ASSERT_NE(eng, nullptr);
  openkv::storage::MvccStore mvcc(*eng);
  const int64_t past = openkv::storage::wall_ms_now() - 10;
  mvcc.put_data("ttl-m", 1000, "v", 1100, past);
  mvcc.put_write("ttl-m", 1100, openkv::storage::WriteInfo{1000, openkv::storage::MvccOp::kPut, past},
                 1100);
  auto gone = facade_->txn_get(0, "ttl-m", ~uint64_t{0}, true);
  EXPECT_FALSE(gone.found);
  EXPECT_EQ(gone.revision, 0u);
}
