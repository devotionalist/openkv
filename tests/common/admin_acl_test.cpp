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

// @file admin_acl_test.cpp
// O1：Admin ACL 分类与连接级 AUTH（Meta handle_line）。

#include "opskv/admin_acl.hpp"
#include "opskv/meta/meta_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <thread>

using opskv::AdminSession;
using opskv::ClusterBootstrap;
using opskv::admin_allowed;
using opskv::admin_try_auth;
using opskv::meta_needs_admin;
using opskv::storage_needs_admin;
using opskv::meta::MetaNode;

TEST(AdminAclTest, ClassifyOps) {
  EXPECT_TRUE(storage_needs_admin("ADMIN_SPLIT"));
  EXPECT_TRUE(storage_needs_admin("ADMIN_CHECKPOINT"));
  EXPECT_FALSE(storage_needs_admin("PUT"));
  EXPECT_FALSE(storage_needs_admin("PING"));
  EXPECT_TRUE(meta_needs_admin("DRAIN_NODE"));
  EXPECT_TRUE(meta_needs_admin("SCALE_OUT"));
  EXPECT_FALSE(meta_needs_admin("GET_ROUTE"));
  EXPECT_FALSE(meta_needs_admin("HEARTBEAT"));
  EXPECT_FALSE(meta_needs_admin("RAFT_AE"));
}

TEST(AdminAclTest, SessionAuth) {
  const std::string tok = "secret-token";
  AdminSession s;
  EXPECT_FALSE(admin_allowed(s, tok));
  EXPECT_FALSE(admin_try_auth(&s, tok, "wrong"));
  EXPECT_FALSE(admin_allowed(s, tok));
  EXPECT_TRUE(admin_try_auth(&s, tok, tok));
  EXPECT_TRUE(admin_allowed(s, tok));
  // ACL 关闭
  AdminSession s2;
  EXPECT_TRUE(admin_allowed(s2, ""));
}

TEST(AdminAclTest, MetaHandleLineRequiresAuth) {
  ::setenv("SKV_ADMIN_TOKEN", "test-acl-tok", 1);
  ClusterBootstrap boot;
  boot.node_id = "meta-acl";
  boot.listen_addr = "127.0.0.1:18551";
  boot.shard_count = 4;
  boot.replica_factor = 3;
  boot.storage_peers = {{"s0", "127.0.0.1:9100"}};
  MetaNode srv(boot);
  ASSERT_NO_THROW(srv.start());
  for (int i = 0; i < 40; ++i) {
    if (srv.is_leader()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(srv.is_leader());

  EXPECT_EQ(srv.handle_line("DRAIN_NODE s0"), "ERR unauthorized");
  EXPECT_EQ(srv.handle_line("GET_ROUTE").rfind("OK ROUTE", 0), 0u);

  AdminSession sess;
  EXPECT_EQ(srv.handle_line("AUTH wrong", &sess), "ERR unauthorized");
  EXPECT_EQ(srv.handle_line("AUTH test-acl-tok", &sess), "OK AUTH");
  // 鉴权后仍可能因节点状态失败，但不得再 unauthorized
  auto drain = srv.handle_line("DRAIN_NODE s0", &sess);
  EXPECT_NE(drain, "ERR unauthorized");

  srv.stop();
  ::unsetenv("SKV_ADMIN_TOKEN");
}
