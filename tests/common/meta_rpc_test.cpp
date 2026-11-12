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

// @file meta_rpc_test.cpp
// format_routing_table_line 须与 MetaNode::format_route / Storage recover 解析一致。

#include "openkv/codec.hpp"
#include "openkv/meta_rpc.hpp"

#include <gtest/gtest.h>

#include "meta/v1/meta.pb.h"

TEST(MetaRpcTest, FormatRoutingTableLineIncludesNrepComma) {
  ::meta::v1::RoutingTable rt;
  rt.set_version(42);
  auto* sh = rt.add_shards();
  sh->set_shard_id(3);
  sh->set_start_key("bench-mr/00000150");
  sh->set_end_key("");
  sh->set_epoch(4);
  auto* r = sh->add_replicas();
  r->set_node_id("storage-0");
  r->set_address("storage-0.storage-headless.openkv.svc.cluster.local:9100");
  r->set_is_leader(true);

  const std::string line = openkv::format_routing_table_line(rt);
  EXPECT_EQ(line.rfind("OK ROUTE 42 1 1 ", 0), 0u);
  // rid:start:end:epoch:nrep,id@addr*L — RF=1 也必须有逗号，recover 才能解析 peers
  const std::string start = openkv::to_hex("bench-mr/00000150");
  const std::string expect_tok =
      "3:" + start + "::4:1,storage-0@storage-0.storage-headless.openkv.svc.cluster.local:9100*L";
  EXPECT_NE(line.find(expect_tok), std::string::npos) << line;
}
