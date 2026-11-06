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

// @file compute_node.cpp
// ComputeNode：注入 stub transport、bootstrap 路由、Meta watch 占位线程。

#include "opskv/compute/compute_node.hpp"

#include "opskv/logging.hpp"

#include <chrono>

namespace opskv::compute {

ComputeNode::ComputeNode(NodeConfig cfg) : cfg_(std::move(cfg)) {
  router_ = std::make_unique<Router>();
  // Demo transport：直连本地 storage；生产替换为 gRPC StoreService stub
  auto get_fn = [](const std::string& addr, uint32_t shard, std::string_view key,
                   bool linearizable) {
    ProxyGetResult r;
    SKV_DEBUG << "compute GET -> " << addr << " shard=" << shard << " key=" << key
              << " lin=" << linearizable;
    // 占位：真实实现调用 storage.v1.StoreService/Get
    r.ok = true;
    r.found = false;
    r.error = "stub_transport";
    return r;
  };
  auto put_fn = [](const std::string& addr, uint32_t shard, std::string_view key,
                   std::string_view value) {
    ProxyPutResult r;
    SKV_DEBUG << "compute PUT -> " << addr << " shard=" << shard << " key=" << key
              << " vlen=" << value.size();
    r.ok = true;
    r.revision = 1;
    return r;
  };
  proxy_ = std::make_unique<KvProxy>(*router_, std::move(get_fn), std::move(put_fn));
}

ComputeNode::~ComputeNode() { stop(); }

void ComputeNode::start() {
  if (running_.exchange(true)) {
    return;
  }
  router_->bootstrap_single(backend_storage_addr_, cfg_.shard_count == 0 ? 8 : cfg_.shard_count);
  watch_thread_ = std::thread([this] { watch_routing(); });
  SKV_INFO << "compute node started id=" << cfg_.node_id
           << " listen=" << cfg_.listen_addr
           << " route_ver=" << router_->version();
}

void ComputeNode::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  if (watch_thread_.joinable()) {
    watch_thread_.join();
  }
}

void ComputeNode::watch_routing() {
  // 生产：MetaService.WatchRoutingTable stream，热更新 Router
  while (running_) {
    SKV_DEBUG << "routing watch heartbeat meta=" << cfg_.meta_addr
              << " version=" << router_->version();
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
}

}  // namespace opskv::compute
