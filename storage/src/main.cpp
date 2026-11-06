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

// @file main.cpp
// Storage 进程入口：主端口 gRPC（Store/Raft/Admin）。行协议仅作进程内 ExecLine 适配，不 listen。

#include "opskv/logging.hpp"
#include "opskv/metrics_http.hpp"
#include "opskv/storage/grpc_server.hpp"
#include "opskv/storage/storage_node.hpp"
#include "opskv/storage/store_service.hpp"
#include "opskv/storage/exec_line_handler.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

namespace {
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }
}  // namespace

int main(int argc, char** argv) {
  using namespace opskv;
  init_logging();
  std::string config_path = "conf/storage.conf";
  if (argc > 1) {
    config_path = argv[1];
  }

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  NodeConfig cfg;
  try {
    cfg = load_node_config(config_path);
  } catch (const std::exception& e) {
    SKV_WARN << "config load failed (" << e.what() << "), using defaults";
    cfg.node_id = "storage-0";
    cfg.listen_addr = "0.0.0.0:9100";
    cfg.meta_addr = "127.0.0.1:8500";
    cfg.data_dir = "./data/storage-0";
    cfg.shard_count = 8;
  }

  storage::StorageNode node(cfg);
  storage::StoreFacade facade(node);
  node.start();

  // ExecLineHandler：行协议解析；gRPC AdminService.ExecLine 复用。
  storage::ExecLineHandler line_server(facade);
  storage::StorageGrpcServer grpc_server(facade, cfg.listen_addr, &line_server);
  try {
    grpc_server.start();
  } catch (const std::exception& e) {
    SKV_ERROR << "grpc server failed: " << e.what();
    node.stop();
    return 1;
  }

  if (!cfg.bootstrap_regions.empty()) {
    SKV_INFO << "waiting for region-0 raft leader (rf=" << cfg.raft_peers.size() << ")...";
    for (int i = 0; i < 200; ++i) {
      auto* g = node.multi_raft().get(0);
      if (g && !g->soft_state().leader_id.empty()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  SKV_INFO << "local_leaders=" << node.multi_raft().leader_shards().size();

  MetricsHttpServer metrics_http;
  metrics_http.start(metrics_addr_from_env(), [&facade] { return facade.metrics().prometheus_text(); });

  while (!g_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  metrics_http.stop();
  grpc_server.stop();
  node.stop();
  return 0;
}
