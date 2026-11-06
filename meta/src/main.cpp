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
// Meta 进程入口：
// - argv[1] 可选 bootstrap 配置路径（默认 conf/bootstrap.conf）
// - 加载失败则用内置单机开发默认（meta-0:8500 + 3 storage）
// - SIGINT/SIGTERM → stop MetaNode 后退出

#include "opskv/logging.hpp"
#include "opskv/meta/meta_fsm.hpp"
#include "opskv/meta/meta_node.hpp"
#include "opskv/metrics_http.hpp"

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
  std::string config_path = "conf/bootstrap.conf";
  if (argc > 1) {
    config_path = argv[1];
  }

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  ClusterBootstrap boot;
  try {
    boot = load_bootstrap(config_path);
  } catch (...) {
    boot.node_id = "meta-0";
    boot.listen_addr = "0.0.0.0:8500";
    boot.meta_addr = "0.0.0.0:8500";
    boot.shard_count = 8;
    boot.replica_factor = 3;
    boot.storage_peers = {{"storage-0", "127.0.0.1:9100"},
                          {"storage-1", "127.0.0.1:9101"},
                          {"storage-2", "127.0.0.1:9102"}};
    boot.meta_peers = {{"meta-0", "127.0.0.1:8500"},
                       {"meta-1", "127.0.0.1:8501"},
                       {"meta-2", "127.0.0.1:8502"}};
  }

  meta::MetaNode server(boot);
  try {
    server.start();
  } catch (const std::exception& e) {
    SKV_ERROR << "meta start failed: " << e.what();
    return 1;
  }

  // 等待选主
  for (int i = 0; i < 100 && !g_stop; ++i) {
    if (!server.leader_address().empty()) {
      SKV_INFO << "meta leader=" << server.leader_address()
               << " self_leader=" << server.is_leader();
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // 不再自动注册 storage-3 / 跑 scale-out 演示（污染 Range 路由表）

  MetricsHttpServer metrics_http;
  // Meta 的 Prometheus 文本仍走 handle_line("METRICS")；HTTP 复用同一入口
  metrics_http.start(metrics_addr_from_env(), [&server] {
    return server.handle_line("METRICS");
  });

  while (!g_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  metrics_http.stop();
  server.stop();
  return 0;
}
