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
// Compute 进程入口：加载配置、启动节点、演示 proxy put/get 后主循环等待信号。

#include "opskv/compute/compute_node.hpp"
#include "opskv/logging.hpp"

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
  std::string config_path = "conf/compute.conf";
  if (argc > 1) {
    config_path = argv[1];
  }

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  NodeConfig cfg;
  try {
    cfg = load_node_config(config_path);
  } catch (...) {
    cfg.node_id = "compute-0";
    cfg.listen_addr = "0.0.0.0:9000";
    cfg.meta_addr = "127.0.0.1:8500";
    cfg.shard_count = 8;
  }

  compute::ComputeNode node(cfg);
  node.start();

  auto put = node.proxy().put("hello", "world");
  SKV_INFO << "proxy put ok=" << put.ok << " err=" << put.error;
  auto get = node.proxy().get("hello");
  SKV_INFO << "proxy get ok=" << get.ok << " found=" << get.found << " err=" << get.error;

  while (!g_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  node.stop();
  return 0;
}
