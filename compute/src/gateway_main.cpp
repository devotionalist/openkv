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

// @file gateway_main.cpp
// gateway_node：gRPC KVService+DSService + HTTP /stats。

#include "openkv/compute/gateway_server.hpp"
#include "openkv/compute/store_client.hpp"
#include "openkv/logging.hpp"
#include "openkv/net/tcp_line_client.hpp"

#include <CLI/CLI.hpp>
#include <grpcpp/grpcpp.h>
#include <httplib.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <deque>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop = true; }

std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == ',') {
      // trim
      while (!cur.empty() && (cur.front() == ' ' || cur.front() == '\t')) cur.erase(cur.begin());
      while (!cur.empty() && (cur.back() == ' ' || cur.back() == '\t')) cur.pop_back();
      if (!cur.empty()) out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  while (!cur.empty() && (cur.front() == ' ' || cur.front() == '\t')) cur.erase(cur.begin());
  while (!cur.empty() && (cur.back() == ' ' || cur.back() == '\t')) cur.pop_back();
  if (!cur.empty()) out.push_back(cur);
  return out;
}

std::string default_stats_listen(const std::string& grpc_listen) {
  std::string host;
  uint16_t port = 0;
  if (!openkv::net::parse_host_port(grpc_listen, &host, &port) || port == 0) {
    return "127.0.0.1:19001";
  }
  if (host.empty() || host == "0.0.0.0" || host == "::") host = "127.0.0.1";
  return host + ":" + std::to_string(static_cast<int>(port) + 1);
}

void wait_storage(const std::vector<std::string>& addrs, std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (const auto& a : addrs) {
    std::string last_err = "unknown";
    for (;;) {
      try {
        openkv::compute::StoreClient c(a);
        c.set_timeout_ms(3000);
        c.ping();
        SKV_INFO << "storage ready: " << a;
        break;
      } catch (const std::exception& e) {
        last_err = e.what();
      } catch (...) {
        last_err = "non-std exception";
      }
      if (std::chrono::steady_clock::now() > deadline) {
        throw std::runtime_error("storage " + a + " not ready: " + last_err);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
}

/// 将 Go flag 风格 `-listen` 规范为 `--listen`，便于 CLI11。
void normalize_go_flags(int argc, char** argv) {
  static std::deque<std::string> rewritten;
  rewritten.clear();
  static const char* kFlags[] = {"listen", "stats-listen", "storages", "metas"};
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    for (const char* f : kFlags) {
      const std::string dash = std::string("-") + f;
      const std::string ddash = std::string("--") + f;
      if (a == dash) {
        rewritten.push_back(ddash);
        argv[i] = rewritten.back().data();
        break;
      }
      if (a.rfind(dash + "=", 0) == 0) {
        rewritten.push_back(ddash + a.substr(dash.size()));
        argv[i] = rewritten.back().data();
        break;
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  using namespace openkv;
  init_logging();
  normalize_go_flags(argc, argv);

  std::string listen = "0.0.0.0:19000";
  std::string stats_listen;
  std::string storages = "127.0.0.1:9100";
  std::string metas = "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502";

  CLI::App app{"OpenKV C++ compute gateway"};
  app.add_option("--listen", listen, "gRPC listen address");
  app.add_option("--stats-listen", stats_listen, "HTTP /stats listen (default: gRPC port+1)");
  app.add_option("--storages", storages, "comma-separated storage addresses");
  app.add_option("--metas", metas, "comma-separated meta addresses");
  CLI11_PARSE(app, argc, argv);

  if (stats_listen.empty()) stats_listen = default_stats_listen(listen);

  auto storage_addrs = split_csv(storages);
  auto meta_addrs = split_csv(metas);

  try {
    wait_storage(storage_addrs, std::chrono::seconds(45));
  } catch (const std::exception& e) {
    SKV_ERROR << e.what();
    return 1;
  }

  compute::GatewayServer gateway(storage_addrs, meta_addrs);
  gateway.start_background();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
  builder.RegisterService(static_cast<kv::v1::KVService::CallbackService*>(&gateway));
  builder.RegisterService(static_cast<kv::v1::DSService::CallbackService*>(&gateway));
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server) {
    SKV_ERROR << "failed to start gRPC on " << listen;
    gateway.stop_background();
    return 1;
  }
  SKV_INFO << "compute-gateway gRPC " << listen << " storages=" << storages
           << " (callback + workers=" << gateway.workers() << ")";

  std::string stats_host;
  uint16_t stats_port = 0;
  httplib::Server http;
  std::thread http_thr;
  if (net::parse_host_port(stats_listen, &stats_host, &stats_port) && stats_port > 0) {
    if (stats_host.empty()) stats_host = "0.0.0.0";
    http.Get("/stats", [&gateway](const httplib::Request&, httplib::Response& res) {
      res.set_content(gateway.stats_json(), "application/json");
    });
    http_thr = std::thread([&http, stats_host, stats_port] {
      SKV_INFO << "compute-gateway stats HTTP " << stats_host << ":" << stats_port;
      http.listen(stats_host, static_cast<int>(stats_port));
    });
  } else {
    SKV_WARN << "bad stats-listen=" << stats_listen;
  }

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  http.stop();
  if (http_thr.joinable()) http_thr.join();
  server->Shutdown();
  server->Wait();
  gateway.stop_background();
  return 0;
}
