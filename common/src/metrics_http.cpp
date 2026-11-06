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

// @file metrics_http.cpp
// cpp-httplib 后台线程 listen；GET /metrics → text_fn()。

#include "opskv/metrics_http.hpp"

#include "opskv/logging.hpp"
#include "opskv/net/tcp_line_client.hpp"

#include <httplib.h>

#include <atomic>
#include <cstdlib>
#include <thread>
#include <utility>

namespace opskv {

struct MetricsHttpServer::Impl {
  std::unique_ptr<httplib::Server> srv;
  std::thread thr;
  std::atomic<bool> running{false};
};

MetricsHttpServer::MetricsHttpServer() : impl_(std::make_unique<Impl>()) {}

MetricsHttpServer::~MetricsHttpServer() { stop(); }

void MetricsHttpServer::start(std::string listen_addr, MetricsTextFn text_fn) {
  stop();
  if (listen_addr.empty() || !text_fn) return;

  std::string host;
  uint16_t port = 0;
  if (!net::parse_host_port(listen_addr, &host, &port) || port == 0) {
    SKV_WARN << "metrics http bad addr=" << listen_addr;
    return;
  }
  if (host.empty()) host = "0.0.0.0";

  impl_->srv = std::make_unique<httplib::Server>();
  impl_->srv->Get("/metrics", [fn = std::move(text_fn)](const httplib::Request&, httplib::Response& res) {
    try {
      res.set_content(fn(), "text/plain; version=0.0.4; charset=utf-8");
    } catch (const std::exception& e) {
      res.status = 500;
      res.set_content(std::string("error: ") + e.what(), "text/plain");
    }
  });
  impl_->srv->Get("/healthz", [](const httplib::Request&, httplib::Response& res) {
    res.set_content("ok\n", "text/plain");
  });

  impl_->running = true;
  const std::string bind_host = host;
  const int bind_port = static_cast<int>(port);
  impl_->thr = std::thread([this, bind_host, bind_port] {
    SKV_INFO << "metrics http listen=" << bind_host << ":" << bind_port;
    if (!impl_->srv->listen(bind_host, bind_port)) {
      SKV_WARN << "metrics http listen failed";
    }
    impl_->running = false;
  });
}

void MetricsHttpServer::stop() {
  if (!impl_) return;
  if (impl_->srv) {
    impl_->srv->stop();
  }
  if (impl_->thr.joinable()) impl_->thr.join();
  impl_->srv.reset();
  impl_->running = false;
}

bool MetricsHttpServer::running() const { return impl_ && impl_->running.load(); }

std::string metrics_addr_from_env() {
  if (const char* e = std::getenv("SKV_METRICS_ADDR")) return std::string(e);
  return {};
}

}  // namespace opskv
