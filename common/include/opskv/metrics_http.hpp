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

// @file metrics_http.hpp
// 可选 HTTP /metrics：暴露已有 Prometheus 文本（cpp-httplib）。
// 环境变量 SKV_METRICS_ADDR=host:port（如 0.0.0.0:9101）；空则不起服务。

#pragma once

#include <functional>
#include <memory>
#include <string>

namespace opskv {

/// 返回完整 Prometheus 文本（可含多行；无需 END 尾标）。
using MetricsTextFn = std::function<std::string()>;

class MetricsHttpServer {
 public:
  MetricsHttpServer();
  ~MetricsHttpServer();

  MetricsHttpServer(const MetricsHttpServer&) = delete;
  MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;

  /// @param listen_addr host:port；空字符串则 no-op
  /// @param text_fn 每次 GET /metrics 调用
  void start(std::string listen_addr, MetricsTextFn text_fn);
  void stop();
  bool running() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// 从 SKV_METRICS_ADDR 读取；未设置返回空。
std::string metrics_addr_from_env();

}  // namespace opskv
