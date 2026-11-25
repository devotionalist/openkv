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

namespace openkv {

// 返回完整 Prometheus 文本（可含多行；无需 END 尾标）。
using MetricsTextFn = std::function<std::string()>;

/* 可选 HTTP /metrics 小服务；start 后后台线程监听。 */
class MetricsHttpServer {
 public:
  MetricsHttpServer();
  ~MetricsHttpServer();

  MetricsHttpServer(const MetricsHttpServer&) = delete;
  MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;

  /**
   * 在 listen_addr 起 HTTP，GET /metrics 调 text_fn。
   * @param listen_addr 监听地址 host:port
   * @param text_fn 返回 Prometheus 文本的回调
   */
  void start(std::string listen_addr, MetricsTextFn text_fn);
  /**
   * 停止监听并 join 后台线程；幂等。
   */
  void stop();
  /**
   * 是否正在监听。
   * @return true 表示已 start 且未 stop
   */
  bool running() const;

 private:
  struct Impl;                  // 实现细节（httplib 等）
  std::unique_ptr<Impl> impl_;  // Pimpl
};

/**
 * 从 SKV_METRICS_ADDR 读取；未设置返回空。
 * @return 监听地址或空串
 */
std::string metrics_addr_from_env();

}  // namespace openkv
