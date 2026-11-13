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

// @file grpc_shutdown.hpp
// 进程级优雅关机：拒新 RPC + 宽限内排空在途（gRPC Server::Shutdown(deadline)+Wait）。

#pragma once

#include "openkv/logging.hpp"
#include "openkv/protocol.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>

namespace openkv {

/// 读 SKV_GRPC_SHUTDOWN_MS；非法或未设时用 default_ms（默认 15000）。
inline int grpc_shutdown_grace_ms(int default_ms = 15000) {
  if (const char* e = std::getenv(proto::env::kGrpcShutdownMs)) {
    char* end = nullptr;
    const long v = std::strtol(e, &end, 10);
    if (end != e && v >= 0 && v <= 600000) return static_cast<int>(v);
  }
  return default_ms < 0 ? 0 : default_ms;
}

/// 拒接新请求；等到在途结束或 deadline；再 Wait 收尾。server 可为空。
inline void graceful_grpc_shutdown(grpc::Server* server, int grace_ms = -1) {
  if (!server) return;
  if (grace_ms < 0) grace_ms = grpc_shutdown_grace_ms();
  SKV_INFO << "grpc graceful shutdown grace_ms=" << grace_ms;
  if (grace_ms == 0) {
    server->Shutdown();
  } else {
    const auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(grace_ms);
    server->Shutdown(deadline);
  }
  server->Wait();
}

inline void graceful_grpc_shutdown(const std::unique_ptr<grpc::Server>& server, int grace_ms = -1) {
  graceful_grpc_shutdown(server.get(), grace_ms);
}

}  // namespace openkv
