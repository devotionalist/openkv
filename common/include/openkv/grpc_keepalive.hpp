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

// @file grpc_keepalive.hpp
// 全仓统一半开连接探活。
//
// 注意：不要用 1s 级 keepalive。Storage 入站 AE 常在 gRPC 回调线程里同步 persist
// （fdatasync），卡住 > keepalive_timeout 会被对端当成死连接掐断，Raft 复制抖 →
// 写路径 apply_timeout（表象是 put p50 秒级）。
// 取 10s/3s：死人仍远快于 15s RPC deadline，又不误杀慢盘 IO。

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

namespace openkv {

/// 出站探活：10s 发 ping，3s 无响应判死。
inline constexpr int kGrpcKeepaliveTimeMs = 10000;
inline constexpr int kGrpcKeepaliveTimeoutMs = 3000;

/// 出站 Channel 参数：keepalive + 较短重连退避；可选消息大小上限（字节，≤0 不改）。
inline grpc::ChannelArguments grpc_client_channel_args(int max_message_bytes = 0) {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, kGrpcKeepaliveTimeMs);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, kGrpcKeepaliveTimeoutMs);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
  if (max_message_bytes > 0) {
    args.SetMaxReceiveMessageSize(max_message_bytes);
    args.SetMaxSendMessageSize(max_message_bytes);
  }
  return args;
}

/// 不安全出站 channel（带统一 keepalive）。
inline std::shared_ptr<grpc::Channel> grpc_insecure_channel(const std::string& addr,
                                                            int max_message_bytes = 0) {
  return grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(),
                                   grpc_client_channel_args(max_message_bytes));
}

/// 服务端：放行客户端无调用 ping；不主动对客户端狂 ping（避免慢 RPC 被误杀）。
inline void apply_server_keepalive(grpc::ServerBuilder* builder) {
  if (!builder) return;
  builder->AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  // 须小于客户端 keepalive time，否则客户端 10s ping 会被当攻击。
  builder->AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
  builder->AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
}

}  // namespace openkv
