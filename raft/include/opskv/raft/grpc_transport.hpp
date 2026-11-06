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

// @file grpc_transport.hpp
// Raft 出站：RaftService gRPC（peer 表 / 队列 / 计数一体）。

#pragma once

#include "opskv/peer_counter.hpp"
#include "opskv/raft/raft_node.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace opskv::raft {

/// 出站走 gRPC；入站由 Storage/Meta 的 RaftService 处理。
class GrpcRaftTransport {
 public:
  GrpcRaftTransport();
  ~GrpcRaftTransport();

  void set_peer(const std::string& id, const std::string& addr);
  std::string address_of(const std::string& id) const;

  void register_node(uint32_t shard_id, const std::string& self_id, std::shared_ptr<RaftNode> node);
  void unregister_node(uint32_t shard_id);

  TransportSend bind(uint32_t shard_id);

  void start();
  void stop();

  std::string encode_out_raft() const { return out_raft_.encode(); }
  std::string encode_out_sync() const { return out_sync_.encode(); }
  uint64_t out_raft_total() const;
  uint64_t out_sync_total() const;

  size_t queue_depth() const;
  uint64_t dropped_jobs() const;
  void set_snap_chunk_bytes(size_t n);

  bool grpc_enabled() const { return true; }

 private:
  PeerCounter out_raft_;
  PeerCounter out_sync_;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace opskv::raft
