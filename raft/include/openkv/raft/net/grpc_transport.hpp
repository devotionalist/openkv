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
// 职责：Raft **出站** gRPC（RaftService）——peer 地址表、发送队列、失败计数一体。
// 架构：register_node 绑定 shard→RaftNode；bind() 得到 TransportSend 闭包交给节点。
// 协作：入站 RPC 由 Storage/Meta 的 RaftService → Store::apply_inbound_raft，不经本类。
// 注意：出站 Unary / InstallSnapshot 使用 Callback API，避免阻塞 worker。

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "openkv/peer_counter.hpp"
#include "openkv/raft/core/raft_node.hpp"

namespace openkv::raft {

/* 出站走 gRPC；入站由 Storage/Meta 的 RaftService 处理。 */
class GrpcRaftTransport {
 public:
  /**
   * 构造未启动的传输；须 set_peer / register_node 后 start。
   */
  GrpcRaftTransport();
  ~GrpcRaftTransport();

  /**
   * 登记/更新 peer 的 gRPC 地址。
   * @param id peer id
   * @param addr host:port
   */
  void set_peer(const std::string& id, const std::string& addr);
  /**
   * 查 peer 地址；未知返回空串。
   * @param id peer id
   * @return 地址
   */
  std::string address_of(const std::string& id) const;

  /**
   * 把本机某 shard 的 RaftNode 挂到传输上（回包/失败回调用）。
   * @param shard_id 分片 id
   * @param self_id 本节点 peer id
   * @param node 协议节点
   */
  void register_node(uint32_t shard_id, const std::string& self_id, std::shared_ptr<RaftNode> node);
  /**
   * 注销 shard，不再向其回注入站应答。
   * @param shard_id 分片 id
   */
  void unregister_node(uint32_t shard_id);

  /**
   * 生成可交给 RaftNode / Store 的 TransportSend 闭包。
   * @param shard_id 发送侧分片 id（打进请求）
   * @return 发送回调
   */
  TransportSend bind(uint32_t shard_id);

  /**
   * 启动出站发送线程/队列。
   */
  void start();
  /**
   * 停止出站；排空或丢弃在途任务见实现。
   */
  void stop();

  /**
   * 出站 Raft RPC 计数编码（运维）。
   * @return 编码串
   */
  std::string encode_out_raft() const { return out_raft_.encode(); }
  /**
   * 出站同步类计数编码。
   * @return 编码串
   */
  std::string encode_out_sync() const { return out_sync_.encode(); }
  /**
   * 出站 Raft 按 peer 计数快照。
   * @return (peer, count) 列表
   */
  std::vector<std::pair<std::string, uint64_t>> snapshot_out_raft() const {
    return out_raft_.snapshot();
  }
  /**
   * 出站同步类按 peer 计数快照。
   * @return (peer, count) 列表
   */
  std::vector<std::pair<std::string, uint64_t>> snapshot_out_sync() const {
    return out_sync_.snapshot();
  }
  /**
   * 出站 Raft 总次数。
   * @return 累计
   */
  uint64_t out_raft_total() const;
  /**
   * 出站同步类总次数。
   * @return 累计
   */
  uint64_t out_sync_total() const;

  /**
   * 发送队列当前深度。
   * @return 待发任务数
   */
  size_t queue_depth() const;
  /**
   * 因队列满等原因丢弃的任务数。
   * @return 丢弃累计
   */
  uint64_t dropped_jobs() const;
  /**
   * 设置 InstallSnapshot 分片大小。
   * @param n 每片字节数
   */
  void set_snap_chunk_bytes(size_t n);

  /**
   * 本构建是否启用 gRPC 出站（恒 true；占位兼容）。
   * @return 总为 true
   */
  bool grpc_enabled() const { return true; }

 private:
  PeerCounter out_raft_;        // 出站 Raft RPC 计数
  PeerCounter out_sync_;        // 出站同步类计数
  struct Impl;                  // 隐藏 gRPC 实现
  std::unique_ptr<Impl> impl_;  // pimpl
};

}  // namespace openkv::raft
