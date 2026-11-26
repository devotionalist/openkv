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

// @file ready_pump.hpp
// Ready 外提 + 出站消息攒批。
// enqueue：调用方已持 mu_。
// has_ready / take_ready / flush_transport / advance：自己抢 mu_（Store / 嵌入发网入口）。

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "openkv/raft/core/ready.hpp"

namespace openkv::raft {

class RaftNode;

/* 管「协议产出怎么交给 Store：Ready 缓冲 + 出站消息 + 唤醒」。 */
class ReadyPump {
 public:
  /**
   * 绑定所属 RaftNode。
   * @param node 宿主节点
   */
  explicit ReadyPump(RaftNode& node) : n_(node) {}

  /**
   * 攒一条出站消息；external_io 时留给 take_ready，否则 flush 时真发。须已持 mu_。
   * @param peer_id 目标 peer
   * @param type RPC 种类
   * @param payload 结构体载荷（Vote / AE / Snap）
   */
  void enqueue(std::string peer_id, MsgType type, RaftReq payload);
  /**
   * 嵌入路径：把 outbound_ 经 TransportSend 发出；external_io 直接空操作。自己抢 mu_。
   */
  void flush_transport();

  /**
   * 是否有待外提 Ready（含出站消息）。
   * @return true 表示可 take
   */
  bool has_ready() const;
  /**
   * 组装并取出一批 Ready，清空内部缓冲。
   * @return Ready 批次
   */
  Ready take_ready();
  /**
   * Store 处理完 Ready 后推进内部 applied/游标。
   * @param rd 刚处理完的批次
   */
  void advance(const Ready& rd);
  /**
   * 注册 Ready 有货时的唤醒回调。
   * @param cb 无参回调（通常 Store::request_ready）
   */
  void set_ready_notify(std::function<void()> cb);
  /**
   * 回包线程：Ready 有货就立刻唤醒 Store，别干等 tick。勿持 mu_ 调用。
   */
  void maybe_notify_ready();

  /**
   * 组装 SoftState 公共字段（不含 replication_lag）。须已持 mu_。
   * @return SoftState 快照
   */
  SoftState soft_state_locked() const;

  /* 一条待发出站 RPC（与 Message 同形，peer_id ≡ Message::to）。 */
  struct Outbound {
    std::string peer_id;                          // 目标 peer
    MsgType type{MsgType::kAppendEntries};        // RPC 种类
    RaftReq payload;                              // 结构体载荷
  };

  std::vector<Outbound> outbound_;      // 待发送消息队列
  std::function<void()> ready_notify_;  // Ready 有货时唤醒 Store

 private:
  RaftNode& n_;  // 宿主节点
};

}  // namespace openkv::raft
