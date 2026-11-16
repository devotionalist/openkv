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

#include "openkv/raft/core/ready.hpp"

#include <functional>
#include <string>
#include <vector>

namespace openkv::raft {

class RaftNode;

/// 管「协议产出怎么交给 Store：Ready 缓冲 + 出站消息 + 唤醒」。
class ReadyPump {
 public:
  explicit ReadyPump(RaftNode& node) : n_(node) {}

  /// 攒一条出站消息；external_io 时留给 take_ready，否则 flush 时真发。须已持 mu_。
  void enqueue(std::string peer_id, std::string type, std::string payload);
  /// 嵌入路径：把 outbound_ 经 TransportSend 发出；external_io 直接空操作。自己抢 mu_。
  void flush_transport();

  bool has_ready() const;
  Ready take_ready();
  void advance(const Ready& rd);
  void set_ready_notify(std::function<void()> cb);
  /// 回包线程：Ready 有货就立刻唤醒 Store，别干等 tick。勿持 mu_ 调用。
  void maybe_notify_ready();

  /// 组装 SoftState 公共字段（不含 replication_lag）。须已持 mu_。
  SoftState soft_state_locked() const;

  struct Outbound {
    std::string peer_id;
    std::string type;
    std::string payload;
  };

  std::vector<Outbound> outbound_;
  std::function<void()> ready_notify_;

 private:
  RaftNode& n_;
};

}  // namespace openkv::raft
