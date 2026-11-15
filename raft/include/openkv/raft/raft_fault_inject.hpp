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

// @file raft_fault_inject.hpp
// 出站 Raft RPC 网络抖动注入（演练用，默认全关）。
//
// 环境变量（进程启动时读一次）：
//   SKV_FAULT_DROP_AE_PCT / SKV_FAULT_DELAY_AE_MS     — 只动 AppendEntries
//   SKV_FAULT_DROP_RAFT_PCT / SKV_FAULT_DELAY_RAFT_MS — 动全部出站 Raft
// 丢包时调用方应走 on_rpc_transport_fail，模拟链路不可达。

#pragma once

#include <cstdint>
#include <string_view>

namespace openkv::raft {

struct RaftOutboundFault {
  int drop_ae_pct{0};     ///< 0–100：AppendEntries 丢弃概率
  int delay_ae_ms{0};     ///< AppendEntries 发送前额外睡眠
  int drop_raft_pct{0};   ///< 0–100：任意出站 Raft 丢弃概率
  int delay_raft_ms{0};   ///< 任意出站 Raft 发送前额外睡眠

  static RaftOutboundFault from_env();

  bool enabled() const {
    return drop_ae_pct > 0 || delay_ae_ms > 0 || drop_raft_pct > 0 || delay_raft_ms > 0;
  }

  /// rand01_99 ∈ [0,99]；命中则本条应丢弃。
  bool should_drop(std::string_view type, int rand01_99) const;

  /// 本条发送前应 sleep 的毫秒数（AE 与全量延迟取较大者）。
  int delay_ms_for(std::string_view type) const;
};

}  // namespace openkv::raft
