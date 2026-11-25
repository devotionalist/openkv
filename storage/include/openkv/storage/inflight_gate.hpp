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

// @file inflight_gate.hpp
// O4：Client / Raft / Admin 入站并发隔离（分通道 inflight 上限）。

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "openkv/protocol.hpp"

namespace openkv::storage {

/* 入站请求通道：Client / Raft / Admin；Bypass 不计入限流。 */
enum class ReqClass : uint8_t {
  kClient = 0,  // 用户读写
  kRaft = 1,    // Raft RPC
  kAdmin = 2,   // Admin / 管控
  kBypass = 3,  // Ping/Stats 等，不限流
};

/**
 * 通道名（日志 / 指标用）。
 * @param c 请求类
 * @return 短字符串，如 "client"
 */
inline const char* req_class_name(ReqClass c) {
  switch (c) {
    case ReqClass::kClient:
      return "client";
    case ReqClass::kRaft:
      return "raft";
    case ReqClass::kAdmin:
      return "admin";
    case ReqClass::kBypass:
      return "bypass";
  }
  return "unknown";
}

/* 按请求类限流：超限立即拒绝（不排队），保护 Raft 不被 Client 打满。 */
class InflightGate {
 public:
  /**
   * 从环境变量读各通道上限（SKV_MAX_INFLIGHT_*）；缺省 Client=128/Raft=256/Admin=32。
   */
  InflightGate() {
    max_[static_cast<int>(ReqClass::kClient)] = env_or("SKV_MAX_INFLIGHT_CLIENT", 128);
    max_[static_cast<int>(ReqClass::kRaft)] = env_or("SKV_MAX_INFLIGHT_RAFT", 256);
    max_[static_cast<int>(ReqClass::kAdmin)] = env_or("SKV_MAX_INFLIGHT_ADMIN", 32);
  }

  /**
   * 运行时改某通道上限；0 会被夹成 1；Bypass 忽略。
   * @param c 请求类
   * @param m 新上限
   */
  void set_max(ReqClass c, uint32_t m) {
    if (c == ReqClass::kBypass) {
      return;
    }
    max_[static_cast<int>(c)].store(m == 0 ? 1 : m, std::memory_order_relaxed);
  }

  /**
   * 某通道当前上限。
   * @param c 请求类
   * @return 上限；Bypass 恒为 0
   */
  uint32_t max(ReqClass c) const {
    if (c == ReqClass::kBypass) {
      return 0;
    }
    return max_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  /**
   * 某通道当前在途数。
   * @param c 请求类
   * @return 在途计数；Bypass 恒为 0
   */
  uint32_t inflight(ReqClass c) const {
    if (c == ReqClass::kBypass) {
      return 0;
    }
    return inflight_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  /**
   * 某通道累计拒绝次数。
   * @param c 请求类
   * @return 拒绝计数；Bypass 恒为 0
   */
  uint64_t rejected(ReqClass c) const {
    if (c == ReqClass::kBypass) {
      return 0;
    }
    return rejected_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  /**
   * 尝试进入通道；成功则 inflight+1，失败则 rejected+1。
   * @param c 请求类；Bypass 恒成功
   * @return true=占用成功，false=已满应立即拒绝
   */
  bool try_enter(ReqClass c) {
    if (c == ReqClass::kBypass) {
      return true;
    }
    const int i = static_cast<int>(c);
    const uint32_t cap = max_[i].load(std::memory_order_relaxed);
    uint32_t cur = inflight_[i].load(std::memory_order_relaxed);
    while (cur < cap) {
      if (inflight_[i].compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel)) {
        return true;
      }
    }
    rejected_[i].fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  /**
   * 离开通道（inflight-1）；须与成功的 try_enter 配对。
   * @param c 请求类；Bypass 无操作
   */
  void leave(ReqClass c) {
    if (c == ReqClass::kBypass) {
      return;
    }
    inflight_[static_cast<int>(c)].fetch_sub(1, std::memory_order_acq_rel);
  }

  /**
   * 编码当前 inflight / reject / max 文本（STATS 用）。
   * @return 空格分隔的 key=value 串
   */
  std::string encode_stats() const {
    return std::string("inflight_client=") + std::to_string(inflight(ReqClass::kClient)) +
           " inflight_raft=" + std::to_string(inflight(ReqClass::kRaft)) +
           " inflight_admin=" + std::to_string(inflight(ReqClass::kAdmin)) +
           " reject_client=" + std::to_string(rejected(ReqClass::kClient)) +
           " reject_raft=" + std::to_string(rejected(ReqClass::kRaft)) +
           " reject_admin=" + std::to_string(rejected(ReqClass::kAdmin)) +
           " max_client=" + std::to_string(max(ReqClass::kClient)) +
           " max_raft=" + std::to_string(max(ReqClass::kRaft)) +
           " max_admin=" + std::to_string(max(ReqClass::kAdmin));
  }

 private:
  /**
   * 读环境变量无符号整数；非法或不在 (0,100000) 则用默认。
   * @param name 环境变量名
   * @param def 缺省值
   * @return 解析后的上限
   */
  static uint32_t env_or(const char* name, uint32_t def) {
    if (const char* e = std::getenv(name)) {
      try {
        unsigned long v = std::stoul(e);
        if (v > 0 && v < 100000) {
          return static_cast<uint32_t>(v);
        }
      } catch (...) {
      }
    }
    return def;
  }

  std::atomic<uint32_t> max_[3]{};       // 各通道上限（Client/Raft/Admin）
  std::atomic<uint32_t> inflight_[3]{};  // 各通道当前在途
  std::atomic<uint64_t> rejected_[3]{};  // 各通道累计拒绝
};

/* RAII：构造 try_enter，析构 leave；bool 转换看是否拿到名额。 */
struct InflightGuard {
  InflightGate* gate{nullptr};      // 所属门；可空
  ReqClass cls{ReqClass::kBypass};  // 占用的通道
  bool held{false};                 // 是否成功占用

  /**
   * 尝试进入；失败时 held=false，请求应立即拒绝。
   * @param g 限流门
   * @param c 请求类
   */
  InflightGuard(InflightGate& g, ReqClass c) : gate(&g), cls(c) { held = gate->try_enter(cls); }
  /**
   * 若 held 则 leave。
   */
  ~InflightGuard() {
    if (held && gate) {
      gate->leave(cls);
    }
  }
  InflightGuard(const InflightGuard&) = delete;
  InflightGuard& operator=(const InflightGuard&) = delete;
  /**
   * 是否占用成功。
   * @return held
   */
  explicit operator bool() const { return held; }
};

/**
 * 按操作名归类到 Client / Raft / Admin / Bypass。
 * @param op 协议操作名（如 PUT、RAFT_AE、ADMIN_SPLIT）
 * @return 对应 ReqClass
 */
inline ReqClass classify_op(const std::string& op) {
  namespace proto = openkv::proto;
  if (op == proto::kOpPing || op == proto::kOpStats || op == proto::kOpMetrics) {
    return ReqClass::kBypass;
  }
  if (op.rfind(proto::kRaftPrefix, 0) == 0) {
    return ReqClass::kRaft;
  }
  if (op.rfind(proto::kAdminPrefix, 0) == 0) {
    return ReqClass::kAdmin;
  }
  return ReqClass::kClient;
}

}  // namespace openkv::storage
