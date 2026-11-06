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

#include "opskv/protocol.hpp"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <string>

namespace opskv::storage {

enum class ReqClass : uint8_t { kClient = 0, kRaft = 1, kAdmin = 2, kBypass = 3 };

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

/// 按请求类限流：超限立即拒绝（不排队），保护 Raft 不被 Client 打满。
class InflightGate {
 public:
  InflightGate() {
    max_[static_cast<int>(ReqClass::kClient)] = env_or("SKV_MAX_INFLIGHT_CLIENT", 128);
    max_[static_cast<int>(ReqClass::kRaft)] = env_or("SKV_MAX_INFLIGHT_RAFT", 256);
    max_[static_cast<int>(ReqClass::kAdmin)] = env_or("SKV_MAX_INFLIGHT_ADMIN", 32);
  }

  void set_max(ReqClass c, uint32_t m) {
    if (c == ReqClass::kBypass) return;
    max_[static_cast<int>(c)].store(m == 0 ? 1 : m, std::memory_order_relaxed);
  }

  uint32_t max(ReqClass c) const {
    if (c == ReqClass::kBypass) return 0;
    return max_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  uint32_t inflight(ReqClass c) const {
    if (c == ReqClass::kBypass) return 0;
    return inflight_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  uint64_t rejected(ReqClass c) const {
    if (c == ReqClass::kBypass) return 0;
    return rejected_[static_cast<int>(c)].load(std::memory_order_relaxed);
  }

  bool try_enter(ReqClass c) {
    if (c == ReqClass::kBypass) return true;
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

  void leave(ReqClass c) {
    if (c == ReqClass::kBypass) return;
    inflight_[static_cast<int>(c)].fetch_sub(1, std::memory_order_acq_rel);
  }

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
  static uint32_t env_or(const char* name, uint32_t def) {
    if (const char* e = std::getenv(name)) {
      try {
        unsigned long v = std::stoul(e);
        if (v > 0 && v < 100000) return static_cast<uint32_t>(v);
      } catch (...) {
      }
    }
    return def;
  }

  std::atomic<uint32_t> max_[3]{};
  std::atomic<uint32_t> inflight_[3]{};
  std::atomic<uint64_t> rejected_[3]{};
};

struct InflightGuard {
  InflightGate* gate{nullptr};
  ReqClass cls{ReqClass::kBypass};
  bool held{false};

  InflightGuard(InflightGate& g, ReqClass c) : gate(&g), cls(c) {
    held = gate->try_enter(cls);
  }
  ~InflightGuard() {
    if (held && gate) gate->leave(cls);
  }
  InflightGuard(const InflightGuard&) = delete;
  InflightGuard& operator=(const InflightGuard&) = delete;
  explicit operator bool() const { return held; }
};

inline ReqClass classify_op(const std::string& op) {
  namespace proto = opskv::proto;
  if (op == proto::kOpPing || op == proto::kOpStats || op == proto::kOpMetrics) {
    return ReqClass::kBypass;
  }
  if (op.rfind(proto::kRaftPrefix, 0) == 0) return ReqClass::kRaft;
  if (op.rfind(proto::kAdminPrefix, 0) == 0) return ReqClass::kAdmin;
  return ReqClass::kClient;
}

}  // namespace opskv::storage
