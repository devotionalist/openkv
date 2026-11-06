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

// @file peer_counter.hpp
// 线程安全的按对端累计计数（拓扑节点间流量）。
#pragma once

#include <cstdint>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace opskv {

class PeerCounter {
 public:
  void add(const std::string& peer, uint64_t n = 1) {
    if (peer.empty() || n == 0) return;
    std::lock_guard<std::mutex> lk(mu_);
    m_[peer] += n;
  }

  uint64_t get(const std::string& peer) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = m_.find(peer);
    return it == m_.end() ? 0 : it->second;
  }

  std::vector<std::pair<std::string, uint64_t>> snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    return {m_.begin(), m_.end()};
  }

  /// 编码为 a:1,b:2（供 STATS 行协议）。
  std::string encode() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::ostringstream oss;
    bool first = true;
    for (const auto& kv : m_) {
      if (!first) oss << ',';
      first = false;
      oss << kv.first << ':' << kv.second;
    }
    return oss.str();
  }

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, uint64_t> m_;
};

}  // namespace opskv
