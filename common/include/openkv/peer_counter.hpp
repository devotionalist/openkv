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

namespace openkv {

/* 按 peer 字符串累计计数；供 STATS / METRICS 按对端拆开。 */
class PeerCounter {
 public:
  /**
   * 给 peer 累加 n；peer 空或 n==0 则忽略。
   * @param peer 对端标识（node_id 或地址）
   * @param n 增量，默认 1
   */
  void add(const std::string& peer, uint64_t n = 1) {
    if (peer.empty() || n == 0) {
      return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    m_[peer] += n;
  }

  /**
   * 查询某 peer 当前累计值；未知返回 0。
   * @param peer 对端标识
   * @return 累计次数
   */
  uint64_t get(const std::string& peer) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = m_.find(peer);
    return it == m_.end() ? 0 : it->second;
  }

  /**
   * 拷贝整表快照（无序）。
   * @return (peer, count) 列表
   */
  std::vector<std::pair<std::string, uint64_t>> snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    return {m_.begin(), m_.end()};
  }

  /**
   * 编码为 a:1,b:2（供 STATS 行协议）。
   * @return 逗号分隔的 peer:count
   */
  std::string encode() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::ostringstream oss;
    bool first = true;
    for (const auto& kv : m_) {
      if (!first) {
        oss << ',';
      }
      first = false;
      oss << kv.first << ':' << kv.second;
    }
    return oss.str();
  }

 private:
  mutable std::mutex mu_;                        // 保护 m_
  std::unordered_map<std::string, uint64_t> m_;  // peer → 累计
};

}  // namespace openkv
