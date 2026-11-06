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

// @file latency_hist.hpp
// O2：固定桶延迟直方图（微秒），按 Region 聚合 propose / get。

#pragma once

#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace opskv::storage {

class LatencyHistogram {
 public:
  static constexpr size_t kBucketCount = 12;
  /// 桶上界（含），单位 µs；超出落入 +Inf。
  static constexpr std::array<uint64_t, kBucketCount> kBounds{
      100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 1000000};

  void observe(uint64_t us);

  uint64_t count() const;
  uint64_t sum_us() const;
  uint64_t avg_us() const;
  /// 近似分位（按桶累计）；无样本返回 0。
  uint64_t quantile_us(double q) const;

  /// count=.. avg_us=.. p50_us=.. p99_us=..
  std::string encode_compact() const;
  /// Prometheus histogram 文本（含 _bucket / _sum / _count）。
  std::string encode_prometheus(const std::string& metric, const std::string& labels) const;

 private:
  mutable std::mutex mu_;
  std::array<uint64_t, kBucketCount + 1> buckets_{};  // 末桶 +Inf
  uint64_t count_{0};
  uint64_t sum_us_{0};
};

struct RegionLatency {
  LatencyHistogram propose;
  LatencyHistogram get;
};

class RegionMetrics {
 public:
  void observe_propose(uint32_t region_id, uint64_t us);
  void observe_get(uint32_t region_id, uint64_t us);

  std::string region_compact(uint32_t region_id) const;
  std::string prometheus_text() const;

 private:
  RegionLatency& ensure(uint32_t region_id);

  mutable std::mutex mu_;
  std::unordered_map<uint32_t, RegionLatency> by_region_;
};

}  // namespace opskv::storage
