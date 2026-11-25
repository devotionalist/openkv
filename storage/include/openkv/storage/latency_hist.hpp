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

namespace openkv::storage {

/* 固定桶延迟直方图（µs）；线程安全 observe / 读出。 */
class LatencyHistogram {
 public:
  static constexpr size_t kBucketCount = 12;  // 有限上界桶数；另有 +Inf
  // 桶上界（含），单位 µs；超出落入 +Inf。
  static constexpr std::array<uint64_t, kBucketCount> kBounds{
      100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 1000000};

  /**
   * 记一次延迟样本。
   * @param us 耗时（微秒）
   */
  void observe(uint64_t us);

  /**
   * 样本总数。
   * @return observe 次数
   */
  uint64_t count() const;
  /**
   * 延迟总和。
   * @return 累计 µs
   */
  uint64_t sum_us() const;
  /**
   * 平均延迟；无样本返回 0。
   * @return 平均 µs
   */
  uint64_t avg_us() const;
  /**
   * 近似分位（按桶累计）；无样本返回 0。
   * @param q 分位，如 0.5 / 0.99
   * @return 近似分位 µs
   */
  uint64_t quantile_us(double q) const;

  /**
   * 紧凑文本：count=.. avg_us=.. p50_us=.. p99_us=..
   * @return 单行摘要
   */
  std::string encode_compact() const;
  /**
   * Prometheus histogram 文本（含 _bucket / _sum / _count）。
   * @param metric 指标名，如 openkv_apply_wait_seconds
   * @param labels 已拼好的 label 串，如 region_id="1"
   * @return 多行 Prometheus 文本
   */
  std::string encode_prometheus(const std::string& metric, const std::string& labels) const;

 private:
  mutable std::mutex mu_;                             // 保护桶与累计
  std::array<uint64_t, kBucketCount + 1> buckets_{};  // 末桶 +Inf
  uint64_t count_{0};                                 // 样本数
  uint64_t sum_us_{0};                                // 累计 µs
};

/* 单 Region 的 propose / get 两路延迟。 */
struct RegionLatency {
  LatencyHistogram propose;  // propose→完成
  LatencyHistogram get;      // 读路径
};

/* 按 Region 聚合延迟；输出 compact / Prometheus。 */
class RegionMetrics {
 public:
  /**
   * 构造空表；首次 observe 时再建 Region 条目。
   */
  RegionMetrics();

  /**
   * 记一次 propose 延迟。
   * @param region_id Region id
   * @param us 耗时（微秒）
   */
  void observe_propose(uint32_t region_id, uint64_t us);
  /**
   * 记一次 get 延迟。
   * @param region_id Region id
   * @param us 耗时（微秒）
   */
  void observe_get(uint32_t region_id, uint64_t us);

  /**
   * 单 Region 紧凑摘要（propose+get）。
   * @param region_id Region id
   * @return 文本；无该 Region 时仍返回可解析空统计
   */
  std::string region_compact(uint32_t region_id) const;
  /**
   * 全部 Region 的 Prometheus 文本。
   * @return 多行指标
   */
  std::string prometheus_text() const;

 private:
  /**
   * libstdc++ moved-from / 堆损坏后 bucket_count==0 时 operator[]/find 会 SIGFPE。
   */
  void ensure_map_usable() const;
  /**
   * 取或创建某 Region 的延迟槽（已持锁语义由调用方保证）。
   * @param region_id Region id
   * @return 可写引用
   */
  RegionLatency& ensure(uint32_t region_id);

  mutable std::mutex mu_;                                          // 保护 by_region_
  mutable std::unordered_map<uint32_t, RegionLatency> by_region_;  // Region → 延迟
};

}  // namespace openkv::storage
