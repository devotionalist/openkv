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

#include "opskv/storage/latency_hist.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>

namespace opskv::storage {

void LatencyHistogram::observe(uint64_t us) {
  size_t i = 0;
  for (; i < kBucketCount; ++i) {
    if (us <= kBounds[i]) break;
  }
  std::lock_guard<std::mutex> lock(mu_);
  buckets_[i] += 1;
  ++count_;
  sum_us_ += us;
}

uint64_t LatencyHistogram::count() const {
  std::lock_guard<std::mutex> lock(mu_);
  return count_;
}

uint64_t LatencyHistogram::sum_us() const {
  std::lock_guard<std::mutex> lock(mu_);
  return sum_us_;
}

uint64_t LatencyHistogram::avg_us() const {
  std::lock_guard<std::mutex> lock(mu_);
  return count_ == 0 ? 0 : sum_us_ / count_;
}

uint64_t LatencyHistogram::quantile_us(double q) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (count_ == 0 || q <= 0) return 0;
  if (q >= 1.0) {
    return buckets_[kBucketCount] > 0 ? 5'000'000ULL : kBounds[kBucketCount - 1];
  }
  const double target = static_cast<double>(count_) * q;
  uint64_t acc = 0;
  for (size_t i = 0; i < kBucketCount; ++i) {
    acc += buckets_[i];
    if (static_cast<double>(acc) >= target) return kBounds[i];
  }
  return kBounds[kBucketCount - 1];
}

std::string LatencyHistogram::encode_compact() const {
  std::ostringstream oss;
  oss << "count=" << count() << " avg_us=" << avg_us() << " p50_us=" << quantile_us(0.50)
      << " p99_us=" << quantile_us(0.99);
  return oss.str();
}

std::string LatencyHistogram::encode_prometheus(const std::string& metric,
                                                const std::string& labels) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::ostringstream oss;
  uint64_t cum = 0;
  for (size_t i = 0; i < kBucketCount; ++i) {
    cum += buckets_[i];
    oss << metric << "_bucket{" << labels << ",le=\"" << kBounds[i] << "\"} " << cum << '\n';
  }
  cum += buckets_[kBucketCount];
  oss << metric << "_bucket{" << labels << ",le=\"+Inf\"} " << cum << '\n';
  oss << metric << "_sum{" << labels << "} " << sum_us_ << '\n';
  oss << metric << "_count{" << labels << "} " << count_ << '\n';
  return oss.str();
}

RegionLatency& RegionMetrics::ensure(uint32_t region_id) {
  return by_region_[region_id];
}

void RegionMetrics::observe_propose(uint32_t region_id, uint64_t us) {
  std::lock_guard<std::mutex> lock(mu_);
  ensure(region_id).propose.observe(us);
}

void RegionMetrics::observe_get(uint32_t region_id, uint64_t us) {
  std::lock_guard<std::mutex> lock(mu_);
  ensure(region_id).get.observe(us);
}

std::string RegionMetrics::region_compact(uint32_t region_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = by_region_.find(region_id);
  if (it == by_region_.end()) {
    return "propose_count=0 get_count=0";
  }
  return "propose_" + it->second.propose.encode_compact() + " get_" +
         it->second.get.encode_compact();
}

std::string RegionMetrics::prometheus_text() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::ostringstream oss;
  oss << "# HELP skv_propose_latency_us Propose+apply latency in microseconds\n";
  oss << "# TYPE skv_propose_latency_us histogram\n";
  oss << "# HELP skv_get_latency_us Get latency in microseconds\n";
  oss << "# TYPE skv_get_latency_us histogram\n";
  for (const auto& [rid, lat] : by_region_) {
    const std::string labels = "region=\"" + std::to_string(rid) + "\"";
    oss << lat.propose.encode_prometheus("skv_propose_latency_us", labels);
    oss << lat.get.encode_prometheus("skv_get_latency_us", labels);
  }
  return oss.str();
}

}  // namespace opskv::storage
