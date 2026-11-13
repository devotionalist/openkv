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

// @file ts_range_queue.hpp
// 无锁 TSO 区间环：多消费者 CAS 取号，多生产者 push 整段 [start,end)。

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace openkv::compute {

/// 固定容量无锁区间队列（MPMC）。
/// 槽位协议：end==0 空闲；发布时先写 next 再 release 写 end；退役时 end→0。
template <std::size_t Cap = 64>
class TsRangeQueue {
  static_assert(Cap >= 2 && (Cap & (Cap - 1)) == 0, "Cap must be power of two >= 2");

 public:
  static constexpr std::size_t capacity = Cap;

  /// 队尾追加 [start, start+count)；满则 false（调用方应加大 Cap / 降预取频率）。
  bool push(uint64_t start, uint32_t count) {
    if (count == 0) return true;
    const uint64_t new_end = start + static_cast<uint64_t>(count);
    for (;;) {
      uint64_t t = tail_.load(std::memory_order_relaxed);
      const uint64_t h = head_.load(std::memory_order_acquire);
      if (t - h >= Cap) return false;
      if (!tail_.compare_exchange_weak(t, t + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        continue;
      }
      Slot& s = slots_[t & (Cap - 1)];
      // 等待槽位退役（上一圈消费者已清空 end）。
      while (s.end.load(std::memory_order_acquire) != 0) {
        // 极短自旋：容量与水位保证 practically 不会卡住。
      }
      s.next.store(start, std::memory_order_relaxed);
      s.end.store(new_end, std::memory_order_release);
      cached_.fetch_add(count, std::memory_order_relaxed);
      return true;
    }
  }

  /// 从队头取连续 n 个；成功写出 *out_start。碎片不足 n 则丢弃并前进。
  bool try_take(uint32_t n, uint64_t* out_start) {
    if (n == 0) {
      if (out_start) *out_start = 0;
      return true;
    }
    for (int spins = 0; spins < 1024; ++spins) {
      const uint64_t h = head_.load(std::memory_order_acquire);
      const uint64_t t = tail_.load(std::memory_order_acquire);
      if (h == t) return false;

      Slot& s = slots_[h & (Cap - 1)];
      const uint64_t end = s.end.load(std::memory_order_acquire);
      if (end == 0) {
        // 生产者尚未发布，或槽已退役但 head 未推进。
        if (h + 1 <= t) {
          uint64_t expect = h;
          head_.compare_exchange_weak(expect, h + 1, std::memory_order_acq_rel, std::memory_order_relaxed);
        }
        continue;
      }

      uint64_t cur = s.next.load(std::memory_order_relaxed);
      if (cur >= end) {
        retire_head(h, s);
        continue;
      }
      if (cur + static_cast<uint64_t>(n) > end) {
        // 丢弃不足以满足 n 的队头碎片。
        if (s.next.compare_exchange_weak(cur, end, std::memory_order_acq_rel, std::memory_order_relaxed)) {
          cached_.fetch_sub(end - cur, std::memory_order_relaxed);
        }
        continue;
      }
      if (s.next.compare_exchange_weak(cur, cur + n, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        cached_.fetch_sub(n, std::memory_order_relaxed);
        if (out_start) *out_start = cur;
        if (cur + n >= end) retire_head(h, s);
        return true;
      }
    }
    return false;
  }

  uint64_t remaining() const { return cached_.load(std::memory_order_relaxed); }

  /// 单测重置；不得与并发 take/push 重叠。
  void reset_for_test() {
    head_.store(0, std::memory_order_relaxed);
    tail_.store(0, std::memory_order_relaxed);
    cached_.store(0, std::memory_order_relaxed);
    for (std::size_t i = 0; i < Cap; ++i) {
      slots_[i].next.store(0, std::memory_order_relaxed);
      slots_[i].end.store(0, std::memory_order_relaxed);
    }
  }

 private:
  struct alignas(64) Slot {
    std::atomic<uint64_t> next{0};
    std::atomic<uint64_t> end{0};
  };

  void retire_head(uint64_t h, Slot& s) {
    uint64_t expect = h;
    if (head_.compare_exchange_strong(expect, h + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
      s.end.store(0, std::memory_order_release);
      s.next.store(0, std::memory_order_relaxed);
    }
  }

  Slot slots_[Cap]{};
  alignas(64) std::atomic<uint64_t> head_{0};
  alignas(64) std::atomic<uint64_t> tail_{0};
  alignas(64) std::atomic<uint64_t> cached_{0};
};

}  // namespace openkv::compute
