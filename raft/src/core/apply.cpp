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

// @file apply.cpp
// apply_committed / compact / 本地 FSM 快照读写。

#include "openkv/raft/core/apply.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string_view>

#include "openkv/logging.hpp"
#include "openkv/raft/core/raft_node.hpp"
#include "openkv/raft/engine/raft_wire.hpp"

namespace openkv::raft {
namespace {

bool is_conf_entry(const LogEntry& e) {
  return !e.data.empty() && static_cast<unsigned char>(e.data[0]) == 0xFF;
}

/* 包住 on_apply：进回调前抬 in_on_apply_，出来（含抛异常）再放下，挡住重入 recover_apply。 */
struct InOnApplyGuard {
  std::atomic<bool>& flag;
  explicit InOnApplyGuard(std::atomic<bool>& f) : flag(f) {
    flag.store(true, std::memory_order_relaxed);
  }
  ~InOnApplyGuard() { flag.store(false, std::memory_order_relaxed); }
  InOnApplyGuard(const InOnApplyGuard&) = delete;
  InOnApplyGuard& operator=(const InOnApplyGuard&) = delete;
};

}  // namespace

void Apply::invoke_on_apply(uint64_t idx, const std::string& data) {
  if (!n_.on_apply_) {
    return;
  }
  InOnApplyGuard guard(n_.in_on_apply_);
  n_.on_apply_(idx, data);
}

uint64_t Apply::term_at_applied() const {
  if (auto e = n_.log_.entry(n_.last_applied_)) {
    return e->term;
  }
  return n_.log_.snapshot_term();
}

// 把 commit_index 之前还没调度的条目推进到 apply（compact 掉的只抬水位）。
void Apply::apply_committed() {
  while (n_.apply_scheduled_ < n_.commit_index_) {
    const uint64_t idx = n_.apply_scheduled_ + 1;

    // 日志已被 compact 掉：只抬水位，不再回调。
    if (idx < n_.log_.first_index()) {
      n_.apply_scheduled_ = idx;
      if (idx > n_.last_applied_) {
        n_.last_applied_ = idx;
      }
      continue;
    }

    auto e = n_.log_.entry(idx);
    if (!e) {
      n_.apply_scheduled_ = idx;
      continue;
    }

    const bool is_conf = is_conf_entry(*e);

    if (n_.opts_.external_io) {
      // 外提路径：普通条目塞进 Ready；ConfChange 仍在协议线程立刻改成员表。
      if (is_conf) {
        n_.membership_.apply_conf_entry(*e);
      }
      n_.ready_io_.ready_committed_.push_back(*e);
      n_.apply_scheduled_ = idx;
      if (is_conf) {
        n_.last_applied_ = idx;
      }
      continue;
    }

    if (is_conf) {
      n_.membership_.apply_conf_entry(*e);
    } else {
      invoke_on_apply(idx, e->data);
    }
    n_.apply_scheduled_ = idx;
    n_.last_applied_ = idx;
    // 本地 FSM 快照：与 compact 阈值同量级，避免每 32 条就在 Raft 锁内写盘。
    const uint64_t snap_every =
        n_.opts_.compact_threshold == 0 ? 256 : std::max<uint64_t>(n_.opts_.compact_threshold, 256);
    if (n_.snap_provider_ && (idx % snap_every == 0)) {
      save_local_fsm_snapshot_locked();
    }
  }

  if (!n_.opts_.external_io) {
    maybe_compact();
  }
}

std::string Apply::local_fsm_snap_path() const {
  if (n_.opts_.persist_dir.empty()) {
    return {};
  }
  return (std::filesystem::path(n_.opts_.persist_dir) /
          ("fsm_" + std::to_string(n_.group_id_) + ".snap"))
      .string();
}

void Apply::save_local_fsm_snapshot_locked() {
  if (!n_.snap_provider_) {
    return;
  }
  const auto path = local_fsm_snap_path();
  if (path.empty()) {
    return;
  }
  uint64_t li = n_.last_applied_;
  uint64_t lt = term_at_applied();
  std::string blob = n_.snap_provider_(&li, &lt);
  try {
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    const auto tmp = path + ".tmp";
    {
      std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
      if (!out) {
        SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap open failed path=" << tmp;
        return;
      }
      out.write(kMagicFsmSnap.data(), static_cast<std::streamsize>(kMagicFsmSnap.size()));
      out.write(reinterpret_cast<const char*>(&li), sizeof(li));
      out.write(reinterpret_cast<const char*>(&lt), sizeof(lt));
      out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
      out.flush();
      if (!out) {
        SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap write failed";
        return;
      }
    }
    std::filesystem::rename(tmp, path);
    SKV_INFO << "shard=" << n_.opts_.shard_id << " fsm snap saved idx=" << li << " term=" << lt
             << " bytes=" << blob.size();
  } catch (const std::exception& e) {
    SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap save: " << e.what();
  }
}

bool Apply::try_restore_fsm_snap_file() {
  const auto path = local_fsm_snap_path();
  if (!n_.snap_applier_ || path.empty() || !std::filesystem::exists(path)) {
    return false;
  }
  try {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
      return false;
    }
    char magic[4] = {};
    uint64_t li = 0, lt = 0;
    in.read(magic, static_cast<std::streamsize>(kMagicFsmSnap.size()));
    in.read(reinterpret_cast<char*>(&li), sizeof(li));
    in.read(reinterpret_cast<char*>(&lt), sizeof(lt));
    if (!in || std::string_view(magic, kMagicFsmSnap.size()) != kMagicFsmSnap) {
      SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap bad header";
      return false;
    }
    std::string blob((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (!n_.snap_applier_(li, lt, blob)) {
      SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap_applier failed idx=" << li;
      return false;
    }
    SKV_INFO << "shard=" << n_.opts_.shard_id << " fsm snap restored idx=" << li << " term=" << lt
             << " bytes=" << blob.size() << " first=" << n_.log_.first_index();
    return true;
  } catch (const std::exception& e) {
    SKV_WARN << "shard=" << n_.opts_.shard_id << " fsm snap restore: " << e.what();
    return false;
  }
}

void Apply::restore_local_fsm_snapshot() {
  if (try_restore_fsm_snap_file()) {
    return;
  }

  if (!n_.on_apply_ || n_.last_applied_ == 0) {
    return;
  }
  const uint64_t first = n_.log_.first_index();
  if (n_.last_applied_ < first) {
    SKV_WARN << "shard=" << n_.opts_.shard_id
             << " fsm snap missing and log compacted past applied; state may be bootstrap-only"
             << " applied=" << n_.last_applied_ << " first=" << first;
    return;
  }
  SKV_INFO << "shard=" << n_.opts_.shard_id << " fsm snap missing; replaying log " << first << ".."
           << n_.last_applied_;
  for (uint64_t i = first; i <= n_.last_applied_; ++i) {
    auto e = n_.log_.entry(i);
    if (!e) {
      continue;
    }
    if (is_conf_entry(*e)) {
      n_.membership_.apply_conf_entry(*e);
      continue;
    }
    invoke_on_apply(i, e->data);
  }
  {
    std::lock_guard<std::mutex> lock(n_.mu_);
    save_local_fsm_snapshot_locked();
  }
}

void Apply::maybe_compact() {
  if (n_.opts_.compact_threshold == 0) {
    return;
  }
  if (!n_.snap_provider_) {
    return;
  }
  const uint64_t first = n_.log_.first_index();
  if (n_.last_applied_ < first) {
    return;
  }
  const uint64_t applied_in_log = n_.last_applied_ - first + 1;
  // 滞回：超过 2*threshold 才砍回 threshold。否则每 apply 一条都 compact+写 FSM 快照，
  // Meta 高写入下会把 Raft 锁堵在磁盘上，TSO/Put 直接超时。
  if (applied_in_log <= n_.opts_.compact_threshold * 2) {
    return;
  }
  // 保留最近 compact_threshold 条已 apply 日志给 AE 追赶。
  // 旧实现 compact_to(last_applied) 会把窗砍空，高写入下 follower 稍慢就只能 InstallSnapshot。
  const uint64_t compact_idx = n_.last_applied_ - n_.opts_.compact_threshold;
  if (compact_idx + 1 <= first) {
    return;
  }
  uint64_t term = n_.log_.snapshot_term();
  if (auto e = n_.log_.entry(compact_idx)) {
    term = e->term;
  }
  const uint64_t old_first = first;
  save_local_fsm_snapshot_locked();
  n_.log_.compact_to(compact_idx, term);
  n_.persist_locked();
  if (n_.log_.first_index() != old_first) {
    SKV_DEBUG << "shard=" << n_.opts_.shard_id << " compacted to index=" << compact_idx
              << " first=" << n_.log_.first_index() << " keep=" << n_.opts_.compact_threshold;
  }
}

void Apply::report_applied(uint64_t index) {
  std::lock_guard<std::mutex> lock(n_.mu_);
  if (!n_.opts_.external_io) {
    return;
  }
  if (index > n_.last_applied_) {
    n_.last_applied_ = index;
  }
  applied_cv_.notify_all();
  maybe_compact();
  n_.persist_locked();
}

bool Apply::wait_applied(uint64_t index, uint32_t timeout_ms, const std::function<void()>& tick) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(n_.mu_);
      if (n_.last_applied_ >= index) {
        return true;
      }
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      std::lock_guard<std::mutex> lock(n_.mu_);
      return n_.last_applied_ >= index;
    }
    if (tick) {
      tick();
    }
    std::unique_lock<std::mutex> lock(n_.mu_);
    if (n_.last_applied_ >= index) {
      return true;
    }
    const auto slice =
        std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(2));
    applied_cv_.wait_until(lock, slice);
  }
}

}  // namespace openkv::raft
