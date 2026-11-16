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

// @file apply.hpp
// 已提交日志推进状态机：apply、compact、本地 FSM 快照。
// apply_committed / maybe_compact / 快照写：调用方已持 mu_。
// report_applied / wait_applied：自己抢 mu_（Store / 客户端入口）。

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <string>

namespace openkv::raft {

class RaftNode;

/// 管「提交水位之后怎么落到状态机、何时砍日志、本地 FSM 快照」。
class Apply {
 public:
  explicit Apply(RaftNode& node) : n_(node) {}

  /// 把 apply_scheduled_ 推进到 commit_index_（外提 Ready 或同步调 on_apply）。须已持 mu_。
  void apply_committed();
  /// 已 apply 过多时：先存 FSM 快照再砍掉前面的日志。须已持 mu_。
  void maybe_compact();
  /// 启动：优先读盘上的 FSM 快照；没有则按 last_applied_ 回放日志。
  void restore_local_fsm_snapshot();
  /// Apply worker 写完 KV 后回告；抬 last_applied_ 并可能 compact。自己抢 mu_。
  void report_applied(uint64_t index);
  /// 等到 last_applied_ >= index，或超时；可选 tick 回调推进协议。自己抢 mu_。
  bool wait_applied(uint64_t index, uint32_t timeout_ms,
                    const std::function<void()>& tick = {});

  std::condition_variable applied_cv_;  ///< wait_applied 等 last_applied_ 抬升

 private:
  std::string local_fsm_snap_path() const;
  void save_local_fsm_snapshot_locked();
  /// 读盘恢复成功返回 true；失败/没有文件返回 false（调用方再走回放）。
  bool try_restore_fsm_snap_file();
  /// 调 on_apply，并用 in_on_apply_ 挡住重入 recover_apply。
  void invoke_on_apply(uint64_t idx, const std::string& data);
  /// last_applied_ 那条的 term；没有条目时用 snapshot_term。
  uint64_t term_at_applied() const;

  RaftNode& n_;
};

}  // namespace openkv::raft
