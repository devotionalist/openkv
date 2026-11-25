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

// 测试侧：用 promise 堵住异步 API，不在 StoreFacade 生产接口里再挂一套同步分支。

#pragma once

#include <future>
#include <utility>

#include "openkv/storage/store_service.hpp"

namespace openkv::storage::test {

/**
 * 把「启动异步 + 回调」收成同步等待。
 * @param start_async 接收 CompleteCb 并启动异步的可调用对象
 * @return 回调里的 ProposeResult
 */
inline ProposeResult wait_propose(auto&& start_async) {
  std::promise<ProposeResult> done;
  start_async([&](ProposeResult r) { done.set_value(std::move(r)); });
  return done.get_future().get();
}

/**
 * 同步 propose（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param cmd 命令
 * @param expected_epoch 期望 epoch；0 表示不校验
 */
inline ProposeResult sync_propose(StoreFacade& f, uint32_t shard, const Command& cmd,
                                  uint64_t expected_epoch = 0) {
  return wait_propose([&](auto cb) { f.propose_async(shard, cmd, std::move(cb), expected_epoch); });
}

/**
 * 同步 1PC（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param key 用户 key
 * @param start_ts 开始时间戳
 * @param commit_ts 提交时间戳
 * @param primary 主键
 * @param mvcc_op MVCC 操作码
 * @param value 值
 * @param expected_epoch 期望 epoch；0 不校验
 * @param cas 是否 CAS
 * @param expected_revision CAS 期望 revision
 * @param expire_at_ms TTL 截止；0 表示无
 */
inline ProposeResult sync_one_pc(StoreFacade& f, uint32_t shard, std::string key, uint64_t start_ts,
                                 uint64_t commit_ts, std::string primary, uint8_t mvcc_op,
                                 std::string value, uint64_t expected_epoch = 0, bool cas = false,
                                 uint64_t expected_revision = 0, int64_t expire_at_ms = 0) {
  return wait_propose([&](auto cb) {
    f.one_pc_async(shard, std::move(key), start_ts, commit_ts, std::move(primary), mvcc_op,
                   std::move(value), expected_epoch, cas, expected_revision, expire_at_ms,
                   std::move(cb));
  });
}

/**
 * 同步 prewrite（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param key 用户 key
 * @param start_ts 开始时间戳
 * @param primary 主键
 * @param mvcc_op MVCC 操作码
 * @param value 值
 * @param expected_epoch 期望 epoch；0 不校验
 * @param cas 是否 CAS
 * @param expected_revision CAS 期望 revision
 * @param expire_at_ms TTL 截止；0 表示无
 */
inline ProposeResult sync_prewrite(StoreFacade& f, uint32_t shard, std::string key,
                                   uint64_t start_ts, std::string primary, uint8_t mvcc_op,
                                   std::string value, uint64_t expected_epoch = 0, bool cas = false,
                                   uint64_t expected_revision = 0, int64_t expire_at_ms = 0) {
  return wait_propose([&](auto cb) {
    f.prewrite_async(shard, std::move(key), start_ts, std::move(primary), mvcc_op, std::move(value),
                     expected_epoch, cas, expected_revision, expire_at_ms, std::move(cb));
  });
}

/**
 * 同步 commit（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param key 用户 key
 * @param start_ts 开始时间戳
 * @param commit_ts 提交时间戳
 * @param expected_epoch 期望 epoch；0 不校验
 */
inline ProposeResult sync_commit(StoreFacade& f, uint32_t shard, std::string key, uint64_t start_ts,
                                 uint64_t commit_ts, uint64_t expected_epoch = 0) {
  return wait_propose([&](auto cb) {
    f.commit_async(shard, std::move(key), start_ts, commit_ts, expected_epoch, std::move(cb));
  });
}

/**
 * 同步 rollback（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param key 用户 key
 * @param start_ts 开始时间戳
 * @param expected_epoch 期望 epoch；0 不校验
 */
inline ProposeResult sync_rollback(StoreFacade& f, uint32_t shard, std::string key,
                                   uint64_t start_ts, uint64_t expected_epoch = 0) {
  return wait_propose([&](auto cb) {
    f.rollback_async(shard, std::move(key), start_ts, expected_epoch, std::move(cb));
  });
}

/**
 * 同步 resolve_lock（测例用）。
 * @param f StoreFacade
 * @param shard 分片 id
 * @param key 用户 key
 * @param start_ts 锁的 start_ts
 * @param commit_ts 决议提交时间；0 表示回滚
 * @param expected_epoch 期望 epoch；0 不校验
 */
inline ProposeResult sync_resolve_lock(StoreFacade& f, uint32_t shard, std::string key,
                                       uint64_t start_ts, uint64_t commit_ts,
                                       uint64_t expected_epoch = 0) {
  return wait_propose([&](auto cb) {
    f.resolve_lock_async(shard, std::move(key), start_ts, commit_ts, expected_epoch, std::move(cb));
  });
}

}  // namespace openkv::storage::test
