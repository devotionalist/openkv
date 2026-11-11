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

// @file exec_line.hpp
// Gateway 出站：缓存 Channel/Stub 的 Meta/Storage Admin ExecLine 与 StoreService。

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace openkv::compute {

/// 从 "… redirect=<addr> …" 解析重定向地址；无则空串。
std::string parse_redirect(std::string_view resp);

/// 按 addr 复用 insecure Channel；传输错误后可用 drop_cached_channel 丢掉。
std::shared_ptr<grpc::Channel> cached_channel(const std::string& addr);
void drop_cached_channel(const std::string& addr);

/// 单次 MetaService.ExecLine（带 admin token / deadline）。
std::string meta_exec_line_one(const std::string& addr, const std::string& line, int timeout_ms);

/// 跟随 not_leader redirect：遍历 addrs，指数退避最多 3 轮、每轮最多 6 次跳转。
std::string meta_exec_line(const std::vector<std::string>& addrs, const std::string& line,
                           int timeout_ms = 3000);

/// 单次 AdminService.Ping（不经 ExecLine/WorkPool；就绪探测用）。
void store_admin_ping(const std::string& addr, int timeout_ms = 3000);

/// 单次 AdminService.ExecLine（Storage）。
std::string store_exec_line(const std::string& addr, const std::string& line, int timeout_ms = 15000);

/// 异步 AdminService.ExecLine（gRPC callback stub）；完成回调可能在 gRPC 内部线程。
void store_exec_line_async(const std::string& addr, const std::string& line, int timeout_ms,
                           std::function<void(grpc::Status, std::string)> cb);

/// StoreService.Propose ONEPC 结果（二进制热路径，免 ExecLine hex）。
struct ProposeOnePcResult {
  bool ok{false};
  uint64_t revision{0};
  std::string error;
  std::string redirect_to;
  bool cas_failed{false};
  uint64_t current_revision{0};
};

struct ProposeOnePcArgs {
  uint32_t shard_id{0};
  std::string key;
  std::string value;
  uint64_t start_ts{0};
  uint64_t commit_ts{0};
  std::string primary;  // 空则等同 key
  uint32_t mvcc_op{0};  // 0=Put 1=Delete
  uint64_t epoch{0};
  bool cas{false};
  uint64_t expected_revision{0};
  int64_t expire_at_ms{0};  // 绝对时间；0=无 TTL
};

/// 异步 StoreService.Propose(op=ONEPC)。
void store_propose_onepc_async(const std::string& addr, ProposeOnePcArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposeOnePcResult)> cb);

/// StoreService.Propose PUT/DELETE（无事务 plain KvRecord）。
struct ProposePlainArgs {
  uint32_t shard_id{0};
  bool is_delete{false};
  std::string key;
  std::string value;
  int64_t ttl_ms{0};  // 相对寿命；Storage 侧 stamp 成绝对 expire
  bool cas{false};
  uint64_t expected_revision{0};
};

using ProposePlainResult = ProposeOnePcResult;

void store_propose_plain_async(const std::string& addr, ProposePlainArgs args, int timeout_ms,
                               std::function<void(grpc::Status, ProposePlainResult)> cb);

struct StoreGetResult {
  bool found{false};
  std::string value;
  uint64_t revision{0};
  bool is_leader{false};
  std::string redirect_to;
  bool locked{false};
  std::string error;
};

/// start_ts==0 → plain Get；>0 → MVCC txn_get。
void store_get_async(const std::string& addr, uint32_t shard_id, std::string key, bool linearizable,
                     uint64_t start_ts, int timeout_ms,
                     std::function<void(grpc::Status, StoreGetResult)> cb);

/// 异步 MetaService.GetTs（单点；批补货见 meta_alloc_ts_range_async）。
void meta_get_ts_async(const std::string& addr, int timeout_ms,
                       std::function<void(grpc::Status, uint64_t)> cb);

/// 异步 Meta ExecLine GET_TS n → (start, got)。
void meta_alloc_ts_range_async(const std::vector<std::string>& meta_addrs, uint32_t count,
                               int timeout_ms,
                               std::function<void(grpc::Status, uint64_t start, uint32_t got)> cb);

}  // namespace openkv::compute
