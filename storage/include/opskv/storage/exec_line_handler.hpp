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

// @file exec_line_handler.hpp
// Storage 管理面行协议解析（gRPC AdminService.ExecLine 入口）。

#pragma once

#include "opskv/admin_acl.hpp"
#include "opskv/peer_counter.hpp"
#include "opskv/storage/inflight_gate.hpp"
#include "opskv/storage/store_service.hpp"

#include <atomic>
#include <cstdint>
#include <string>

namespace opskv::storage {

// 行协议命令解析（PUT/GET/ADMIN_*/Raft 行语义等），供 gRPC ExecLine 调用。
class ExecLineHandler {
 public:
  explicit ExecLineHandler(StoreFacade& facade);
  ~ExecLineHandler() = default;

  /// gRPC AdminService.ExecLine 入口。
  std::string process_line(const std::string& line, AdminSession* sess = nullptr);

  InflightGate& inflight_gate() { return gate_; }

 private:
  std::string handle_line(const std::string& line, AdminSession* sess);

  StoreFacade& facade_;
  std::string admin_token_;
  InflightGate gate_;
  std::atomic<uint64_t> req_total_{0};
  std::atomic<uint64_t> req_gw_{0};
  std::atomic<uint64_t> req_raft_{0};
  std::atomic<uint64_t> req_sync_{0};
  std::atomic<uint64_t> req_admin_{0};
  PeerCounter peer_raft_;
  PeerCounter peer_sync_;
};

}  // namespace opskv::storage
