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

// @file raft_fault_inject.cpp

#include "openkv/raft/util/raft_fault_inject.hpp"

#include <algorithm>
#include <cstdlib>
#include <string>

#include "openkv/protocol.hpp"

namespace openkv::raft {
namespace {

int env_int(const char* key, int def, int lo, int hi) {
  const char* e = std::getenv(key);
  if (!e || !*e) {
    return def;
  }
  try {
    int v = std::stoi(e);
    return std::clamp(v, lo, hi);
  } catch (...) {
    return def;
  }
}

}  // namespace

RaftOutboundFault RaftOutboundFault::from_env() {
  RaftOutboundFault f;
  f.drop_ae_pct = env_int(proto::env::kFaultDropAePct, 0, 0, 100);
  f.delay_ae_ms = env_int(proto::env::kFaultDelayAeMs, 0, 0, 5000);
  f.drop_raft_pct = env_int(proto::env::kFaultDropRaftPct, 0, 0, 100);
  f.delay_raft_ms = env_int(proto::env::kFaultDelayRaftMs, 0, 0, 5000);
  return f;
}

bool RaftOutboundFault::should_drop(std::string_view type, int rand01_99) const {
  // r ∈ [0,99]；pct=0 时 r < pct 恒假，不必再写 >0。
  const int r = std::clamp(rand01_99, 0, 99);
  if (r < drop_raft_pct) {
    return true;
  }
  return type == "AppendEntries" && r < drop_ae_pct;
}

int RaftOutboundFault::delay_ms_for(std::string_view type) const {
  if (type == "AppendEntries") {
    return std::max(delay_raft_ms, delay_ae_ms);
  }
  return delay_raft_ms;
}

}  // namespace openkv::raft
