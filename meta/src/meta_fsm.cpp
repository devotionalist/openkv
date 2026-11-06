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

// @file meta_fsm.cpp
// MetaCommand 编解码与 apply：
// - 编码格式：文本帧，字段以 `|` 分隔，`\` 转义（便于调试与日志）
// - apply_meta_command：按 MetaOp 分发到 ClusterState 对应写方法
// 数值 op 码与字段顺序须保持稳定，以免破坏已落盘 Raft 日志。

#include "opskv/meta/meta_fsm.hpp"

#include <sstream>

namespace opskv::meta {
namespace {

std::string escape(std::string_view s) {
  std::string out;
  for (char c : s) {
    if (c == '|' || c == '\\') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

std::string unescape(std::string_view s) {
  std::string out;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '\\' && i + 1 < s.size()) {
      out.push_back(s[++i]);
    } else {
      out.push_back(s[i]);
    }
  }
  return out;
}

std::vector<std::string> split_pipe(std::string_view s) {
  std::vector<std::string> parts;
  std::string cur;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '\\' && i + 1 < s.size()) {
      cur.push_back(s[i]);
      cur.push_back(s[++i]);
    } else if (s[i] == '|') {
      parts.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(s[i]);
    }
  }
  parts.push_back(cur);
  return parts;
}

}  // namespace

// 按 op 序列化变长字段；见 MetaCommand 字段注释。
std::string encode_meta_command(const MetaCommand& cmd) {
  std::ostringstream oss;
  oss << static_cast<int>(cmd.op);
  switch (cmd.op) {
    case MetaOp::kRegister:
      oss << '|' << static_cast<int>(cmd.node.role) << '|' << escape(cmd.node.node_id) << '|'
          << escape(cmd.node.address);
      break;
    case MetaOp::kHeartbeat:
      oss << '|' << escape(cmd.node.node_id) << '|' << cmd.leader_shards.size();
      for (uint32_t s : cmd.leader_shards) {
        oss << '|' << s;
      }
      break;
    case MetaOp::kSetLearner:
      oss << '|' << cmd.shard_id << '|' << escape(cmd.learner.node_id) << '|'
          << escape(cmd.learner.address);
      break;
    case MetaOp::kPromote:
      oss << '|' << cmd.shard_id << '|' << escape(cmd.learner.node_id);
      break;
    case MetaOp::kBumpRoute:
      break;
    case MetaOp::kSplitRegion:
      oss << '|' << cmd.shard_id << '|' << escape(cmd.split_key);
      break;
    case MetaOp::kMergeRegions:
      oss << '|' << cmd.shard_id << '|' << cmd.merge_right_id;
      break;
    case MetaOp::kRemoveReplica:
      oss << '|' << cmd.shard_id << '|' << escape(cmd.learner.node_id);
      break;
    case MetaOp::kPlaceRegion:
      oss << '|' << cmd.shard_id << '|' << cmd.place_replicas.size();
      for (const auto& r : cmd.place_replicas) {
        oss << '|' << escape(r.node_id) << '|' << escape(r.address);
      }
      break;
    case MetaOp::kAllocTs:
      oss << '|' << cmd.ts_count;
      break;
    case MetaOp::kAdvanceSafePoint:
      oss << '|' << cmd.ts_count;  // retain_ts
      break;
    case MetaOp::kReportMinStartTs:
      oss << '|' << cmd.ts_value;
      if (!cmd.node.node_id.empty()) oss << '|' << escape(cmd.node.node_id);
      break;
    case MetaOp::kSweepStale:
      oss << '|' << cmd.ts_count;  // ttl_ms
      break;
    case MetaOp::kSetNodeState:
      oss << '|' << escape(cmd.node.node_id) << '|' << cmd.ts_value;
      break;
    case MetaOp::kUpsertMigrate:
      oss << '|' << escape(cmd.migrate.task_id) << '|' << cmd.migrate.region_id << '|'
          << cmd.migrate.state << '|' << cmd.migrate.desired_peers.size();
      for (const auto& p : cmd.migrate.desired_peers) oss << '|' << escape(p);
      break;
    case MetaOp::kEraseMigrate:
      oss << '|' << escape(cmd.migrate.task_id);
      break;
    case MetaOp::kEraseTombstone:
      oss << '|' << escape(cmd.node.node_id);
      break;
  }
  return oss.str();
}

// 与 encode 对称；字段不足或非法 op 返回 false。
bool decode_meta_command(std::string_view data, MetaCommand* out) {
  auto parts = split_pipe(data);
  if (parts.empty()) {
    return false;
  }
  out->op = static_cast<MetaOp>(std::stoi(parts[0]));
  try {
    switch (out->op) {
      case MetaOp::kRegister:
        if (parts.size() < 4) return false;
        out->node.role = static_cast<NodeRole>(std::stoi(parts[1]));
        out->node.node_id = unescape(parts[2]);
        out->node.address = unescape(parts[3]);
        return true;
      case MetaOp::kHeartbeat: {
        if (parts.size() < 3) return false;
        out->node.node_id = unescape(parts[1]);
        size_t n = static_cast<size_t>(std::stoul(parts[2]));
        if (parts.size() < 3 + n) return false;
        out->leader_shards.clear();
        for (size_t i = 0; i < n; ++i) {
          out->leader_shards.push_back(static_cast<uint32_t>(std::stoul(parts[3 + i])));
        }
        return true;
      }
      case MetaOp::kSetLearner:
        if (parts.size() < 4) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        out->learner.node_id = unescape(parts[2]);
        out->learner.address = unescape(parts[3]);
        out->learner.is_learner = true;
        return true;
      case MetaOp::kPromote:
        if (parts.size() < 3) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        out->learner.node_id = unescape(parts[2]);
        return true;
      case MetaOp::kBumpRoute:
        return true;
      case MetaOp::kSplitRegion:
        if (parts.size() < 3) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        out->split_key = unescape(parts[2]);
        return true;
      case MetaOp::kMergeRegions:
        if (parts.size() < 3) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        out->merge_right_id = static_cast<uint32_t>(std::stoul(parts[2]));
        return true;
      case MetaOp::kRemoveReplica:
        if (parts.size() < 3) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        out->learner.node_id = unescape(parts[2]);
        return true;
      case MetaOp::kPlaceRegion: {
        if (parts.size() < 3) return false;
        out->shard_id = static_cast<uint32_t>(std::stoul(parts[1]));
        size_t n = static_cast<size_t>(std::stoul(parts[2]));
        if (parts.size() < 3 + n * 2) return false;
        out->place_replicas.clear();
        for (size_t i = 0; i < n; ++i) {
          Replica r;
          r.node_id = unescape(parts[3 + i * 2]);
          r.address = unescape(parts[3 + i * 2 + 1]);
          out->place_replicas.push_back(std::move(r));
        }
        return true;
      }
      case MetaOp::kAllocTs:
        if (parts.size() < 2) return false;
        out->ts_count = static_cast<uint32_t>(std::stoul(parts[1]));
        if (out->ts_count == 0) out->ts_count = 1;
        return true;
      case MetaOp::kAdvanceSafePoint:
        if (parts.size() < 2) return false;
        out->ts_count = static_cast<uint32_t>(std::stoul(parts[1]));
        return true;
      case MetaOp::kReportMinStartTs:
        if (parts.size() < 2) return false;
        out->ts_value = std::stoull(parts[1]);
        if (parts.size() >= 3) out->node.node_id = unescape(parts[2]);
        return true;
      case MetaOp::kSweepStale:
        if (parts.size() < 2) return false;
        out->ts_count = static_cast<uint32_t>(std::stoul(parts[1]));
        if (out->ts_count == 0) out->ts_count = 10000;
        return true;
      case MetaOp::kSetNodeState:
        if (parts.size() < 3) return false;
        out->node.node_id = unescape(parts[1]);
        out->ts_value = std::stoull(parts[2]);
        return true;
      case MetaOp::kUpsertMigrate: {
        if (parts.size() < 5) return false;
        out->migrate.task_id = unescape(parts[1]);
        out->migrate.region_id = static_cast<uint32_t>(std::stoul(parts[2]));
        out->migrate.state = std::stoi(parts[3]);
        size_t np = static_cast<size_t>(std::stoul(parts[4]));
        if (parts.size() < 5 + np) return false;
        out->migrate.desired_peers.clear();
        for (size_t i = 0; i < np; ++i) {
          out->migrate.desired_peers.push_back(unescape(parts[5 + i]));
        }
        return true;
      }
      case MetaOp::kEraseMigrate:
        if (parts.size() < 2) return false;
        out->migrate.task_id = unescape(parts[1]);
        return true;
      case MetaOp::kEraseTombstone:
        if (parts.size() < 2) return false;
        out->node.node_id = unescape(parts[1]);
        return true;
    }
  } catch (...) {
    return false;
  }
  return false;
}

// Raft 已提交后调用；各副本顺序一致。忽略未知 op（向前兼容）。
void apply_meta_command(ClusterState& state, const MetaCommand& cmd) {
  switch (cmd.op) {
    case MetaOp::kRegister:
      state.register_node(cmd.node);
      break;
    case MetaOp::kHeartbeat:
      state.heartbeat(cmd.node.node_id, cmd.leader_shards);
      break;
    case MetaOp::kSetLearner:
      state.set_learner(cmd.shard_id, cmd.learner);
      break;
    case MetaOp::kPromote:
      state.promote_learner(cmd.shard_id, cmd.learner.node_id);
      break;
    case MetaOp::kBumpRoute:
      state.bump_route_version();
      break;
    case MetaOp::kSplitRegion:
      state.split_region(cmd.shard_id, cmd.split_key);
      break;
    case MetaOp::kMergeRegions:
      state.merge_regions(cmd.shard_id, cmd.merge_right_id);
      break;
    case MetaOp::kRemoveReplica:
      state.remove_replica(cmd.shard_id, cmd.learner.node_id);
      break;
    case MetaOp::kPlaceRegion:
      state.place_replicas(cmd.shard_id, cmd.place_replicas);
      break;
    case MetaOp::kAllocTs:
      state.alloc_ts(cmd.ts_count);
      break;
    case MetaOp::kAdvanceSafePoint:
      state.advance_gc_safe_ts(cmd.ts_count);
      break;
    case MetaOp::kReportMinStartTs:
      state.report_min_start_ts(cmd.ts_value, cmd.node.node_id);
      break;
    case MetaOp::kSweepStale:
      state.sweep_stale(static_cast<int64_t>(cmd.ts_count));
      break;
    case MetaOp::kSetNodeState:
      state.set_node_state(cmd.node.node_id, static_cast<NodeState>(cmd.ts_value));
      break;
    case MetaOp::kUpsertMigrate:
      state.upsert_migrate(cmd.migrate);
      break;
    case MetaOp::kEraseMigrate:
      state.erase_migrate(cmd.migrate.task_id);
      break;
    case MetaOp::kEraseTombstone:
      state.erase_tombstone(cmd.node.node_id);
      break;
  }
}

}  // namespace opskv::meta
