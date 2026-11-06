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

// @file config.hpp
// SeaStarKV 公共配置模型与加载接口。
//
// 架构位置：各进程启动时读本地配置（.conf key=value 或 .toml）；
// bootstrap 描述集群初始拓扑，运行时路由以 Meta 服务为准。
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace opskv {

/// 集群内一个对等节点的标识与可达地址。
struct PeerConfig {
  std::string node_id;
  std::string address;
};

/// 单节点运行时参数（监听地址、数据目录、Raft/RocksDB 相关 tunable）。
struct NodeConfig {
  std::string node_id;
  std::string listen_addr{"0.0.0.0:9000"};
  /// 对外可达地址（REGISTER / peer 自描述）；空则 listen_addr，且 0.0.0.0→127.0.0.1。
  std::string advertise_addr;
  std::string meta_addr{"127.0.0.1:8500"};
  std::string data_dir{"./data"};
  uint32_t shard_count{16};
  uint32_t raft_election_timeout_ms{1000};
  uint32_t raft_heartbeat_ms{100};
  uint32_t rocksdb_write_buffer_mb{64};
  /// E1：Raft 日志 + RocksDB WAL 同步级别：none|data|all（可用 SKV_PERSIST_SYNC 覆盖）。
  std::string persist_sync{"data"};
  /// 多副本：id@host:port,... ；空则单节点（仅自己）。
  std::vector<PeerConfig> raft_peers;
  /// 启动时创建的 Region（逗号分隔）；缺省 "0"。空字符串 = 不预建（等待 PLACE/ENSURE）。
  std::vector<uint32_t> bootstrap_regions{0};
};

// 初始集群静态配置（bootstrap）；运行时以 Meta 路由表为准
/// 集群引导信息：存储/计算/Meta 节点列表、分片数与副本因子。
struct ClusterBootstrap {
  std::vector<PeerConfig> storage_peers;
  std::vector<PeerConfig> compute_peers;
  std::vector<PeerConfig> meta_peers;  // Meta Raft 副本（>=3 时多数派 HA）
  std::string meta_addr;
  std::string node_id;                 // 本 Meta 进程 id（多副本时必填）
  std::string listen_addr{"0.0.0.0:8500"};
  /// Meta 本地数据根（raft persist 在 data_dir/raft）；空则 ./data/<node_id>
  std::string data_dir;
  uint32_t shard_count{16};
  uint32_t replica_factor{3};
  uint32_t raft_election_timeout_ms{800};
  uint32_t raft_heartbeat_ms{100};
  /// Meta 日志压缩阈值；0=不压缩。有 ClusterState 快照后默认 256。
  size_t raft_compact_threshold{256};
};

/// 从 path 加载节点配置；文件不存在或格式错误时抛出 std::runtime_error。
NodeConfig load_node_config(const std::string& path);
/// 从 path 加载集群 bootstrap；peer 列表格式见 config.cpp 注释。
ClusterBootstrap load_bootstrap(const std::string& path);

}  // namespace opskv
