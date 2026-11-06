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

// @file config.cpp
// 配置加载：.conf 为 key=value；.toml 走 toml11（字段名与 conf 一致）。

#include "opskv/config.hpp"

#include <toml.hpp>

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace opskv {
namespace {

std::string trim(std::string s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.erase(s.begin());
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r')) s.pop_back();
  return s;
}

bool ends_with(std::string_view s, std::string_view suf) {
  return s.size() >= suf.size() && s.substr(s.size() - suf.size()) == suf;
}

std::unordered_map<std::string, std::string> parse_kv_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("cannot open config: " + path);
  std::unordered_map<std::string, std::string> m;
  std::string line;
  while (std::getline(in, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#') continue;
    auto pos = line.find('=');
    if (pos == std::string::npos) continue;
    m[trim(line.substr(0, pos))] = trim(line.substr(pos + 1));
  }
  return m;
}

std::unordered_map<std::string, std::string> parse_toml_file(const std::string& path) {
  toml::value data;
  try {
    data = toml::parse(path);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("toml parse failed: ") + e.what());
  }
  std::unordered_map<std::string, std::string> m;
  auto put = [&](const char* key) {
    if (!data.contains(key)) return;
    try {
      m[key] = toml::find<std::string>(data, key);
      return;
    } catch (...) {
    }
    try {
      m[key] = std::to_string(toml::find<std::int64_t>(data, key));
      return;
    } catch (...) {
    }
    try {
      m[key] = toml::find<bool>(data, key) ? "true" : "false";
      return;
    } catch (...) {
    }
    try {
      auto arr = toml::find<std::vector<std::string>>(data, key);
      std::ostringstream oss;
      for (size_t i = 0; i < arr.size(); ++i) {
        if (i) oss << ',';
        oss << arr[i];
      }
      m[key] = oss.str();
    } catch (...) {
    }
  };
  static const char* kKeys[] = {
      "node_id",
      "listen_addr",
      "advertise_addr",
      "meta_addr",
      "data_dir",
      "shard_count",
      "replica_factor",
      "raft_election_timeout_ms",
      "raft_heartbeat_ms",
      "rocksdb_write_buffer_mb",
      "persist_sync",
      "raft_peers",
      "bootstrap_regions",
      "raft_compact_threshold",
      "storage_peers",
      "compute_peers",
      "meta_peers",
  };
  for (const char* k : kKeys) put(k);
  return m;
}

std::unordered_map<std::string, std::string> load_map(const std::string& path) {
  if (ends_with(path, ".toml") || ends_with(path, ".TOML")) {
    return parse_toml_file(path);
  }
  return parse_kv_file(path);
}

std::vector<PeerConfig> parse_peers_csv(const std::string& s) {
  std::vector<PeerConfig> peers;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    item = trim(item);
    auto at = item.find('@');
    if (at == std::string::npos) continue;
    peers.push_back({item.substr(0, at), item.substr(at + 1)});
  }
  return peers;
}

}  // namespace

NodeConfig load_node_config(const std::string& path) {
  auto m = load_map(path);
  NodeConfig c;
  if (m.count("node_id")) c.node_id = m["node_id"];
  if (m.count("listen_addr")) c.listen_addr = m["listen_addr"];
  if (m.count("advertise_addr")) c.advertise_addr = m["advertise_addr"];
  if (m.count("meta_addr")) c.meta_addr = m["meta_addr"];
  if (m.count("data_dir")) c.data_dir = m["data_dir"];
  if (m.count("shard_count")) c.shard_count = static_cast<uint32_t>(std::stoul(m["shard_count"]));
  if (m.count("raft_election_timeout_ms")) {
    c.raft_election_timeout_ms = static_cast<uint32_t>(std::stoul(m["raft_election_timeout_ms"]));
  }
  if (m.count("raft_heartbeat_ms")) {
    c.raft_heartbeat_ms = static_cast<uint32_t>(std::stoul(m["raft_heartbeat_ms"]));
  }
  if (m.count("persist_sync")) c.persist_sync = m["persist_sync"];
  if (m.count("rocksdb_write_buffer_mb")) {
    c.rocksdb_write_buffer_mb = static_cast<uint32_t>(std::stoul(m["rocksdb_write_buffer_mb"]));
  }
  if (m.count("raft_peers")) c.raft_peers = parse_peers_csv(m["raft_peers"]);
  if (m.count("bootstrap_regions")) {
    c.bootstrap_regions.clear();
    std::stringstream ss(m["bootstrap_regions"]);
    std::string item;
    while (std::getline(ss, item, ',')) {
      item = trim(item);
      if (item.empty()) continue;
      c.bootstrap_regions.push_back(static_cast<uint32_t>(std::stoul(item)));
    }
  }
  return c;
}

ClusterBootstrap load_bootstrap(const std::string& path) {
  auto m = load_map(path);
  ClusterBootstrap b;
  if (m.count("meta_addr")) b.meta_addr = m["meta_addr"];
  if (m.count("node_id")) b.node_id = m["node_id"];
  if (m.count("listen_addr")) b.listen_addr = m["listen_addr"];
  if (m.count("shard_count")) b.shard_count = static_cast<uint32_t>(std::stoul(m["shard_count"]));
  if (m.count("replica_factor")) {
    b.replica_factor = static_cast<uint32_t>(std::stoul(m["replica_factor"]));
  }
  if (m.count("raft_election_timeout_ms")) {
    b.raft_election_timeout_ms = static_cast<uint32_t>(std::stoul(m["raft_election_timeout_ms"]));
  }
  if (m.count("raft_heartbeat_ms")) {
    b.raft_heartbeat_ms = static_cast<uint32_t>(std::stoul(m["raft_heartbeat_ms"]));
  }
  if (m.count("data_dir")) b.data_dir = m["data_dir"];
  if (m.count("raft_compact_threshold")) {
    b.raft_compact_threshold = static_cast<size_t>(std::stoull(m["raft_compact_threshold"]));
  }
  if (m.count("storage_peers")) b.storage_peers = parse_peers_csv(m["storage_peers"]);
  if (m.count("compute_peers")) b.compute_peers = parse_peers_csv(m["compute_peers"]);
  if (m.count("meta_peers")) b.meta_peers = parse_peers_csv(m["meta_peers"]);
  return b;
}

}  // namespace opskv
