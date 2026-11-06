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

// @file main.cpp
// skv — OpsKV C++ 运维 CLI（生命周期 + Meta/Storage TCP + KV）；CLI11 解析。
#include "cluster.hpp"
#include "tcp_line.hpp"

#include <CLI/CLI.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

std::vector<std::string> storage_addrs() {
  if (const char* e = std::getenv("SKV_STORAGES")) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : std::string(e)) {
      if (c == ',') {
        if (!cur.empty()) out.push_back(cur);
        cur.clear();
      } else {
        cur.push_back(c);
      }
    }
    if (!cur.empty()) out.push_back(cur);
    if (!out.empty()) return out;
  }
  return {"127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102",
          "127.0.0.1:9103", "127.0.0.1:9104", "127.0.0.1:9105"};
}

std::vector<std::string> meta_addrs() {
  if (const char* e = std::getenv("SKV_META")) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : std::string(e)) {
      if (c == ',') {
        if (!cur.empty()) out.push_back(cur);
        cur.clear();
      } else {
        cur.push_back(c);
      }
    }
    if (!cur.empty()) out.push_back(cur);
    if (!out.empty()) return out;
  }
  return {"127.0.0.1:8500", "127.0.0.1:8501", "127.0.0.1:8502"};
}

// Storage TCP：优先走 preferred；跟随 redirect；再回退 preferred/全量列表。
// Admin propose（SPLIT/ConfChange）常 >2s，过短会 recv timeout →「truncated」假失败。
std::string storage_call(const std::string& req,
                         const std::vector<std::string>& preferred = {},
                         int timeout_ms = 8000, int max_attempts = 20) {
  auto addrs = preferred.empty() ? storage_addrs() : preferred;
  if (addrs.empty()) addrs = storage_addrs();
  std::string hint;
  std::string last_err;
  if (max_attempts < 1) max_attempts = 1;
  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    std::string addr = hint.empty() ? addrs[static_cast<size_t>(attempt) % addrs.size()] : hint;
    try {
      auto resp = skv::tcp_line(addr, req, timeout_ms);
      if (resp.find("not_leader") != std::string::npos) {
        last_err = resp;
        // 仅跟随仍在候选地址集内的 redirect（踢出节点上的陈旧 *L 会导致死循环）
        if (auto r = skv::redirect_of(resp); r && *r != addr && *r != hint) {
          bool known = false;
          for (const auto& a : addrs) {
            if (a == *r) {
              known = true;
              break;
            }
          }
          if (known) {
            hint = *r;
          } else {
            hint.clear();
          }
        } else {
          hint.clear();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        continue;
      }
      if (resp.find("no_region") != std::string::npos) {
        last_err = resp;
        hint.clear();
        continue;
      }
      return resp;
    } catch (const std::exception& e) {
      last_err = e.what();
      hint.clear();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  throw std::runtime_error("storage_call failed: " + last_err);
}

std::string meta_call(const std::string& req);  // forward

std::vector<std::string> region_addrs(uint32_t rid) {
  std::vector<std::string> out;
  try {
    auto resp = meta_call("GET_ROUTE");
    auto parts = skv::split_ws(resp);
    for (size_t i = 4; i < parts.size(); ++i) {
      const auto& tok = parts[i];
      auto c1 = tok.find(':');
      if (c1 == std::string::npos) continue;
      if (static_cast<uint32_t>(std::stoul(tok.substr(0, c1))) != rid) continue;
      auto c2 = tok.find(':', c1 + 1);
      auto c3 = tok.find(':', c2 + 1);
      if (c2 == std::string::npos || c3 == std::string::npos) continue;
      std::string rest = tok.substr(c3 + 1);
      auto comma = rest.find(',');
      if (comma == std::string::npos) break;
      std::stringstream ss(rest.substr(comma + 1));
      std::string item;
      while (std::getline(ss, item, ',')) {
        while (!item.empty() && (item.back() == 'L' || item.back() == 'N') &&
               item.size() >= 2 && item[item.size() - 2] == '*') {
          item = item.substr(0, item.size() - 2);
        }
        auto at = item.find('@');
        if (at != std::string::npos) out.push_back(item.substr(at + 1));
      }
      break;
    }
  } catch (...) {
  }
  return out;
}

std::string meta_call(const std::string& req) {
  std::string hint;
  std::string last_err;
  auto addrs = meta_addrs();
  for (int attempt = 0; attempt < 8; ++attempt) {
    std::string addr = hint.empty() ? addrs[static_cast<size_t>(attempt) % addrs.size()] : hint;
    try {
      auto resp = skv::tcp_line(addr, req);
      if (resp.find("not_leader") != std::string::npos) {
        if (auto r = skv::redirect_of(resp)) {
          last_err = resp;
          // 自指 / 重复 redirect：换下一个种子，避免卡死在假 Leader
          if (*r == addr || *r == hint) {
            hint.clear();
            continue;
          }
          hint = *r;
          continue;
        }
      }
      return resp;
    } catch (const std::exception& e) {
      last_err = e.what();
      hint.clear();
    }
  }
  throw std::runtime_error("meta_call failed: " + last_err);
}

uint32_t locate_region(const std::string& key) {
  try {
    auto resp = meta_call("LOCATE " + skv::to_hex(key));
    auto parts = skv::split_ws(resp);
    if (parts.size() >= 3 && parts[0] == "OK") {
      return static_cast<uint32_t>(std::stoul(parts[2]));
    }
  } catch (...) {
  }
  return 0;
}

int cmd_put(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: skv put <key> <value>\n";
    return 2;
  }
  uint32_t region = locate_region(args[0]);
  std::ostringstream req;
  req << "PUT " << region << ' ' << skv::to_hex(args[0]) << ' ' << skv::to_hex(args[1]);
  // 迁副本后首轮 propose 常 >1s；过短会 truncation/假 not_leader。流量脚本另用外层 timeout 卡死保护。
  auto resp = storage_call(req.str(), region_addrs(region), 8000, 8);
  std::cout << resp << "  (region=" << region << ")\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_get(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv get <key>\n";
    return 2;
  }
  uint32_t region = locate_region(args[0]);
  std::ostringstream req;
  req << "GET " << region << ' ' << skv::to_hex(args[0]);
  auto resp = storage_call(req.str(), region_addrs(region), 8000, 8);
  auto parts = skv::split_ws(resp);
  if (parts.size() >= 4 && parts[0] == "OK" && parts[3] != "-") {
    std::string val;
    if (skv::from_hex(parts[3], &val)) {
      std::cout << "found=true rev=" << parts[2] << " value=" << val << " region=" << region
                << "\n";
      return 0;
    }
  }
  if (parts.size() >= 3 && parts[0] == "OK") {
    std::cout << "found=false region=" << region << "\n";
    return 0;
  }
  std::cout << resp << "\n";
  return 1;
}

int cmd_del(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv del <key>\n";
    return 2;
  }
  uint32_t region = locate_region(args[0]);
  std::ostringstream req;
  req << "DEL " << region << ' ' << skv::to_hex(args[0]);
  auto resp = storage_call(req.str(), region_addrs(region));
  std::cout << resp << "  (region=" << region << ")\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_locate(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv locate <key>\n";
    return 2;
  }
  auto resp = meta_call("LOCATE " + skv::to_hex(args[0]));
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

std::unordered_map<std::string, std::string> load_storage_nodes() {
  std::unordered_map<std::string, std::string> out;
  auto resp = meta_call("LIST_NODES storage");
  auto parts = skv::split_ws(resp);
  if (parts.size() < 3 || parts[0] != "OK") return out;
  for (size_t i = 3; i < parts.size(); ++i) {
    auto at = parts[i].find('@');
    if (at == std::string::npos) continue;
    auto r1 = parts[i].rfind(':');
    if (r1 == std::string::npos || r1 == 0) continue;
    auto r2 = parts[i].rfind(':', r1 - 1);
    if (r2 == std::string::npos || r2 <= at) continue;
    out[parts[i].substr(0, at)] = parts[i].substr(at + 1, r2 - at - 1);
  }
  return out;
}

struct RegionPeers {
  std::vector<std::pair<std::string, std::string>> replicas;
  std::string leader_id;
};

RegionPeers parse_region_peers(uint32_t rid) {
  RegionPeers rp;
  auto resp = meta_call("GET_ROUTE");
  auto parts = skv::split_ws(resp);
  for (size_t i = 4; i < parts.size(); ++i) {
    const auto& tok = parts[i];
    auto c1 = tok.find(':');
    if (c1 == std::string::npos) continue;
    uint32_t id = static_cast<uint32_t>(std::stoul(tok.substr(0, c1)));
    if (id != rid) continue;
    auto c2 = tok.find(':', c1 + 1);
    auto c3 = tok.find(':', c2 == std::string::npos ? tok.size() : c2 + 1);
    if (c2 == std::string::npos || c3 == std::string::npos) continue;
    std::string rest = tok.substr(c3 + 1);
    auto comma = rest.find(',');
    if (comma == std::string::npos) return rp;
    std::stringstream ss(rest.substr(comma + 1));
    std::string item;
    while (std::getline(ss, item, ',')) {
      if (item.empty()) continue;
      const bool is_leader = item.find("*L") != std::string::npos;
      while (!item.empty() && (item.back() == 'L' || item.back() == 'N') &&
             item.size() >= 2 && item[item.size() - 2] == '*') {
        item = item.substr(0, item.size() - 2);
      }
      auto at = item.find('@');
      if (at == std::string::npos) continue;
      rp.replicas.emplace_back(item.substr(0, at), item.substr(at + 1));
      if (is_leader) rp.leader_id = rp.replicas.back().first;
    }
    break;
  }
  return rp;
}

int cmd_split(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: skv split <region_id> <split_key>\n"
              << "  (meta-only; cross-store move = skv place / balance, not scatter-on-split)\n";
    return 2;
  }
  auto resp = meta_call("SPLIT_REGION " + args[0] + " " + skv::to_hex(args[1]));
  std::cout << resp << "\n";
  if (resp.rfind("OK SPLIT", 0) != 0) return 1;
  auto parts = skv::split_ws(resp);
  // OK SPLIT left right
  if (parts.size() < 4) return 1;
  const uint32_t left_rid = static_cast<uint32_t>(std::stoul(parts[2]));
  std::vector<std::string> left_addrs;
  for (int i = 0; i < 10 && left_addrs.empty(); ++i) {
    left_addrs = region_addrs(left_rid);
    if (left_addrs.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (left_addrs.empty()) {
    std::cerr << "split: no left-region peers in Meta route (refuse all-storage broadcast)\n";
    return 1;
  }
  // 只打左 Region 副本 / 跟 Leader，避免对 6 台全广播刷 not_leader 噪音
  try {
    auto prep = storage_call("ADMIN_PREPARE_SPLIT " + parts[2] + " " + skv::to_hex(args[1]),
                             left_addrs);
    std::cout << "prepare -> " << prep << "\n";
    if (prep.rfind("OK", 0) != 0) return 1;
  } catch (const std::exception& e) {
    std::cerr << "prepare FAIL " << e.what() << "\n";
    return 1;
  }
  try {
    auto spl = storage_call(
        "ADMIN_SPLIT " + parts[2] + " " + parts[3] + " " + skv::to_hex(args[1]), left_addrs);
    std::cout << "split -> " << spl << "\n";
    if (spl.rfind("OK", 0) != 0) return 1;
  } catch (const std::exception& e) {
    std::cerr << "split FAIL " << e.what() << "\n";
    return 1;
  }
  return 0;
}

int cmd_merge(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: skv merge <left_region_id> <right_region_id>\n";
    return 2;
  }
  // 先搬数据再改 Meta，避免路由已合并但数据仍在 right
  std::ostringstream admin;
  admin << "ADMIN_MERGE " << args[0] << ' ' << args[1];
  for (const auto& a : storage_addrs()) {
    try {
      std::cout << a << " -> " << skv::tcp_line(a, admin.str()) << "\n";
    } catch (const std::exception& e) {
      std::cout << a << " FAIL " << e.what() << "\n";
    }
  }
  auto resp = meta_call("MERGE_REGIONS " + args[0] + " " + args[1]);
  std::cout << resp << "\n";
  return resp.rfind("OK MERGE", 0) == 0 ? 0 : 1;
}

int cmd_nodes(const std::vector<std::string>& args) {
  std::string req = "LIST_NODES";
  if (!args.empty()) req += " " + args[0];
  auto resp = meta_call(req);
  std::cout << resp << "\n";
  // 附带可读状态：0=joining 1=active 2=draining 3=offline 4=tombstone
  static const char* kNames[] = {"joining", "active", "draining", "offline", "tombstone"};
  auto parts = skv::split_ws(resp);
  for (size_t i = 3; i < parts.size(); ++i) {
    auto colon = parts[i].rfind(':');
    if (colon == std::string::npos) continue;
    try {
      int st = std::stoi(parts[i].substr(colon + 1));
      if (st >= 0 && st <= 4) {
        std::cout << "  " << parts[i].substr(0, parts[i].find('@')) << "  " << kNames[st]
                  << "\n";
      }
    } catch (...) {
    }
  }
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_join(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv join <storage-id>\n";
    return 2;
  }
  auto resp = meta_call("JOIN_NODE " + args[0]);
  std::cout << resp << "\n";
  if (resp.rfind("OK", 0) != 0) {
    if (resp.find("unreachable") != std::string::npos) {
      std::cerr << "join refused: store unreachable (PING failed)\n";
    } else if (resp.find("heartbeat") != std::string::npos) {
      std::cerr << "join refused: store heartbeat missing/stale\n";
    }
    return 1;
  }
  std::cout << "joined " << args[0] << " → Active; can receive replicas via balance/place\n";
  return 0;
}

int cmd_scale_in(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv scale-in <storage-id>\n";
    return 2;
  }
  auto resp = meta_call("STORE_DELETE " + args[0]);
  std::cout << resp << "\n";
  if (resp.rfind("OK", 0) != 0) return 1;
  std::cout << "draining " << args[0]
            << "; RegionScheduler will migrate replicas then mark Tombstone\n";
  return 0;
}

int cmd_scale_in_status(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv scale-in-status <storage-id>\n";
    return 2;
  }
  auto resp = meta_call("STORE_STATUS " + args[0]);
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_remove_tombstone(const std::vector<std::string>& args) {
  if (args.empty()) {
    std::cerr << "usage: skv remove-tombstone <storage-id>\n";
    return 2;
  }
  auto resp = meta_call("REMOVE_TOMBSTONE " + args[0]);
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_balance(const std::vector<std::string>& args) {
  std::string req = "BALANCE";
  if (!args.empty()) req += " " + args[0];
  auto resp = meta_call(req);
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_place(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: skv place <region_id> <node1,node2,...>\n";
    return 2;
  }
  const uint32_t rid = static_cast<uint32_t>(std::stoul(args[0]));
  std::vector<std::string> desired;
  {
    std::stringstream ss(args[1]);
    std::string item;
    while (std::getline(ss, item, ',')) {
      if (!item.empty()) desired.push_back(item);
    }
  }
  if (desired.empty()) {
    std::cerr << "empty peer list\n";
    return 2;
  }

  auto nodes = load_storage_nodes();
  for (const auto& id : desired) {
    if (!nodes.count(id)) {
      std::cerr << "unknown storage node: " << id << " (see skv nodes storage)\n";
      return 1;
    }
  }

  auto cur = parse_region_peers(rid);
  if (cur.replicas.empty()) {
    std::cerr << "region " << rid << " not found in route\n";
    return 1;
  }

  // place 开始时的原成员（promote 后会改 cur，transfer 勿把新 peer 当成 preexisting）
  const std::vector<std::string> original_ids = [&] {
    std::vector<std::string> ids;
    for (const auto& [nid, _] : cur.replicas) ids.push_back(nid);
    return ids;
  }();
  auto was_original = [&](const std::string& id) {
    for (const auto& o : original_ids)
      if (o == id) return true;
    return false;
  };

  auto has = [&](const std::string& id) {
    for (const auto& [nid, _] : cur.replicas)
      if (nid == id) return true;
    return false;
  };

  // Meta 路由在 PLACE_REGION 前不会更新；Admin 须走「当前成员 + 目标节点」。
  // 半途重试时 Leader 常已在 desired 上，而 Meta 仍列旧副本且可能不带 redirect。
  auto live_addrs = [&]() {
    std::vector<std::string> out;
    auto push = [&](const std::string& addr) {
      if (addr.empty()) return;
      for (const auto& a : out)
        if (a == addr) return;
      out.push_back(addr);
    };
    for (const auto& [_, addr] : cur.replicas) push(addr);
    for (const auto& id : desired) {
      auto it = nodes.find(id);
      if (it != nodes.end()) push(it->second);
    }
    return out;
  };

  // 返回：1=仍 joint，0=已确认非 joint，-1=无人可询（不可当作已退出）
  // exclude_addr：踢出节点常收不到 LeaveJoint，会永久 joint=1，须排除（与 StorageNode::place 一致）
  auto joint_status = [&](const std::string& exclude_addr = {}) -> int {
    bool saw_ok = false;
    for (const auto& a : live_addrs()) {
      if (!exclude_addr.empty() && a == exclude_addr) continue;
      try {
        auto st = skv::tcp_line(a, "ADMIN_REGION_STATS " + std::to_string(rid), 1500);
        if (st.rfind("OK", 0) != 0) continue;
        saw_ok = true;
        if (st.find("joint=1") != std::string::npos) return 1;
      } catch (...) {
      }
    }
    return saw_ok ? 0 : -1;
  };
  auto region_in_joint = [&]() -> bool { return joint_status() != 0; };

  auto wait_caught_up = [&](const std::string& peer_id) -> bool {
    for (int i = 0; i < 400; ++i) {  // ~40s
      std::string r;
      try {
        r = storage_call("ADMIN_PEER_PROGRESS " + std::to_string(rid) + " " + peer_id,
                         live_addrs());
      } catch (const std::exception& e) {
        r = std::string("ERR ") + e.what();
      }
      std::cout << "progress " << peer_id << " -> " << r << "\n";
      auto parts = skv::split_ws(r);
      // OK PROGRESS match commit last caught [learner]
      if (parts.size() >= 6 && parts[0] == "OK" && parts[5] == "1") return true;
      if (i > 0 && i % 50 == 0) {
        for (const auto& oid : original_ids) {
          auto it = nodes.find(oid);
          if (it == nodes.end()) continue;
          try {
            skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 1000);
          } catch (...) {
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
  };

  // PROMOTE 仅表示已 propose；须等 learner→voter(0) 且确认 LeaveJoint（joint_status==0）
  auto wait_promoted = [&](const std::string& peer_id) -> bool {
    for (int i = 0; i < 200; ++i) {
      const int js = joint_status();
      try {
        auto r = storage_call("ADMIN_PEER_PROGRESS " + std::to_string(rid) + " " + peer_id,
                              live_addrs());
        auto parts = skv::split_ws(r);
        int learner = 2;
        if (parts.size() >= 7 && parts[0] == "OK") learner = std::stoi(parts[6]);
        std::cout << "promote-wait " << peer_id << " joint_status=" << js
                  << " learner=" << learner << "\n";
        // learner=0 且已确认非 joint；learner=2 时催选主并继续等
        if (js == 0 && learner == 0) return true;
        if (learner == 2 && i > 0 && i % 20 == 0) {
          for (const auto& oid : original_ids) {
            auto it = nodes.find(oid);
            if (it == nodes.end()) continue;
            try {
              skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 1000);
            } catch (...) {
            }
          }
        }
      } catch (const std::exception& e) {
        std::cout << "promote-wait " << peer_id << " ERR " << e.what() << "\n";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
  };

  // 1) 加入缺失副本：ENSURE → ADD_LEARNER → 等追赶 → PROMOTE
  for (const auto& id : desired) {
    if (has(id)) continue;
    const std::string& addr = nodes[id];
    // 数据面可能已完成上一轮 place（Meta 路由未更新）：已是 voter 且追平则跳过 ConfChange
    {
      try {
        auto r = storage_call("ADMIN_PEER_PROGRESS " + std::to_string(rid) + " " + id, live_addrs());
        auto parts = skv::split_ws(r);
        if (parts.size() >= 7 && parts[0] == "OK" && parts[5] == "1" && parts[6] == "0") {
          std::cout << "skip add " << id << " (already voter caught-up)\n";
          cur.replicas.emplace_back(id, addr);
          continue;
        }
      } catch (...) {
      }
    }
    std::ostringstream peers;
    for (size_t i = 0; i < cur.replicas.size(); ++i) {
      if (i) peers << ',';
      peers << cur.replicas[i].first << '@' << cur.replicas[i].second;
    }
    peers << ',' << id << '@' << addr << "*N";
    try {
      auto er = skv::tcp_line(addr, "ADMIN_ENSURE_REGION " + std::to_string(rid) + " " + peers.str(),
                             3000);
      std::cout << "ensure " << id << " -> " << er << "\n";
    } catch (const std::exception& e) {
      std::cerr << "ensure " << id << " FAIL " << e.what() << "\n";
      return 1;
    }
    {
      std::ostringstream req;
      req << "ADMIN_ADD_LEARNER " << rid << ' ' << id << '@' << addr;
      std::string r;
      bool ok = false;
      for (int attempt = 0; attempt < 40; ++attempt) {
        try {
          r = storage_call(req.str(), live_addrs());
        } catch (const std::exception& e) {
          r = std::string("ERR ") + e.what();
        }
        std::cout << "add_learner " << id << " -> " << r << "\n";
        if (r.rfind("OK", 0) == 0) {
          ok = true;
          break;
        }
        if (r.find("busy") != std::string::npos || r.find("not_leader") != std::string::npos ||
            r.find("joint") != std::string::npos || r.find("add_learner_failed") != std::string::npos) {
          if (r.find("not_leader") != std::string::npos && attempt >= 3 && !desired.empty()) {
            auto it = nodes.find(desired[0]);
            if (it != nodes.end()) {
              try {
                skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 1500);
              } catch (...) {
              }
            }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(150));
          continue;
        }
        break;
      }
      if (!ok) return 1;
    }
    if (!wait_caught_up(id)) {
      std::cerr << "timeout waiting catch-up for " << id << "\n";
      return 1;
    }
    {
      std::string r;
      bool ok = false;
      for (int attempt = 0; attempt < 40; ++attempt) {
        try {
          r = storage_call("ADMIN_PROMOTE " + std::to_string(rid) + " " + id, live_addrs());
        } catch (const std::exception& e) {
          r = std::string("ERR ") + e.what();
        }
        std::cout << "promote " << id << " -> " << r << "\n";
        if (r.rfind("OK", 0) == 0) {
          ok = true;
          break;
        }
        // 上一轮 ConfChange 的 Joint 未退出时 promote 会失败，短等再试
        if (r.find("busy") != std::string::npos || r.find("promote_failed") != std::string::npos ||
            r.find("not_leader") != std::string::npos || r.find("joint") != std::string::npos) {
          std::this_thread::sleep_for(std::chrono::milliseconds(150));
          continue;
        }
        break;
      }
      if (!ok) return 1;
    }
    if (!wait_promoted(id)) {
      std::cerr << "timeout waiting promote settle for " << id << "\n";
      return 1;
    }
    cur.replicas.emplace_back(id, addr);
  }

  // 2) 若当前 Leader 将被踢出，先转到仍保留的原 voter（勿转刚 promote 的节点）
  {
    auto rp = parse_region_peers(rid);
    bool leader_kept = rp.leader_id.empty();
    for (const auto& d : desired) {
      if (d == rp.leader_id) leader_kept = true;
    }
    if (!rp.leader_id.empty() && !leader_kept && !desired.empty()) {
      std::string target;
      for (const auto& d : desired) {
        if (d == rp.leader_id) continue;
        if (was_original(d)) {
          target = d;
          break;
        }
      }
      if (target.empty()) {
        for (const auto& d : desired) {
          if (d != rp.leader_id) {
            target = d;
            break;
          }
        }
      }
      if (target.empty()) target = desired[0];
      auto tr = storage_call(
          "ADMIN_TRANSFER_LEADER " + std::to_string(rid) + " " + target, live_addrs());
      std::cout << "transfer " << rp.leader_id << " -> " << target << " : " << tr << "\n";
      auto it = nodes.find(target);
      if (it != nodes.end()) {
        try {
          auto cr = skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 2000);
          std::cout << "campaign " << target << " -> " << cr << "\n";
        } catch (const std::exception& e) {
          std::cout << "campaign " << target << " FAIL " << e.what() << "\n";
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  // 3) 踢掉多余副本，并 DESTROY 防脑裂
  // 连续 REMOVE 须等 Joint 退出，否则 propose_conf_change 直接失败 → remove_failed
  std::vector<std::pair<std::string, std::string>> victims;
  for (const auto& [nid, naddr] : cur.replicas) {
    bool keep = false;
    for (const auto& d : desired)
      if (d == nid) {
        keep = true;
        break;
      }
    if (!keep) victims.emplace_back(nid, naddr);
  }
  for (const auto& [nid, naddr] : victims) {
    std::string r;
    bool ok = false;
    for (int attempt = 0; attempt < 40; ++attempt) {
      try {
        r = storage_call("ADMIN_REMOVE_PEER " + std::to_string(rid) + " " + nid, live_addrs());
      } catch (const std::exception& e) {
        r = std::string("ERR ") + e.what();
      }
      std::cout << "remove " << nid << " -> " << r << "\n";
      if (r.rfind("OK", 0) == 0) {
        ok = true;
        break;
      }
      if (r.find("busy") != std::string::npos || r.find("remove_failed") != std::string::npos ||
          r.find("not_leader") != std::string::npos || r.find("no_region") != std::string::npos) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        continue;
      }
      break;
    }
    if (!ok) return 1;
    // REMOVE 进入 Joint 后须等 LeaveJoint；若此时 DESTROY 被踢节点，旧多数派缺票会永久卡死选举
    // 询 joint 时排除 victim：它可能永远停在 joint=1（未复制到 LeaveJoint）
    bool left_joint = false;
    for (int w = 0; w < 300; ++w) {
      if (joint_status(naddr) == 0) {
        left_joint = true;
        break;
      }
      if (w > 0 && w % 15 == 0) {
        for (const auto& d : desired) {
          auto it = nodes.find(d);
          if (it == nodes.end()) continue;
          try {
            skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 1000);
          } catch (...) {
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!left_joint) {
      std::cerr << "refuse DESTROY " << nid << ": still in joint or unreachable (would brick elections)\n";
      return 1;
    }
    try {
      auto dr = skv::tcp_line(naddr, "ADMIN_DESTROY_REGION " + std::to_string(rid), 2000);
      std::cout << "destroy " << nid << " -> " << dr << "\n";
    } catch (const std::exception& e) {
      std::cout << "destroy " << nid << " FAIL " << e.what() << "\n";
    }
    // 已 DESTROY 的节点从 live 集摘掉，避免后续 REMOVE 再轮询到 no_region
    cur.replicas.erase(std::remove_if(cur.replicas.begin(), cur.replicas.end(),
                                      [&](const auto& p) { return p.first == nid; }),
                       cur.replicas.end());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  // 4) 催目标集竞选（踢出节点上的陈旧 leader_id 会让 redirect 指到 DESTROY 地址）
  for (const auto& id : desired) {
    auto it = nodes.find(id);
    if (it == nodes.end()) continue;
    try {
      auto cr = skv::tcp_line(it->second, "ADMIN_CAMPAIGN " + std::to_string(rid), 1500);
      std::cout << "post-place campaign " << id << " -> " << cr << "\n";
      if (cr.rfind("OK", 0) == 0) break;
    } catch (...) {
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  // 5) 发布 Meta 路由（数据面已成功时必须尽力写上，否则 STORE_STATUS/路由长期不一致）
  std::ostringstream peers;
  for (size_t i = 0; i < desired.size(); ++i) {
    if (i) peers << ',';
    peers << desired[i];
  }
  const std::string place_req = "PLACE_REGION " + args[0] + " " + peers.str();
  for (int attempt = 0; attempt < 8; ++attempt) {
    try {
      auto resp = meta_call(place_req);
      std::cout << resp << "\n";
      if (resp.rfind("OK PLACE", 0) == 0) return 0;
      std::cerr << "PLACE_REGION attempt " << (attempt + 1) << " -> " << resp << "\n";
    } catch (const std::exception& e) {
      std::cerr << "PLACE_REGION attempt " << (attempt + 1) << " FAIL " << e.what() << "\n";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  return 1;
}

int cmd_ping() {
  int fail = 0;
  for (const auto& a : meta_addrs()) {
    try {
      auto r = skv::tcp_line(a, "PING", 800);
      std::cout << "meta  " << a << "  " << r << "\n";
    } catch (const std::exception& e) {
      std::cout << "meta  " << a << "  FAIL " << e.what() << "\n";
      ++fail;
    }
  }
  for (const auto& a : storage_addrs()) {
    try {
      auto r = skv::tcp_line(a, "PING", 800);
      std::cout << "store " << a << "  " << r << "\n";
    } catch (const std::exception& e) {
      std::cout << "store " << a << "  FAIL " << e.what() << "\n";
      ++fail;
    }
  }
  const char* gw = std::getenv("SKV_GATEWAY");
  std::string gaddr = gw ? gw : "127.0.0.1:19000";
  std::cout << "gw    " << gaddr << "  "
            << (skv::tcp_reachable(gaddr, 400) ? "UP" : "DOWN") << "\n";
  if (!skv::tcp_reachable(gaddr, 400)) ++fail;
  return fail == 0 ? 0 : 1;
}

int cmd_route() {
  auto resp = meta_call("GET_ROUTE");
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_who_leader() {
  auto resp = meta_call("WHO_LEADER");
  std::cout << resp << "\n";
  return resp.rfind("OK", 0) == 0 ? 0 : 1;
}

int cmd_smoke() {
  const std::string key = "skv-smoke";
  const std::string val = "ok";
  if (cmd_put({key, val}) != 0) return 1;
  if (cmd_get({key}) != 0) return 1;
  std::cout << "SMOKE OK\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  CLI::App app{"skv — OpsKV ops CLI (C++)"};
  app.require_subcommand(1);
  app.footer(
      "Env: SKV_ROOT SKV_STORAGES SKV_META SKV_GATEWAY\n"
      "     SKV_SPLIT_KEYS / SKV_SPLIT_BYTES / SKV_MERGE_KEYS");

  int rc = 0;
  auto run = [&](auto fn) {
    return [fn, &rc]() {
      try {
        rc = fn();
      } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        rc = 1;
      }
    };
  };

  app.add_subcommand("status", "Show cluster process status")
      ->callback(run([] { return skv::cmd_status(skv::find_root()); }));

  bool start_fresh = false;
  auto* start = app.add_subcommand("start", "Start local cluster");
  start->add_flag("--fresh", start_fresh, "Wipe data before start");
  start->callback(run([&] { return skv::cmd_start(skv::find_root(), start_fresh); }));

  app.add_subcommand("stop", "Stop local cluster")
      ->callback(run([] { return skv::cmd_stop(skv::find_root()); }));

  std::string restart_name;
  auto* restart = app.add_subcommand("restart", "Restart one process");
  restart->add_option("name", restart_name, "meta-N|storage-N|gateway")->required();
  restart->callback(run([&] { return skv::cmd_restart(skv::find_root(), restart_name); }));

  std::string kill_name;
  auto* kill = app.add_subcommand("kill", "SIGKILL one process");
  kill->add_option("name", kill_name)->required();
  kill->callback(run([&] { return skv::cmd_kill(skv::find_root(), kill_name); }));

  std::string logs_name;
  int logs_n = 80;
  bool logs_f = false;
  auto* logs = app.add_subcommand("logs", "Tail process log");
  logs->add_option("name", logs_name)->required();
  logs->add_option("-n", logs_n, "Lines");
  logs->add_flag("-f,--follow", logs_f);
  logs->callback(run([&] { return skv::cmd_logs(skv::find_root(), logs_name, logs_n, logs_f); }));

  std::string put_key, put_val;
  auto* c_put = app.add_subcommand("put", "PUT <key> <value>");
  c_put->add_option("key", put_key)->required();
  c_put->add_option("value", put_val)->required();
  c_put->callback(run([&] { return cmd_put({put_key, put_val}); }));

  std::string get_key;
  auto* c_get = app.add_subcommand("get", "GET <key>");
  c_get->add_option("key", get_key)->required();
  c_get->callback(run([&] { return cmd_get({get_key}); }));

  std::string del_key;
  auto add_del = [&](const char* name) {
    auto* c = app.add_subcommand(name, "DEL <key>");
    c->add_option("key", del_key)->required();
    c->callback(run([&] { return cmd_del({del_key}); }));
  };
  add_del("del");
  add_del("delete");

  std::string locate_key;
  auto* c_locate = app.add_subcommand("locate", "LOCATE <key>");
  c_locate->add_option("key", locate_key)->required();
  c_locate->callback(run([&] { return cmd_locate({locate_key}); }));

  std::string split_rid, split_key;
  auto* c_split = app.add_subcommand("split", "SPLIT region at key (meta-only; no place)");
  c_split->add_option("region_id", split_rid)->required();
  c_split->add_option("split_key", split_key)->required();
  c_split->callback(run([&] { return cmd_split({split_rid, split_key}); }));

  std::string left_id, right_id;
  auto* c_merge = app.add_subcommand("merge", "MERGE left right");
  c_merge->add_option("left_id", left_id)->required();
  c_merge->add_option("right_id", right_id)->required();
  c_merge->callback(run([&] { return cmd_merge({left_id, right_id}); }));

  std::string place_rid, peers_csv;
  auto* c_place = app.add_subcommand("place", "PLACE region on nodes");
  c_place->add_option("region_id", place_rid)->required();
  c_place->add_option("peers", peers_csv, "node1,node2,...")->required();
  c_place->callback(run([&] { return cmd_place({place_rid, peers_csv}); }));

  std::string nodes_role = "storage";
  auto* c_nodes = app.add_subcommand("nodes", "LIST_NODES");
  c_nodes->add_option("role", nodes_role);
  c_nodes->callback(run([&] { return cmd_nodes({nodes_role}); }));

  std::string join_id;
  auto* c_join = app.add_subcommand("join", "Join pending store (Joining→Active)");
  c_join->add_option("node_id", join_id)->required();
  c_join->callback(run([&] { return cmd_join({join_id}); }));

  std::string scale_in_id;
  auto* c_si = app.add_subcommand("scale-in", "store delete → Draining");
  c_si->add_option("node_id", scale_in_id)->required();
  c_si->callback(run([&] { return cmd_scale_in({scale_in_id}); }));

  std::string sis_id;
  auto* c_sis = app.add_subcommand("scale-in-status", "STORE_STATUS while draining");
  c_sis->add_option("node_id", sis_id)->required();
  c_sis->callback(run([&] { return cmd_scale_in_status({sis_id}); }));

  std::string rt_id;
  auto* c_rt = app.add_subcommand("remove-tombstone", "Erase Tombstone store metadata");
  c_rt->add_option("node_id", rt_id)->required();
  c_rt->callback(run([&] { return cmd_remove_tombstone({rt_id}); }));

  std::string bal_n;
  auto* c_bal = app.add_subcommand("balance", "Enqueue RF-constant balance moves");
  c_bal->add_option("count", bal_n, "max moves (default 1)");
  c_bal->callback(run([&] {
    return bal_n.empty() ? cmd_balance({}) : cmd_balance({bal_n});
  }));

  app.add_subcommand("ping", "Ping meta/storage/gateway")->callback(run([] { return cmd_ping(); }));
  app.add_subcommand("route", "GET_ROUTE")->callback(run([] { return cmd_route(); }));
  app.add_subcommand("who-leader", "WHO_LEADER")->callback(run([] { return cmd_who_leader(); }));
  app.add_subcommand("leader", "alias of who-leader")->callback(run([] { return cmd_who_leader(); }));
  app.add_subcommand("smoke", "put+get smoke")->callback(run([] { return cmd_smoke(); }));

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }
  return rc;
}
