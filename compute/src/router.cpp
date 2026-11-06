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

// @file router.cpp
// Router：Range 定位 + Leader 选取。

#include "opskv/compute/router.hpp"

namespace opskv::compute {
namespace {

void rebuild_sorted(const RoutingTable& table, std::vector<opskv::RegionMeta>* sorted) {
  sorted->clear();
  sorted->reserve(table.regions.size());
  for (const auto& [id, rr] : table.regions) {
    sorted->push_back({id, rr.range});
  }
  opskv::sort_regions(*sorted);
}

}  // namespace

void Router::update(RoutingTable table) {
  std::lock_guard<std::mutex> lock(mu_);
  if (table.version < table_.version) return;
  table_ = std::move(table);
  rebuild_sorted(table_, &sorted_);
}

uint64_t Router::version() const {
  std::lock_guard<std::mutex> lock(mu_);
  return table_.version;
}

std::optional<opskv::RegionId> Router::region_of(std::string_view key) const {
  std::lock_guard<std::mutex> lock(mu_);
  return opskv::locate_region(sorted_, key);
}

uint32_t Router::shard_of(std::string_view key) const {
  auto id = region_of(key);
  return id ? *id : 0;
}

std::optional<std::string> Router::leader_addr(opskv::RegionId region_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = table_.regions.find(region_id);
  if (it == table_.regions.end()) return std::nullopt;
  for (const auto& r : it->second.replicas) {
    if (r.is_leader) return r.address;
  }
  if (!it->second.replicas.empty()) return it->second.replicas.front().address;
  return std::nullopt;
}

std::optional<std::string> Router::leader_addr_for_key(std::string_view key) const {
  auto id = region_of(key);
  if (!id) return std::nullopt;
  return leader_addr(*id);
}

std::vector<std::string> Router::replica_addrs(opskv::RegionId region_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> out;
  auto it = table_.regions.find(region_id);
  if (it == table_.regions.end()) return out;
  for (const auto& r : it->second.replicas) out.push_back(r.address);
  return out;
}

void Router::bootstrap_single(const std::string& storage_addr, uint32_t) {
  RoutingTable t;
  t.version = 1;
  t.shard_count = 1;
  RegionRoute rr;
  rr.region_id = 0;
  rr.range = {};
  rr.replicas.push_back({"storage-0", storage_addr, true});
  t.regions.emplace(0, std::move(rr));
  update(std::move(t));
}

}  // namespace opskv::compute
