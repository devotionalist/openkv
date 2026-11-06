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

// @file store_meta.cpp

#include "opskv/raftstore/store_meta.hpp"

namespace opskv::raftstore {

void StoreMeta::upsert(RegionLocalState st) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_[st.region_id] = std::move(st);
}

void StoreMeta::erase(uint32_t region_id) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_.erase(region_id);
}

std::optional<RegionLocalState> StoreMeta::get(uint32_t region_id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) return std::nullopt;
  return it->second;
}

void StoreMeta::set_leader(uint32_t region_id, std::string leader) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_[region_id].region_id = region_id;
  regions_[region_id].leader_id = std::move(leader);
}

void StoreMeta::set_epoch(uint32_t region_id, uint64_t epoch) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_[region_id].region_id = region_id;
  regions_[region_id].epoch = epoch;
}

void StoreMeta::set_range(uint32_t region_id, opskv::KeyRange range) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_[region_id].region_id = region_id;
  regions_[region_id].range = std::move(range);
}

std::vector<uint32_t> StoreMeta::all_regions() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  out.reserve(regions_.size());
  for (const auto& [id, st] : regions_) {
    if (!st.destroyed) out.push_back(id);
  }
  return out;
}

std::vector<uint32_t> StoreMeta::leader_regions(const std::string& self_id) const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<uint32_t> out;
  for (const auto& [id, st] : regions_) {
    if (!st.destroyed && st.leader_id == self_id) out.push_back(id);
  }
  return out;
}

}  // namespace opskv::raftstore
