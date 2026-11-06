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

// @file coprocessor.cpp

#include "opskv/storage/coprocessor.hpp"

namespace opskv::storage {

CoprocessorResponse Coprocessor::execute(const CoprocessorRequest& req,
                                         const opskv::KeyRange& region_range) const {
  CoprocessorResponse out;
  if (!eng_) return out;
  const uint32_t lim = req.limit == 0 ? 256 : req.limit;
  eng_->for_each([&](std::string_view key, const KvRecord& rec) {
    if (!region_range.contains(key)) return true;
    if (!req.start_key.empty() && key < req.start_key) return true;
    if (!req.end_key.empty() && key >= req.end_key) return false;
    if (key.rfind("\xff\xffskv/", 0) == 0) return true;
    if (!req.key_prefix.empty() && key.rfind(req.key_prefix, 0) != 0) return true;
    if (!req.value_prefix.empty() && rec.value.rfind(req.value_prefix, 0) != 0) return true;
    ++out.matched;
    if (!req.count_only) {
      CoprocessorPair p;
      p.key = std::string(key);
      p.revision = rec.revision;
      if (!req.keys_only) p.value = rec.value;
      out.pairs.push_back(std::move(p));
    }
    if (out.matched >= lim) {
      out.truncated = true;
      return false;
    }
    return true;
  });
  return out;
}

}  // namespace opskv::storage
