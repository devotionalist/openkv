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

// @file cluster.hpp
// 本地集群生命周期：status / start / stop / restart / kill。
#pragma once

#include <string>
#include <vector>

namespace skv {

struct Component {
  std::string name;
  std::string addr;   // host:port for health
  std::string conf;   // relative to root, empty for gateway
};

std::string find_root();
const std::vector<Component>& default_components();

int cmd_status(const std::string& root);
int cmd_start(const std::string& root, bool fresh);
int cmd_stop(const std::string& root);
int cmd_restart(const std::string& root, const std::string& name);
int cmd_kill(const std::string& root, const std::string& name);
int cmd_logs(const std::string& root, const std::string& name, int lines, bool follow);

}  // namespace skv
