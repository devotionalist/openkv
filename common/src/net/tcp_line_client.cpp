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

#include "opskv/net/tcp_line_client.hpp"

namespace opskv::net {

bool parse_host_port(std::string_view addr, std::string* host, uint16_t* port) {
  if (!host || !port) return false;
  auto pos = addr.rfind(':');
  if (pos == std::string_view::npos) return false;
  *host = std::string(addr.substr(0, pos));
  try {
    *port = static_cast<uint16_t>(std::stoi(std::string(addr.substr(pos + 1))));
  } catch (...) {
    return false;
  }
  return true;
}

}  // namespace opskv::net
