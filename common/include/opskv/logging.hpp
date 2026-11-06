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

// @file logging.hpp
// SeaStarKV 公共日志：spdlog 后端；保留 SKV_* + << 链式宏兼容旧调用点。

#pragma once

#include <sstream>
#include <string_view>

namespace opskv {

enum class LogLevel { kDebug, kInfo, kWarn, kError };

void set_log_level(LogLevel level);
LogLevel log_level();

/// 确保默认 logger 已初始化（幂等；进程入口可显式调用）。
void init_logging();

/// RAII：流式拼接，析构时写入 spdlog。
class LogMessage {
 public:
  LogMessage(LogLevel level, const char* file, int line);
  ~LogMessage();

  template <typename T>
  LogMessage& operator<<(const T& v) {
    ss_ << v;
    return *this;
  }

 private:
  LogLevel level_;
  const char* file_;
  int line_;
  std::ostringstream ss_;
};

}  // namespace opskv

#define SKV_LOG(level) \
  ::opskv::LogMessage(::opskv::LogLevel::level, __FILE__, __LINE__)
#define SKV_INFO SKV_LOG(kInfo)
#define SKV_WARN SKV_LOG(kWarn)
#define SKV_ERROR SKV_LOG(kError)
#define SKV_DEBUG SKV_LOG(kDebug)
