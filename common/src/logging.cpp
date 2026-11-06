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

// @file logging.cpp
// spdlog 后端：保持 SKV_* << 调用方式不变。

#include "opskv/logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>

namespace opskv {
namespace {

std::atomic<LogLevel> g_level{LogLevel::kInfo};
std::once_flag g_init;

spdlog::level::level_enum to_spd(LogLevel l) {
  switch (l) {
    case LogLevel::kDebug:
      return spdlog::level::debug;
    case LogLevel::kInfo:
      return spdlog::level::info;
    case LogLevel::kWarn:
      return spdlog::level::warn;
    case LogLevel::kError:
      return spdlog::level::err;
  }
  return spdlog::level::info;
}

}  // namespace

void init_logging() {
  std::call_once(g_init, [] {
    auto sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    auto logger = std::make_shared<spdlog::logger>("skv", sink);
    logger->set_pattern("%Y-%m-%d %H:%M:%S.%e [%^%l%$] %v");
    logger->set_level(to_spd(g_level.load()));
    spdlog::set_default_logger(logger);
    spdlog::flush_on(spdlog::level::warn);
  });
}

void set_log_level(LogLevel level) {
  g_level.store(level);
  init_logging();
  spdlog::set_level(to_spd(level));
}

LogLevel log_level() { return g_level.load(); }

LogMessage::LogMessage(LogLevel level, const char* file, int line)
    : level_(level), file_(file), line_(line) {
  init_logging();
}

LogMessage::~LogMessage() {
  if (level_ < g_level.load()) return;
  // 附带源位置，便于对照旧格式
  const std::string msg = ss_.str();
  const char* base = file_;
  if (const char* slash = std::strrchr(file_, '/')) base = slash + 1;
#ifdef _WIN32
  if (const char* bslash = std::strrchr(file_, '\\')) {
    if (bslash + 1 > base) base = bslash + 1;
  }
#endif
  switch (level_) {
    case LogLevel::kDebug:
      spdlog::debug("{}:{} {}", base, line_, msg);
      break;
    case LogLevel::kInfo:
      spdlog::info("{}:{} {}", base, line_, msg);
      break;
    case LogLevel::kWarn:
      spdlog::warn("{}:{} {}", base, line_, msg);
      break;
    case LogLevel::kError:
      spdlog::error("{}:{} {}", base, line_, msg);
      break;
  }
}

}  // namespace opskv
