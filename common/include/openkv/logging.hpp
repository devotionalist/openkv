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

namespace openkv {

/* 日志级别；由 set_log_level 过滤低于阈值的条目。 */
enum class LogLevel {
  kDebug,  // 调试细节
  kInfo,   // 常规运行信息
  kWarn,   // 可恢复异常 / 降级
  kError,  // 失败路径
};

/**
 * 设置进程级最低输出级别。
 * @param level 新阈值
 */
void set_log_level(LogLevel level);
/**
 * 当前最低输出级别。
 * @return 当前阈值
 */
LogLevel log_level();

/**
 * 确保默认 logger 已初始化（幂等；进程入口可显式调用）。
 */
void init_logging();

/* RAII：流式拼接，析构时写入 spdlog。 */
class LogMessage {
 public:
  /**
   * 构造一条待输出日志；通常经 SKV_* 宏传入 __FILE__/__LINE__。
   * @param level 本条级别
   * @param file 源文件
   * @param line 源行号
   */
  LogMessage(LogLevel level, const char* file, int line);
  ~LogMessage();

  /**
   * 追加一段可流式输出的值。
   * @param v 任意支持 operator<< 的值
   * @return *this，便于链式
   */
  template <typename T>
  LogMessage& operator<<(const T& v) {
    ss_ << v;
    return *this;
  }

 private:
  LogLevel level_;         // 本条级别
  const char* file_;       // 源文件
  int line_;               // 源行号
  std::ostringstream ss_;  // 拼接缓冲
};

}  // namespace openkv

#define SKV_LOG(level) ::openkv::LogMessage(::openkv::LogLevel::level, __FILE__, __LINE__)
#define SKV_INFO SKV_LOG(kInfo)
#define SKV_WARN SKV_LOG(kWarn)
#define SKV_ERROR SKV_LOG(kError)
#define SKV_DEBUG SKV_LOG(kDebug)
