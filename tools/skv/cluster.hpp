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

/* 本地进程组件（meta / storage / gateway 等）的名字、探活地址与配置路径。 */
struct Component {
  std::string name;  // 组件名（如 meta0、storage1）
  std::string addr;  // 探活 host:port
  std::string conf;  // 相对 root 的配置路径；gateway 可为空
};

/**
 * 向上查找含 Makefile / conf 的仓库根目录。
 * @return 绝对路径；找不到则空串或抛错（实现侧约定）
 */
std::string find_root();

/**
 * 默认本地拓扑（开发用）。
 * @return 组件列表只读引用
 */
const std::vector<Component>& default_components();

/**
 * 打印各组件是否可达。
 * @param root 仓库根
 * @return 进程退出码（0=全好）
 */
int cmd_status(const std::string& root);

/**
 * 按默认拓扑拉起本地进程。
 * @param root 仓库根
 * @param fresh true 时清数据再起
 * @return 进程退出码
 */
int cmd_start(const std::string& root, bool fresh);

/**
 * 停掉本地集群进程。
 * @param root 仓库根
 * @return 进程退出码
 */
int cmd_stop(const std::string& root);

/**
 * 重启单个组件。
 * @param root 仓库根
 * @param name 组件名
 * @return 进程退出码
 */
int cmd_restart(const std::string& root, const std::string& name);

/**
 * 强制杀单个组件进程。
 * @param root 仓库根
 * @param name 组件名
 * @return 进程退出码
 */
int cmd_kill(const std::string& root, const std::string& name);

/**
 * 看组件日志。
 * @param root 仓库根
 * @param name 组件名
 * @param lines 尾部行数
 * @param follow 是否跟随
 * @return 进程退出码
 */
int cmd_logs(const std::string& root, const std::string& name, int lines, bool follow);

}  // namespace skv
