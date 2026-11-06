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

// @file cluster.cpp
#include "cluster.hpp"

#include "tcp_line.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

namespace skv {
namespace {

fs::path run_dir(const std::string& root) { return fs::path(root) / ".run"; }

std::optional<pid_t> read_pid(const std::string& root, const std::string& name) {
  auto f = run_dir(root) / (name + ".pid");
  std::ifstream in(f);
  if (!in) return std::nullopt;
  pid_t pid = 0;
  in >> pid;
  if (!in || pid <= 0) return std::nullopt;
  return pid;
}

bool alive(pid_t pid) { return pid > 0 && ::kill(pid, 0) == 0; }

void write_pid(const std::string& root, const std::string& name, pid_t pid) {
  fs::create_directories(run_dir(root));
  std::ofstream out(run_dir(root) / (name + ".pid"));
  out << pid;
}

void stop_pid(const std::string& root, const std::string& name, bool force) {
  auto pid = read_pid(root, name);
  if (!pid) {
    std::cout << name << ": no pid\n";
    return;
  }
  if (!alive(*pid)) {
    std::cout << name << ": stale pid=" << *pid << "\n";
    fs::remove(run_dir(root) / (name + ".pid"));
    return;
  }
  std::cout << name << ": stop pid=" << *pid << (force ? " (KILL)" : "") << "\n";
  ::kill(*pid, force ? SIGKILL : SIGTERM);
  for (int i = 0; i < 30; ++i) {
    if (!alive(*pid)) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (alive(*pid)) ::kill(*pid, SIGKILL);
  fs::remove(run_dir(root) / (name + ".pid"));
}

pid_t spawn_logged(const std::string& root, const std::string& name,
                   const std::vector<std::string>& argv) {
  fs::create_directories(run_dir(root));
  auto log_path = run_dir(root) / (name + ".log");
  int logfd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (logfd < 0) throw std::runtime_error("open log " + log_path.string());

  pid_t pid = ::fork();
  if (pid < 0) {
    ::close(logfd);
    throw std::runtime_error("fork failed");
  }
  if (pid == 0) {
    ::setsid();
    ::dup2(logfd, STDOUT_FILENO);
    ::dup2(logfd, STDERR_FILENO);
    ::close(logfd);
    ::chdir(root.c_str());
    std::vector<std::string> copy = argv;
    std::vector<char*> av;
    av.reserve(copy.size() + 1);
    for (auto& s : copy) av.push_back(s.data());
    av.push_back(nullptr);
    ::execv(av[0], av.data());
    std::perror("execv");
    _exit(127);
  }
  ::close(logfd);
  write_pid(root, name, pid);
  return pid;
}

bool wait_port(std::string_view addr, int sec) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(sec);
  while (std::chrono::steady_clock::now() < deadline) {
    if (tcp_reachable(addr, 200)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  return false;
}

const Component* find_comp(const std::string& name) {
  for (const auto& c : default_components()) {
    if (c.name == name) return &c;
  }
  return nullptr;
}

}  // namespace

std::string find_root() {
  if (const char* e = std::getenv("SKV_ROOT")) {
    if (fs::exists(fs::path(e) / "conf" / "bootstrap.conf")) return e;
  }
  fs::path cur = fs::current_path();
  for (int i = 0; i < 8; ++i) {
    if (fs::exists(cur / "conf" / "bootstrap.conf")) return cur.string();
    if (!cur.has_parent_path() || cur == cur.parent_path()) break;
    cur = cur.parent_path();
  }
  // binary next to build/tools/skv → ../../
  char buf[4096];
  ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  (void)n;
#if defined(__APPLE__)
  // fall through: cwd already tried
#endif
  throw std::runtime_error("cannot find SKV_ROOT (set env or cd to repo)");
}

const std::vector<Component>& default_components() {
  static const std::vector<Component> k = {
      {"meta-0", "127.0.0.1:8500", "conf/bootstrap.conf"},
      {"meta-1", "127.0.0.1:8501", "conf/meta-1.conf"},
      {"meta-2", "127.0.0.1:8502", "conf/meta-2.conf"},
      {"storage-0", "127.0.0.1:9100", "conf/storage.conf"},
      {"storage-1", "127.0.0.1:9101", "conf/storage-1.conf"},
      {"storage-2", "127.0.0.1:9102", "conf/storage-2.conf"},
      {"storage-3", "127.0.0.1:9103", "conf/storage-3.conf"},
      {"storage-4", "127.0.0.1:9104", "conf/storage-4.conf"},
      {"storage-5", "127.0.0.1:9105", "conf/storage-5.conf"},
      {"gateway", "127.0.0.1:19000", ""},
  };
  return k;
}

int cmd_status(const std::string& root) {
  std::cout << "root=" << root << "\n";
  int up = 0, down = 0;
  for (const auto& c : default_components()) {
    auto pid = read_pid(root, c.name);
    bool proc = pid && alive(*pid);
    bool port = tcp_reachable(c.addr, 250);
    const char* st = (proc && port) ? "UP" : (port ? "PORT_ONLY" : (proc ? "NO_PORT" : "DOWN"));
    if (std::string(st) == "UP") ++up;
    else ++down;
    std::cout << "  " << c.name << "\t" << st << "\t" << c.addr;
    if (pid) std::cout << "\tpid=" << *pid;
    std::cout << "\n";
  }
  std::cout << "summary: up=" << up << " down=" << down << "\n";
  return down == 0 ? 0 : 1;
}

int cmd_start(const std::string& root, bool fresh) {
  if (fresh) {
    std::cout << "fresh: remove data/\n";
    fs::remove_all(fs::path(root) / "data");
  }
  auto bin = fs::path(root) / "build";
  auto meta = bin / "meta" / "meta_server";
  auto stor = bin / "storage" / "storage_node";
  auto gw = run_dir(root) / "compute-gateway";
  if (!fs::exists(meta) || !fs::exists(stor)) {
    std::cerr << "missing binaries; build first:\n"
              << "  cmake --build build -j --target meta_server storage_node\n";
    return 1;
  }
  if (!fs::exists(gw)) {
    std::cerr << "missing gateway; build:\n"
              << "  (cd client/go && go build -o " << gw << " ./cmd/compute-gateway)\n";
    return 1;
  }

  // stop leftovers
  for (const auto& c : default_components()) stop_pid(root, c.name, true);

  spawn_logged(root, "meta-0", {meta.string(), (fs::path(root) / "conf/bootstrap.conf").string()});
  spawn_logged(root, "meta-1", {meta.string(), (fs::path(root) / "conf/meta-1.conf").string()});
  spawn_logged(root, "meta-2", {meta.string(), (fs::path(root) / "conf/meta-2.conf").string()});
  spawn_logged(root, "storage-0",
               {stor.string(), (fs::path(root) / "conf/storage.conf").string()});
  spawn_logged(root, "storage-1",
               {stor.string(), (fs::path(root) / "conf/storage-1.conf").string()});
  spawn_logged(root, "storage-2",
               {stor.string(), (fs::path(root) / "conf/storage-2.conf").string()});
  spawn_logged(root, "storage-3",
               {stor.string(), (fs::path(root) / "conf/storage-3.conf").string()});
  spawn_logged(root, "storage-4",
               {stor.string(), (fs::path(root) / "conf/storage-4.conf").string()});
  spawn_logged(root, "storage-5",
               {stor.string(), (fs::path(root) / "conf/storage-5.conf").string()});

  for (auto port : {"127.0.0.1:8500", "127.0.0.1:8501", "127.0.0.1:8502", "127.0.0.1:9100",
                    "127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103", "127.0.0.1:9104",
                    "127.0.0.1:9105"}) {
    if (!wait_port(port, 30)) {
      std::cerr << "timeout waiting " << port << "\n";
      return 1;
    }
  }

  const char* stores =
      "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,"
      "127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105";
  spawn_logged(root, "gateway",
               {gw.string(), "-listen", "0.0.0.0:19000", "-storages", stores, "-metas",
                "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502"});
  if (!wait_port("127.0.0.1:19000", 25)) {
    std::cerr << "gateway not ready\n";
    return 1;
  }
  std::cout << "cluster started\n";
  return cmd_status(root);
}

int cmd_stop(const std::string& root) {
  // reverse order
  const char* order[] = {"gateway",    "storage-0", "storage-1", "storage-2",
                         "storage-3",  "storage-4", "storage-5", "meta-0",
                         "meta-1",     "meta-2",    "supervisor"};
  for (auto* n : order) stop_pid(root, n, false);
  std::cout << "cluster stopped\n";
  return 0;
}

int cmd_kill(const std::string& root, const std::string& name) {
  if (!find_comp(name) && name != "supervisor") {
    std::cerr << "unknown component: " << name << "\n";
    return 1;
  }
  stop_pid(root, name, true);
  return 0;
}

int cmd_restart(const std::string& root, const std::string& name) {
  const auto* c = find_comp(name);
  if (!c) {
    std::cerr << "unknown component: " << name << "\n";
    return 1;
  }
  stop_pid(root, name, true);
  auto bin = fs::path(root) / "build";
  if (name.rfind("meta-", 0) == 0) {
    auto meta = bin / "meta" / "meta_server";
    spawn_logged(root, name, {meta.string(), (fs::path(root) / c->conf).string()});
  } else if (name.rfind("storage-", 0) == 0) {
    auto stor = bin / "storage" / "storage_node";
    spawn_logged(root, name, {stor.string(), (fs::path(root) / c->conf).string()});
  } else if (name == "gateway") {
    auto gw = run_dir(root) / "compute-gateway";
    spawn_logged(root, "gateway",
                 {gw.string(), "-listen", "0.0.0.0:19000", "-storages",
                  "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,"
                  "127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105",
                  "-metas", "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502"});
  }
  if (!wait_port(c->addr, 30)) {
    std::cerr << "restart failed: " << c->addr << " not up\n";
    return 1;
  }
  std::cout << name << " restarted\n";
  return 0;
}

int cmd_logs(const std::string& root, const std::string& name, int lines, bool follow) {
  auto path = run_dir(root) / (name + ".log");
  if (!fs::exists(path)) {
    std::cerr << "no log: " << path << "\n";
    return 1;
  }
  // use tail via execlp for -f / -n
  pid_t pid = ::fork();
  if (pid == 0) {
    std::string n = std::to_string(lines);
    if (follow) {
      execlp("tail", "tail", "-n", n.c_str(), "-f", path.c_str(), static_cast<char*>(nullptr));
    } else {
      execlp("tail", "tail", "-n", n.c_str(), path.c_str(), static_cast<char*>(nullptr));
    }
    _exit(127);
  }
  int st = 0;
  ::waitpid(pid, &st, 0);
  return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
}

}  // namespace skv
