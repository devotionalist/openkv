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

// Package lifecycle：本地 .run 集群节点上下线（对齐 skv restart/kill）。
package lifecycle

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Component struct {
	Name string
	Addr string
	Conf string // relative to root；gateway 为空
	Role string // meta|storage|gateway
}

func DefaultComponents() []Component {
	return []Component{
		{"meta-0", "127.0.0.1:8500", "conf/bootstrap.conf", "meta"},
		{"meta-1", "127.0.0.1:8501", "conf/meta-1.conf", "meta"},
		{"meta-2", "127.0.0.1:8502", "conf/meta-2.conf", "meta"},
		{"storage-0", "127.0.0.1:9100", "conf/storage.conf", "storage"},
		{"storage-1", "127.0.0.1:9101", "conf/storage-1.conf", "storage"},
		{"storage-2", "127.0.0.1:9102", "conf/storage-2.conf", "storage"},
		{"storage-3", "127.0.0.1:9103", "conf/storage-3.conf", "storage"},
		{"storage-4", "127.0.0.1:9104", "conf/storage-4.conf", "storage"},
		{"storage-5", "127.0.0.1:9105", "conf/storage-5.conf", "storage"},
		{"gateway", "127.0.0.1:19000", "", "gateway"},
	}
}

func Find(name string) *Component {
	for _, c := range DefaultComponents() {
		if c.Name == name {
			cc := c
			return &cc
		}
	}
	return nil
}

type Status struct {
	Name  string   `json:"name"`
	Role  string   `json:"role"`
	Addr  string   `json:"addr"`
	PID   int      `json:"pid"`
	Alive bool     `json:"alive"`
	Port  bool     `json:"port"`
	Up    bool     `json:"up"`
	Disk  DiskInfo `json:"disk,omitempty"`
}

func StatusOf(root, runDir, name string) Status {
	c := Find(name)
	st := Status{Name: name}
	if c != nil {
		st.Role, st.Addr = c.Role, c.Addr
		if c.Role == "meta" || c.Role == "storage" {
			st.Disk = ProbeDisk(DataDirFor(root, *c))
		}
	}
	pid := readPID(runDir, name)
	st.PID = pid
	st.Alive = pid > 0 && processAlive(pid)
	if c != nil {
		st.Port = tcpUp(c.Addr, 250)
	}
	st.Up = st.Alive && st.Port
	return st
}

func ListStatus(root, runDir string) []Status {
	out := make([]Status, 0, len(DefaultComponents()))
	for _, c := range DefaultComponents() {
		out = append(out, StatusOf(root, runDir, c.Name))
	}
	return out
}

// shortNameFromAddr：meta-0.meta-headless.ns.svc → meta-0；gateway.ns.svc → gateway。
func shortNameFromAddr(addr, role string, i int) string {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "" {
		if role == "gateway" {
			return "gateway"
		}
		return fmt.Sprintf("%s-%d", role, i)
	}
	short := strings.Split(host, ".")[0]
	if short != "" {
		return short
	}
	if role == "gateway" {
		return "gateway"
	}
	return fmt.Sprintf("%s-%d", role, i)
}

// ComponentsFromAddrs 按 Web 启动参数生成拓扑目录（K8s / 远程，无本机 .run）。
func ComponentsFromAddrs(metas, storages []string, gateway string) []Component {
	out := make([]Component, 0, len(metas)+len(storages)+1)
	for i, a := range metas {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		out = append(out, Component{Name: shortNameFromAddr(a, "meta", i), Addr: a, Role: "meta"})
	}
	for i, a := range storages {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		out = append(out, Component{Name: shortNameFromAddr(a, "storage", i), Addr: a, Role: "storage"})
	}
	gateway = strings.TrimSpace(gateway)
	if gateway != "" {
		out = append(out, Component{Name: shortNameFromAddr(gateway, "gateway", 0), Addr: gateway, Role: "gateway"})
	}
	return out
}

// ListStatusRemote：无 pid；Up = TCP 可达（探测配置的 advertise 地址）。
func ListStatusRemote(comps []Component) []Status {
	out := make([]Status, 0, len(comps))
	for _, c := range comps {
		st := Status{Name: c.Name, Role: c.Role, Addr: c.Addr}
		st.Port = tcpUp(c.Addr, 400)
		st.Alive = st.Port
		st.Up = st.Port
		out = append(out, st)
	}
	return out
}

func Stop(root, runDir, name string, force bool) error {
	if Find(name) == nil {
		return fmt.Errorf("unknown component: %s", name)
	}
	pid := readPID(runDir, name)
	if pid <= 0 {
		_ = os.Remove(filepath.Join(runDir, name+".pid"))
		return nil
	}
	if !processAlive(pid) {
		_ = os.Remove(filepath.Join(runDir, name+".pid"))
		return nil
	}
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	_ = syscall.Kill(pid, sig)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if processAlive(pid) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		time.Sleep(200 * time.Millisecond)
	}
	_ = os.Remove(filepath.Join(runDir, name+".pid"))
	return nil
}

func Start(root, runDir, name string) error {
	c := Find(name)
	if c == nil {
		return fmt.Errorf("unknown component: %s", name)
	}
	// 已在线则跳过
	if st := StatusOf(root, runDir, name); st.Up {
		return nil
	}
	_ = Stop(root, runDir, name, true)

	var argv []string
	switch c.Role {
	case "meta":
		bin := filepath.Join(root, "build", "meta", "meta_server")
		if !fileExists(bin) {
			return fmt.Errorf("missing binary: %s", bin)
		}
		argv = []string{bin, filepath.Join(root, c.Conf)}
	case "storage":
		bin := filepath.Join(root, "build", "storage", "storage_node")
		if !fileExists(bin) {
			return fmt.Errorf("missing binary: %s", bin)
		}
		argv = []string{bin, filepath.Join(root, c.Conf)}
	case "gateway":
		bin := filepath.Join(runDir, "compute-gateway")
		if !fileExists(bin) {
			alt := filepath.Join(root, ".run", "compute-gateway")
			if fileExists(alt) {
				bin = alt
			} else {
				return fmt.Errorf("missing gateway binary: %s", bin)
			}
		}
		argv = []string{
			bin, "-listen", "0.0.0.0:19000",
			"-stats-listen", "127.0.0.1:19001",
			"-storages", "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105",
			"-metas", "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502",
		}
	default:
		return fmt.Errorf("bad role")
	}

	if err := spawn(root, runDir, name, argv); err != nil {
		return err
	}
	if !waitPort(c.Addr, 30*time.Second) {
		return fmt.Errorf("%s started but %s not ready", name, c.Addr)
	}
	return nil
}

func Restart(root, runDir, name string) error {
	if err := Stop(root, runDir, name, true); err != nil {
		return err
	}
	return Start(root, runDir, name)
}

func ResolveRoot(runDir, explicit string) string {
	if explicit != "" {
		return explicit
	}
	if e := os.Getenv("SKV_ROOT"); e != "" {
		if fileExists(filepath.Join(e, "conf", "bootstrap.conf")) {
			return e
		}
	}
	if runDir != "" {
		parent := filepath.Dir(runDir)
		if fileExists(filepath.Join(parent, "conf", "bootstrap.conf")) {
			return parent
		}
	}
	cwd, _ := os.Getwd()
	cur := cwd
	for i := 0; i < 8; i++ {
		if fileExists(filepath.Join(cur, "conf", "bootstrap.conf")) {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return cwd
}

func readPID(runDir, name string) int {
	b, err := os.ReadFile(filepath.Join(runDir, name+".pid"))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func tcpUp(addr string, ms int) bool {
	c, err := net.DialTimeout("tcp", addr, time.Duration(ms)*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func waitPort(addr string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if tcpUp(addr, 200) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func spawn(root, runDir, name string, argv []string) error {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}
	logPath := filepath.Join(runDir, name+".log")
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = root
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return err
	}
	pid := cmd.Process.Pid
	// 故意不 Wait：后台守护；关闭我们持有的 log fd（子进程已 dup）
	_ = logf.Close()
	go func() { _ = cmd.Wait() }()
	return os.WriteFile(filepath.Join(runDir, name+".pid"), []byte(strconv.Itoa(pid)), 0o644)
}
