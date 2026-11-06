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

package lifecycle

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DiskInfo 节点所在主机分区容量 + 本节点数据目录占用。
type DiskInfo struct {
	Path       string `json:"path"`        // 用于探测的路径（data_dir）
	TotalBytes uint64 `json:"total_bytes"` // 主机分区总容量
	UsedBytes  uint64 `json:"used_bytes"`  // 主机分区已用
	FreeBytes  uint64 `json:"free_bytes"`  // 主机分区可用
	DataBytes  uint64 `json:"data_bytes"`  // 本节点 data_dir 占用
	Ok         bool   `json:"ok"`
}

// DataDirFor 解析组件数据目录（相对 root）。
// storage：读 conf 的 data_dir；meta：conf 有则用，否则 data/<name>。
func DataDirFor(root string, c Component) string {
	if c.Role == "gateway" || c.Conf == "" {
		return ""
	}
	confPath := filepath.Join(root, c.Conf)
	if v := readConfValue(confPath, "data_dir"); v != "" {
		return resolveDataPath(root, v)
	}
	if c.Role == "meta" {
		return filepath.Join(root, "data", c.Name)
	}
	if c.Role == "storage" {
		return filepath.Join(root, "data", c.Name)
	}
	return ""
}

func readConfValue(path, key string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	prefix := key + "="
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

func resolveDataPath(root, raw string) string {
	if filepath.IsAbs(raw) {
		return raw
	}
	return filepath.Clean(filepath.Join(root, raw))
}

// ProbeDisk 统计 path 所在主机分区容量，以及 path 目录自身占用。
// path 不存在时仍尽量对父目录/根做 Statfs，以拿到主机容量。
func ProbeDisk(path string) DiskInfo {
	info := DiskInfo{Path: path}
	if path == "" {
		return info
	}

	statPath := path
	if _, err := os.Stat(statPath); err != nil {
		// 目录尚未创建：沿父路径回退到已存在的路径
		cur := path
		for {
			parent := filepath.Dir(cur)
			if parent == cur {
				statPath = "/"
				break
			}
			if _, err := os.Stat(parent); err == nil {
				statPath = parent
				break
			}
			cur = parent
		}
	}

	var st syscall.Statfs_t
	if err := syscall.Statfs(statPath, &st); err != nil {
		return info
	}
	// Bsize 在部分平台是 int64；统一转 uint64
	bsize := uint64(st.Bsize)
	info.TotalBytes = st.Blocks * bsize
	info.FreeBytes = st.Bavail * bsize
	if info.TotalBytes >= info.FreeBytes {
		info.UsedBytes = info.TotalBytes - info.FreeBytes
	}
	info.DataBytes = dirSize(path)
	info.Ok = info.TotalBytes > 0
	return info
}

func dirSize(root string) uint64 {
	var total uint64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total
}
