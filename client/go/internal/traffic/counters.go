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

// Package traffic：Gateway 进程内流量分类计数（供 /stats 与拓扑节点间连线）。
package traffic

import (
	"sync"
	"sync/atomic"
)

var (
	// ClientIn：客户端 → Gateway gRPC
	ClientIn atomic.Uint64
	// ToStore：Gateway → Storage TCP（合计）
	ToStore atomic.Uint64
	// ToMeta：Gateway → Meta TCP（合计）
	ToMeta atomic.Uint64
)

var (
	peerMu       sync.Mutex
	toStorePeers = map[string]uint64{}
	toMetaPeers  = map[string]uint64{}
)

func AddToStore(addr string) {
	ToStore.Add(1)
	if addr == "" {
		return
	}
	peerMu.Lock()
	toStorePeers[addr]++
	peerMu.Unlock()
}

func AddToMeta(addr string) {
	ToMeta.Add(1)
	if addr == "" {
		return
	}
	peerMu.Lock()
	toMetaPeers[addr]++
	peerMu.Unlock()
}

func SnapshotStorePeers() map[string]uint64 {
	peerMu.Lock()
	defer peerMu.Unlock()
	out := make(map[string]uint64, len(toStorePeers))
	for k, v := range toStorePeers {
		out[k] = v
	}
	return out
}

func SnapshotMetaPeers() map[string]uint64 {
	peerMu.Lock()
	defer peerMu.Unlock()
	out := make(map[string]uint64, len(toMetaPeers))
	for k, v := range toMetaPeers {
		out[k] = v
	}
	return out
}
