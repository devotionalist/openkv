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

package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NodeStats 节点累计请求计数（分类 + 对端明细）。
type NodeStats struct {
	Requests      uint64            `json:"requests"`
	Client        uint64            `json:"client"`
	Gw            uint64            `json:"gw"`
	Store         uint64            `json:"store"`
	Raft          uint64            `json:"raft"`
	Sync          uint64            `json:"sync"`
	Admin         uint64            `json:"admin"`
	MetaOut       uint64            `json:"meta_out"`
	ToStore       uint64            `json:"to_store"`
	ToMeta        uint64            `json:"to_meta"`
	PeerRaft      map[string]uint64 `json:"peer_raft,omitempty"`       // from_id → count（入站）
	PeerSync      map[string]uint64 `json:"peer_sync,omitempty"`
	PeerRaftOut   map[string]uint64 `json:"peer_raft_out,omitempty"`   // to_id → count（出站）
	PeerSyncOut   map[string]uint64 `json:"peer_sync_out,omitempty"`
	PeerStore     map[string]uint64 `json:"peer_store,omitempty"` // meta：store node_id
	MetaOutPeers  map[string]uint64 `json:"meta_out_peers,omitempty"`
	ToStorePeers  map[string]uint64 `json:"to_store_peers,omitempty"`
	ToMetaPeers   map[string]uint64 `json:"to_meta_peers,omitempty"`
}

// FetchTCPStats 直连 Storage（不跟随 Meta redirect），读 STATS。
func FetchTCPStats(addr string) (NodeStats, error) {
	var out NodeStats
	resp, err := TCPLine(addr, "STATS", 800*time.Millisecond)
	if err != nil {
		return out, err
	}
	return parseStatsLine(resp)
}

// FetchMetaStats 直连 Meta ExecLine 读 STATS（gRPC 下不可走 Storage Admin）。
func FetchMetaStats(addr string) (NodeStats, error) {
	var out NodeStats
	resp, err := metaExecLine(addr, "STATS", 800*time.Millisecond)
	if err != nil {
		return out, err
	}
	return parseStatsLine(resp)
}

func parsePeerMap(s string) map[string]uint64 {
	out := map[string]uint64{}
	if s == "" {
		return out
	}
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		// id 可能含冒号（addr），取最后一个 : 分割数值
		i := strings.LastIndex(tok, ":")
		if i <= 0 {
			continue
		}
		key := tok[:i]
		n, err := strconv.ParseUint(tok[i+1:], 10, 64)
		if err != nil {
			continue
		}
		out[key] = n
	}
	return out
}

func parseStatsLine(resp string) (NodeStats, error) {
	var out NodeStats
	parts := strings.Fields(resp)
	if len(parts) < 2 || parts[0] != "OK" || parts[1] != "STATS" {
		return out, fmt.Errorf("bad stats: %s", resp)
	}
	for _, tok := range parts[2:] {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		switch k {
		case "peer_raft":
			out.PeerRaft = parsePeerMap(v)
		case "peer_sync":
			out.PeerSync = parsePeerMap(v)
		case "peer_raft_out":
			out.PeerRaftOut = parsePeerMap(v)
		case "peer_sync_out":
			out.PeerSyncOut = parsePeerMap(v)
		case "peer_store":
			out.PeerStore = parsePeerMap(v)
		case "meta_out_peers":
			out.MetaOutPeers = parsePeerMap(v)
		case "to_store_peers":
			out.ToStorePeers = parsePeerMap(v)
		case "to_meta_peers":
			out.ToMetaPeers = parsePeerMap(v)
		default:
			n, _ := strconv.ParseUint(v, 10, 64)
			switch k {
			case "requests":
				out.Requests = n
			case "client":
				out.Client = n
			case "gw":
				out.Gw = n
			case "store":
				out.Store = n
			case "raft":
				out.Raft = n
			case "sync":
				out.Sync = n
			case "admin":
				out.Admin = n
			case "meta_out":
				out.MetaOut = n
			case "to_store":
				out.ToStore = n
			case "to_meta":
				out.ToMeta = n
			}
		}
	}
	if out.Gw == 0 && out.Client > 0 && out.Raft == 0 && out.Sync == 0 {
		out.Gw = out.Client
	}
	return out, nil
}

// FetchGatewayStats HTTP GET /stats（gateway -stats-listen）。
func FetchGatewayStats(httpAddr string) (NodeStats, error) {
	var out NodeStats
	if httpAddr == "" {
		return out, fmt.Errorf("empty gateway stats addr")
	}
	url := "http://" + httpAddr + "/stats"
	cli := &http.Client{Timeout: 800 * time.Millisecond}
	resp, err := cli.Get(url)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return out, err
	}
	var raw struct {
		Requests      uint64            `json:"requests"`
		Client        uint64            `json:"client"`
		Gw            uint64            `json:"gw"`
		ToStore       uint64            `json:"to_store"`
		ToMeta        uint64            `json:"to_meta"`
		ToStorePeers  map[string]uint64 `json:"to_store_peers"`
		ToMetaPeers   map[string]uint64 `json:"to_meta_peers"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return out, err
	}
	out.Requests = raw.Requests
	out.Client = raw.Client
	out.Gw = raw.Gw
	if out.Gw == 0 {
		out.Gw = out.Client
	}
	out.ToStore = raw.ToStore
	out.ToMeta = raw.ToMeta
	out.ToStorePeers = raw.ToStorePeers
	out.ToMetaPeers = raw.ToMetaPeers
	return out, nil
}

// RateTracker 根据累计计数估算 QPS。
type RateTracker struct {
	mu   sync.Mutex
	last map[string]rateSample
}

type rateSample struct {
	count uint64
	at    time.Time
	qps   float64
}

func NewRateTracker() *RateTracker {
	return &RateTracker{last: map[string]rateSample{}}
}

// Observe 记录累计请求数，返回估算 QPS（首次采样返回 0）。
func (t *RateTracker) Observe(key string, count uint64) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	prev, ok := t.last[key]
	if !ok || now.Before(prev.at) || count < prev.count {
		t.last[key] = rateSample{count: count, at: now, qps: 0}
		return 0
	}
	dt := now.Sub(prev.at).Seconds()
	qps := 0.0
	if dt >= 0.4 {
		qps = float64(count-prev.count) / dt
	} else {
		qps = prev.qps
	}
	t.last[key] = rateSample{count: count, at: now, qps: qps}
	return qps
}

// FlowEdge 角色级汇总边。
type FlowEdge struct {
	ID    string  `json:"id"`
	From  string  `json:"from"`
	To    string  `json:"to"`
	Kind  string  `json:"kind"`
	Label string  `json:"label"`
	QPS   float64 `json:"qps"`
	Count uint64  `json:"count"`
}

// NodeLink 节点对边（画线用）。
type NodeLink struct {
	From  string  `json:"from"`
	To    string  `json:"to"`
	Kind  string  `json:"kind"` // gateway | meta | raft | sync
	Label string  `json:"label"`
	QPS   float64 `json:"qps"`
	Count uint64  `json:"count"`
}
