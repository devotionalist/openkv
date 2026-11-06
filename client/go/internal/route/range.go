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

// Package route：从 Meta GET_ROUTE 解析 Range 路由并按 key 定位 Region。
package route

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "github.com/amydemo/opskv/client/go/gen/meta/v1"
	"github.com/amydemo/opskv/client/go/internal/traffic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Replica struct {
	NodeID   string
	Address  string
	IsLeader bool
}

type Region struct {
	ID       uint32
	StartKey []byte // inclusive; empty = -inf
	EndKey   []byte // exclusive; empty = +inf
	Epoch    uint64
	Replicas []Replica
}

type Table struct {
	Version uint64
	Regions []Region
}

func (r Region) Contains(key []byte) bool {
	if len(r.StartKey) > 0 && string(key) < string(r.StartKey) {
		return false
	}
	if len(r.EndKey) > 0 && string(key) >= string(r.EndKey) {
		return false
	}
	return true
}

func (t *Table) Locate(key []byte) (uint32, bool) {
	for _, r := range t.Regions {
		if r.Contains(key) {
			return r.ID, true
		}
	}
	return 0, false
}

func (t *Table) Region(id uint32) (Region, bool) {
	for _, r := range t.Regions {
		if r.ID == id {
			return r, true
		}
	}
	return Region{}, false
}

// MetaRPC 跟随 not_leader redirect 发一行 Meta 命令（A1：指数退避重试）。
func MetaRPC(metaAddrs []string, req string) (string, error) {
	return metaLeaderRPC(metaAddrs, req)
}

// metaLeaderRPC 跟随 not_leader redirect 发一行 Meta 命令。
func metaLeaderRPC(metaAddrs []string, req string) (string, error) {
	var last error
	backoff := 20 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		for _, addr := range metaAddrs {
			cur := addr
			for i := 0; i < 6; i++ {
				traffic.AddToMeta(cur)
				resp, e := tcpLine(cur, req)
				if e != nil {
					last = e
					break
				}
				if strings.Contains(resp, "not_leader") {
					if j := strings.Index(resp, "redirect="); j >= 0 {
						cur = strings.TrimSpace(resp[j+9:])
						continue
					}
					last = fmt.Errorf("%s", resp)
					break
				}
				return resp, nil
			}
		}
		time.Sleep(backoff)
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return "", last
}

// AllocTs 向 Meta 申请单调时间戳（GET_TS），跟随 not_leader redirect。
func AllocTs(metaAddrs []string, count uint32) (start uint64, err error) {
	if count == 0 {
		count = 1
	}
	resp, err := metaLeaderRPC(metaAddrs, fmt.Sprintf("GET_TS %d", count))
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) >= 3 && parts[0] == "OK" && parts[1] == "TS" {
		return strconv.ParseUint(parts[2], 10, 64)
	}
	return 0, fmt.Errorf("bad ts: %s", resp)
}

// ReportMinStartTs 上报本 Gateway 活跃事务最小 start_ts；math.MaxUint64 表示无活跃。
func ReportMinStartTs(metaAddrs []string, minStart uint64) error {
	return ReportMinStartTsAs(metaAddrs, minStart, "gateway")
}

// ReportMinStartTsAs T2：带 reporter id 的分源上报。
func ReportMinStartTsAs(metaAddrs []string, minStart uint64, reporter string) error {
	req := fmt.Sprintf("REPORT_MIN_START_TS %d", minStart)
	if reporter != "" {
		req += " " + reporter
	}
	resp, err := metaLeaderRPC(metaAddrs, req)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "OK ") {
		return fmt.Errorf("report min start: %s", resp)
	}
	return nil
}

// GetSafePoint 读 Meta GC 水位。
func GetSafePoint(metaAddrs []string) (safe, next, minHint uint64, err error) {
	resp, err := metaLeaderRPC(metaAddrs, "GET_SAFE_POINT")
	if err != nil {
		return 0, 0, 0, err
	}
	parts := strings.Fields(resp)
	if len(parts) < 5 || parts[0] != "OK" || parts[1] != "SAFE" {
		return 0, 0, 0, fmt.Errorf("bad safe point: %s", resp)
	}
	safe, _ = strconv.ParseUint(parts[2], 10, 64)
	next, _ = strconv.ParseUint(parts[3], 10, 64)
	minHint, _ = strconv.ParseUint(parts[4], 10, 64)
	return safe, next, minHint, nil
}

// Fetch 向 Meta 拉路由（A3：任意副本可读，无需 redirect）。
func Fetch(metaAddrs []string) (*Table, error) {
	var last error
	for _, addr := range metaAddrs {
		traffic.AddToMeta(addr)
		resp, err := tcpLineTimeout(addr, "GET_ROUTE", 3*time.Second)
		if err != nil {
			last = err
			continue
		}
		if strings.Contains(resp, "not_leader") {
			// 兼容旧 Meta；仍跟随 redirect
			if j := strings.Index(resp, "redirect="); j >= 0 {
				cur := strings.TrimSpace(resp[j+9:])
				resp2, e2 := tcpLineTimeout(cur, "GET_ROUTE", 3*time.Second)
				if e2 != nil {
					last = e2
					continue
				}
				return parseRoute(resp2)
			}
			last = fmt.Errorf("%s", resp)
			continue
		}
		return parseRoute(resp)
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return nil, last
}

// WatchFetch A2：长轮询直到 route_ver > since 或超时；timeoutMs 建议 3000–5000。
func WatchFetch(metaAddrs []string, since uint64, timeoutMs int) (*Table, error) {
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	req := fmt.Sprintf("WATCH_ROUTE %d %d", since, timeoutMs)
	deadline := time.Duration(timeoutMs+2000) * time.Millisecond
	var last error
	for _, addr := range metaAddrs {
		traffic.AddToMeta(addr)
		resp, err := tcpLineTimeout(addr, req, deadline)
		if err != nil {
			last = err
			continue
		}
		if strings.Contains(resp, "not_leader") {
			if j := strings.Index(resp, "redirect="); j >= 0 {
				cur := strings.TrimSpace(resp[j+9:])
				resp2, e2 := tcpLineTimeout(cur, req, deadline)
				if e2 != nil {
					last = e2
					continue
				}
				return parseRoute(resp2)
			}
			last = fmt.Errorf("%s", resp)
			continue
		}
		return parseRoute(resp)
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return nil, last
}

func tcpLine(addr, req string) (string, error) {
	return tcpLineTimeout(addr, req, 3*time.Second)
}

func tcpLineTimeout(addr, req string, deadline time.Duration) (string, error) {
	return metaExecLine(addr, req, deadline)
}

func metaExecLine(addr, req string, deadline time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	if tok := os.Getenv("SKV_ADMIN_TOKEN"); tok != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-skv-admin-token", tok)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	resp, err := metav1.NewMetaServiceClient(conn).ExecLine(ctx, &metav1.ExecLineRequest{Line: req})
	if err != nil {
		return "", err
	}
	return resp.GetBody(), nil
}

// OK ROUTE ver n n rid:startHex:endHex:epoch:nrep,node@addr*L,...
// 兼容旧格式 rid:start:end:nrep,...
func parseRoute(resp string) (*Table, error) {
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "OK" || parts[1] != "ROUTE" {
		return nil, fmt.Errorf("bad route: %s", resp)
	}
	ver, _ := strconv.ParseUint(parts[2], 10, 64)
	n, _ := strconv.Atoi(parts[3])
	t := &Table{Version: ver}
	for _, tok := range parts[5:] {
		colon := strings.SplitN(tok, ":", 5)
		if len(colon) < 4 {
			continue
		}
		id64, _ := strconv.ParseUint(colon[0], 10, 32)
		start, _ := hex.DecodeString(colon[1])
		end, _ := hex.DecodeString(colon[2])
		var epoch uint64 = 1
		var rest string
		if len(colon) == 5 {
			epoch, _ = strconv.ParseUint(colon[3], 10, 64)
			rest = colon[4]
		} else {
			rest = colon[3]
		}
		rp := strings.Split(rest, ",")
		reg := Region{ID: uint32(id64), StartKey: start, EndKey: end, Epoch: epoch}
		for i := 1; i < len(rp); i++ {
			item := rp[i]
			leader := strings.Contains(item, "*L")
			item = strings.ReplaceAll(item, "*L", "")
			item = strings.ReplaceAll(item, "*N", "")
			at := strings.Index(item, "@")
			if at < 0 {
				continue
			}
			reg.Replicas = append(reg.Replicas, Replica{
				NodeID:   item[:at],
				Address:  item[at+1:],
				IsLeader: leader,
			})
		}
		t.Regions = append(t.Regions, reg)
		if len(t.Regions) >= n {
			break
		}
	}
	if len(t.Regions) == 0 {
		t.Regions = []Region{{ID: 0, Epoch: 1}}
	}
	return t, nil
}

// Cache 线程安全路由缓存。
type Cache struct {
	mu    sync.RWMutex
	table *Table
}

func (c *Cache) Update(t *Table) {
	c.mu.Lock()
	c.table = t
	c.mu.Unlock()
}

func (c *Cache) Locate(key []byte) uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.table == nil {
		return 0
	}
	if id, ok := c.table.Locate(key); ok {
		return id
	}
	return 0
}

func (c *Cache) Epoch(regionID uint32) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.table == nil {
		return 0
	}
	if r, ok := c.table.Region(regionID); ok {
		return r.Epoch
	}
	return 0
}

// BumpEpoch 将本地缓存中 region 的 epoch 抬到至少 to（Storage 返回 expected= 时用）。
func (c *Cache) BumpEpoch(regionID uint32, to uint64) {
	if to == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.table == nil {
		return
	}
	for i := range c.table.Regions {
		if c.table.Regions[i].ID == regionID && c.table.Regions[i].Epoch < to {
			c.table.Regions[i].Epoch = to
			return
		}
	}
}

func (c *Cache) Snapshot() *Table {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.table
}

// ReplicaAddrs 返回 region 副本地址；leader 优先。无路由时返回 nil。
func (c *Cache) ReplicaAddrs(regionID uint32) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.table == nil {
		return nil
	}
	r, ok := c.table.Region(regionID)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(r.Replicas))
	var leader string
	for _, rp := range r.Replicas {
		if rp.Address == "" {
			continue
		}
		if rp.IsLeader {
			leader = rp.Address
			continue
		}
		out = append(out, rp.Address)
	}
	if leader != "" {
		return append([]string{leader}, out...)
	}
	return out
}
