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

// Package cluster：Web 管控台访问 Meta / Storage（gRPC ExecLine）。
package cluster

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "github.com/amydemo/opskv/client/go/gen/meta/v1"
	storagev1 "github.com/amydemo/opskv/client/go/gen/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func withAdminToken(ctx context.Context) context.Context {
	tok := os.Getenv("SKV_ADMIN_TOKEN")
	if tok == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-skv-admin-token", tok)
}

func TCPLine(addr, req string, timeout time.Duration) (string, error) {
	return storageExecLine(addr, req, timeout)
}

func storageExecLine(addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = withAdminToken(ctx)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	resp, err := storagev1.NewAdminServiceClient(conn).ExecLine(ctx, &storagev1.ExecLineRequest{Line: req})
	if err != nil {
		return "", err
	}
	return resp.GetBody(), nil
}

func metaExecLine(addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = withAdminToken(ctx)
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

// StorageRPC 向 seed 发命令；遇 not_leader redirect= 则跟到 Leader（最多 6 跳）。
func StorageRPC(seedAddrs []string, req string, timeout time.Duration) (string, error) {
	var last error
	tried := map[string]bool{}
	for _, seed := range seedAddrs {
		if seed == "" || tried[seed] {
			continue
		}
		cur := seed
		for hop := 0; hop < 6; hop++ {
			if tried[cur] {
				break
			}
			tried[cur] = true
			resp, err := TCPLine(cur, req, timeout)
			if err != nil {
				last = err
				break
			}
			if strings.Contains(resp, "not_leader") {
				if j := strings.Index(resp, "redirect="); j >= 0 {
					cur = strings.TrimSpace(resp[j+9:])
					if i := strings.IndexAny(cur, " \t"); i >= 0 {
						cur = cur[:i]
					}
					continue
				}
				last = fmt.Errorf("%s", resp)
				break
			}
			return resp, nil
		}
	}
	if last == nil {
		last = fmt.Errorf("no storage for %q", req)
	}
	return "", last
}

func TCPUp(addr string, ms int) bool {
	c, err := net.DialTimeout("tcp", addr, time.Duration(ms)*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// MetaRPC 跟随 not_leader redirect。
func MetaRPC(metaAddrs []string, req string) (string, error) {
	var last error
	for _, addr := range metaAddrs {
		cur := addr
		for i := 0; i < 4; i++ {
			resp, e := metaExecLine(cur, req, 3*time.Second)
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
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return "", last
}

type Replica struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	IsLeader  bool   `json:"is_leader"`
	IsLearner bool   `json:"is_learner"`
}

type Region struct {
	ID       uint32    `json:"id"`
	StartKey string    `json:"start_key"`
	EndKey   string    `json:"end_key"`
	StartHex string    `json:"start_hex"`
	EndHex   string    `json:"end_hex"`
	Epoch    uint64    `json:"epoch"`
	Replicas []Replica `json:"replicas"`
}

type RouteTable struct {
	Version uint64   `json:"version"`
	Regions []Region `json:"regions"`
}

func FetchRoute(metaAddrs []string) (*RouteTable, error) {
	resp, err := MetaRPC(metaAddrs, "GET_ROUTE")
	if err != nil {
		return nil, err
	}
	return parseRoute(resp)
}

func parseRoute(resp string) (*RouteTable, error) {
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "OK" || parts[1] != "ROUTE" {
		return nil, fmt.Errorf("bad route: %s", resp)
	}
	ver, _ := strconv.ParseUint(parts[2], 10, 64)
	n, _ := strconv.Atoi(parts[3])
	t := &RouteTable{Version: ver}
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
		reg := Region{
			ID: uint32(id64), Epoch: epoch,
			StartKey: displayKey(start), EndKey: displayKey(end),
			StartHex: hex.EncodeToString(start), EndHex: hex.EncodeToString(end),
		}
		for i := 1; i < len(rp); i++ {
			item := rp[i]
			leader := strings.Contains(item, "*L")
			learner := strings.Contains(item, "*N")
			item = strings.ReplaceAll(item, "*L", "")
			item = strings.ReplaceAll(item, "*N", "")
			at := strings.Index(item, "@")
			if at < 0 {
				continue
			}
			reg.Replicas = append(reg.Replicas, Replica{
				NodeID: item[:at], Address: item[at+1:], IsLeader: leader, IsLearner: learner,
			})
		}
		t.Regions = append(t.Regions, reg)
		if len(t.Regions) >= n {
			break
		}
	}
	if len(t.Regions) == 0 {
		t.Regions = []Region{{ID: 0, StartKey: "−∞", EndKey: "+∞", Epoch: 1}}
	}
	return t, nil
}

func displayKey(b []byte) string {
	if len(b) == 0 {
		return "−∞"
	}
	for _, c := range b {
		if c < 32 || c > 126 {
			return "0x" + hex.EncodeToString(b)
		}
	}
	return string(b)
}

func GetSafePoint(metaAddrs []string) (safe, next, minHint uint64, err error) {
	resp, err := MetaRPC(metaAddrs, "GET_SAFE_POINT")
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

type NodeInfo struct {
	ID             string   `json:"id"`
	Address        string   `json:"address"`
	Role           string   `json:"role"`
	State          string   `json:"state"`
	StateCode      int      `json:"state_code"`
	Up             bool     `json:"up"`
	Regions        *int     `json:"regions,omitempty"`
	Leaders        *int     `json:"leaders,omitempty"`
	RegionIDs      []uint32 `json:"region_ids,omitempty"`       // 本节点承载的 Region
	LeaderRegionIDs []uint32 `json:"leader_region_ids,omitempty"` // 本节点任 Leader 的 Region
}

// NodeStateName 对齐 Meta NodeState 枚举。
func NodeStateName(code int) string {
	names := []string{"Joining", "Active", "Draining", "Offline", "Tombstone"}
	if code >= 0 && code < len(names) {
		return names[code]
	}
	return fmt.Sprintf("%d", code)
}

func ListNodes(metaAddrs []string, role string) ([]NodeInfo, string, error) {
	req := "LIST_NODES"
	if role != "" {
		req += " " + role
	}
	resp, err := MetaRPC(metaAddrs, req)
	if err != nil {
		return nil, "", err
	}
	parts := strings.Fields(resp)
	nodes := []NodeInfo{}
	if len(parts) >= 3 && parts[0] == "OK" && parts[1] == "NODES" {
		n, _ := strconv.Atoi(parts[2])
		for i := 0; i < n && 3+i < len(parts); i++ {
			tok := parts[3+i]
			at := strings.Index(tok, "@")
			if at < 0 {
				continue
			}
			id := tok[:at]
			rest := tok[at+1:]
			r1 := strings.LastIndex(rest, ":")
			if r1 < 0 {
				continue
			}
			stateStr := rest[r1+1:]
			rest2 := rest[:r1]
			r2 := strings.LastIndex(rest2, ":")
			if r2 < 0 {
				continue
			}
			roleCh := rest2[r2+1:]
			addr := rest2[:r2]
			state, _ := strconv.Atoi(stateStr)
			roleName := "compute"
			if roleCh == "S" {
				roleName = "storage"
			}
			nodes = append(nodes, NodeInfo{
				ID: id, Address: addr, Role: roleName,
				State: NodeStateName(state), StateCode: state, Up: TCPUp(addr, 400),
			})
		}
	}
	return nodes, resp, nil
}

// StoreStatus 解析 STORE_STATUS：OK STORE id state=N regions=R leaders=L
func StoreStatus(metaAddrs []string, nodeID string) (stateCode, regions, leaders int, raw string, err error) {
	raw, err = MetaRPC(metaAddrs, "STORE_STATUS "+nodeID)
	if err != nil {
		return 0, 0, 0, raw, err
	}
	parts := strings.Fields(raw)
	if len(parts) < 3 || parts[0] != "OK" {
		return 0, 0, 0, raw, fmt.Errorf("%s", raw)
	}
	for _, p := range parts[3:] {
		if strings.HasPrefix(p, "state=") {
			stateCode, _ = strconv.Atoi(strings.TrimPrefix(p, "state="))
		} else if strings.HasPrefix(p, "regions=") {
			regions, _ = strconv.Atoi(strings.TrimPrefix(p, "regions="))
		} else if strings.HasPrefix(p, "leaders=") {
			leaders, _ = strconv.Atoi(strings.TrimPrefix(p, "leaders="))
		}
	}
	return stateCode, regions, leaders, raw, nil
}

// EnrichStorageStatus 为 storage 节点填充 regions/leaders（STORE_STATUS）+ 路由上的 Region 列表。
func EnrichStorageStatus(metaAddrs []string, nodes []NodeInfo) {
	for i := range nodes {
		if nodes[i].Role != "storage" {
			continue
		}
		_, reg, lead, _, err := StoreStatus(metaAddrs, nodes[i].ID)
		if err != nil {
			continue
		}
		r, l := reg, lead
		nodes[i].Regions = &r
		nodes[i].Leaders = &l
	}
	EnrichNodeRegions(metaAddrs, nodes)
}

// EnrichNodeRegions 按 GET_ROUTE 填充每个 Store 上的 Region / Leader Region 列表。
func EnrichNodeRegions(metaAddrs []string, nodes []NodeInfo) {
	table, err := FetchRoute(metaAddrs)
	if err != nil || table == nil {
		return
	}
	byID := map[string]*NodeInfo{}
	for i := range nodes {
		if nodes[i].Role != "storage" {
			continue
		}
		nodes[i].RegionIDs = nil
		nodes[i].LeaderRegionIDs = nil
		byID[nodes[i].ID] = &nodes[i]
	}
	for _, reg := range table.Regions {
		for _, rep := range reg.Replicas {
			n := byID[rep.NodeID]
			if n == nil {
				continue
			}
			n.RegionIDs = append(n.RegionIDs, reg.ID)
			if rep.IsLeader {
				n.LeaderRegionIDs = append(n.LeaderRegionIDs, reg.ID)
			}
		}
	}
}
