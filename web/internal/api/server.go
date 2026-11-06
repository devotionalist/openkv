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

package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amydemo/opskv/client/go/kv"
	"github.com/amydemo/opskv/web/internal/cluster"
	"github.com/amydemo/opskv/web/internal/lifecycle"
)

// Server 是独立 Web 管控模块的 HTTP API。
type Server struct {
	MetaAddrs []string
	Storages  []string
	Gateway   string
	RunDir    string
	Root      string // 仓库根；空则从 RunDir / SKV_ROOT 推断

	ratesOnce sync.Once
	rates     *cluster.RateTracker
}

func (s *Server) rateTracker() *cluster.RateTracker {
	s.ratesOnce.Do(func() { s.rates = cluster.NewRateTracker() })
	return s.rates
}

func gatewayStatsHTTP(grpcAddr string) string {
	host, port, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return "127.0.0.1:19001"
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	p, _ := strconv.Atoi(port)
	if p <= 0 {
		p = 19000
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/route", s.handleRoute)
	mux.HandleFunc("/api/nodes", s.handleNodes)
	mux.HandleFunc("/api/safe-point", s.handleSafePoint)
	mux.HandleFunc("/api/ping", s.handlePing)
	mux.HandleFunc("/api/locate", s.handleLocate)
	mux.HandleFunc("/api/kv/get", s.handleKVGet)
	mux.HandleFunc("/api/kv/put", s.handleKVPut)
	mux.HandleFunc("/api/ds", s.handleDS)
	mux.HandleFunc("/api/split", s.handleSplit)
	mux.HandleFunc("/api/merge", s.handleMerge)
	mux.HandleFunc("/api/place", s.handlePlace)
	mux.HandleFunc("/api/suggest-place", s.handleSuggestPlace)
	mux.HandleFunc("/api/add-replica", s.handleAddReplica)
	mux.HandleFunc("/api/suggest-replica", s.handleSuggestReplica)
	mux.HandleFunc("/api/replica-online", s.handleReplicaOnline)
	mux.HandleFunc("/api/replica-offline", s.handleReplicaOffline)
	mux.HandleFunc("/api/replica-kick", s.handleReplicaKick)
	mux.HandleFunc("/api/scale-task", s.handleScaleTask)
	mux.HandleFunc("/api/scale-tasks", s.handleScaleTasks)
	mux.HandleFunc("/api/find-merge-right", s.handleFindMergeRight)
	mux.HandleFunc("/api/advance-safe", s.handleAdvanceSafe)
	mux.HandleFunc("/api/join-node", s.handleJoinNode)
	mux.HandleFunc("/api/scale-in", s.handleScaleIn)
	mux.HandleFunc("/api/scale-in-status", s.handleScaleInStatus)
	mux.HandleFunc("/api/remove-tombstone", s.handleRemoveTombstone)
	mux.HandleFunc("/api/node-online", s.handleNodeOnline)
	mux.HandleFunc("/api/node-offline", s.handleNodeOffline)
	mux.HandleFunc("/api/node-kick", s.handleNodeKick)
	mux.HandleFunc("/api/balance", s.handleBalance)
	mux.HandleFunc("/api/processes", s.handleProcesses)
	mux.HandleFunc("/api/topology", s.handleTopology)
	mux.HandleFunc("/api/node/stop", s.handleNodeStop)
	mux.HandleFunc("/api/node/start", s.handleNodeStart)
	mux.HandleFunc("/api/node/restart", s.handleNodeRestart)
	return mux
}

func (s *Server) root() string {
	return lifecycle.ResolveRoot(s.RunDir, s.Root)
}

func (s *Server) requireRunDir(w http.ResponseWriter) bool {
	if s.RunDir == "" {
		writeErr(w, 400, fmt.Errorf("需要配置 -run-dir / SKV_RUN 才能上下线节点"))
		return false
	}
	return true
}

func (s *Server) buildTopology() map[string]any {
	leaderAddr := ""
	if resp, err := cluster.MetaRPC(s.MetaAddrs, "WHO_LEADER"); err == nil {
		// OK LEADER <addr>
		parts := strings.Fields(resp)
		if len(parts) >= 3 {
			leaderAddr = parts[2]
		}
	}
	storeLeaders := map[string]bool{}
	if table, err := cluster.FetchRoute(s.MetaAddrs); err == nil {
		for _, r := range table.Regions {
			for _, rep := range r.Replicas {
				if rep.IsLeader {
					storeLeaders[rep.Address] = true
					storeLeaders[rep.NodeID] = true
				}
			}
		}
	}

	type rates struct {
		Gw      float64 `json:"gw"`
		Store   float64 `json:"store"`
		Raft    float64 `json:"raft"`
		Sync    float64 `json:"sync"`
		Admin   float64 `json:"admin"`
		MetaOut float64 `json:"meta_out"`
		ToStore float64 `json:"to_store"`
		ToMeta  float64 `json:"to_meta"`
	}
	type node struct {
		Name         string              `json:"name"`
		Role         string              `json:"role"`
		Addr         string              `json:"addr"`
		Up           bool                `json:"up"`
		Alive        bool                `json:"alive"`
		Port         bool                `json:"port"`
		PID          int                 `json:"pid"`
		IsLeader     bool                `json:"is_leader"`
		ClusterState string              `json:"cluster_state,omitempty"` // Joining/Active/…
		StateLabel   string              `json:"state_label,omitempty"`   // 中文短标签
		Disk         *lifecycle.DiskInfo `json:"disk,omitempty"`
		QPS          float64             `json:"qps"`
		ClientQPS    float64             `json:"client_qps"`
		ReqTotal     uint64              `json:"req_total,omitempty"`
		ReqClient    uint64              `json:"req_client,omitempty"`
		Counts       cluster.NodeStats   `json:"counts"`
		Rates        rates               `json:"rates"`
	}
	// Meta 注册态（storage / compute）
	metaStateByID := map[string]string{}
	if listed, _, err := cluster.ListNodes(s.MetaAddrs, ""); err == nil {
		for _, n := range listed {
			metaStateByID[n.ID] = n.State
		}
	}
	rt := s.rateTracker()
	// 本地 -run-dir：用 DefaultComponents（127.0.0.1 + pid）；远程/K8s：用 -metas/-storages/-gateway
	var statuses []lifecycle.Status
	if s.RunDir != "" {
		statuses = lifecycle.ListStatus(s.root(), s.RunDir)
	} else {
		statuses = lifecycle.ListStatusRemote(lifecycle.ComponentsFromAddrs(s.MetaAddrs, s.Storages, s.Gateway))
	}
	nodes := make([]node, len(statuses))
	var wg sync.WaitGroup
	for i, st := range statuses {
		n := node{
			Name: st.Name, Role: st.Role, Addr: st.Addr,
			Up: st.Up, Alive: st.Alive, Port: st.Port, PID: st.PID,
		}
		if st.Disk.Ok && (st.Role == "meta" || st.Role == "storage") {
			d := st.Disk
			n.Disk = &d
		}
		nodes[i] = n
		if !st.Up || (st.Role != "meta" && st.Role != "storage" && st.Role != "gateway") {
			continue
		}
		wg.Add(1)
		go func(idx int, st lifecycle.Status) {
			defer wg.Done()
			var stats cluster.NodeStats
			var err error
			switch st.Role {
			case "meta":
				stats, err = cluster.FetchMetaStats(st.Addr)
			case "storage":
				stats, err = cluster.FetchTCPStats(st.Addr)
			case "gateway":
				stats, err = cluster.FetchGatewayStats(gatewayStatsHTTP(st.Addr))
			}
			if err != nil {
				return
			}
			nodes[idx].Counts = stats
			nodes[idx].ReqTotal = stats.Requests
			nodes[idx].ReqClient = stats.Client
			nodes[idx].QPS = rt.Observe(st.Name+":all", stats.Requests)
			r := rates{
				Gw:      rt.Observe(st.Name+":gw", stats.Gw),
				Store:   rt.Observe(st.Name+":store", stats.Store),
				Raft:    rt.Observe(st.Name+":raft", stats.Raft),
				Sync:    rt.Observe(st.Name+":sync", stats.Sync),
				Admin:   rt.Observe(st.Name+":admin", stats.Admin),
				MetaOut: rt.Observe(st.Name+":meta_out", stats.MetaOut),
				ToStore: rt.Observe(st.Name+":to_store", stats.ToStore),
				ToMeta:  rt.Observe(st.Name+":to_meta", stats.ToMeta),
			}
			nodes[idx].Rates = r
			switch st.Role {
			case "gateway":
				nodes[idx].ClientQPS = r.Gw
			case "storage":
				nodes[idx].ClientQPS = r.Gw
			case "meta":
				nodes[idx].ClientQPS = r.Gw + r.Store
			}
		}(i, st)
	}
	wg.Wait()

	stateLabel := func(st string) string {
		switch strings.ToLower(st) {
		case "joining":
			return "待加入"
		case "active":
			return "在用"
		case "draining":
			return "缩容中"
		case "offline":
			return "心跳超时"
		case "tombstone":
			return "已腾空"
		default:
			return st
		}
	}
	var metas, stores, gws []node
	for _, n := range nodes {
		switch n.Role {
		case "meta":
			n.IsLeader = leaderAddr != "" && n.Addr == leaderAddr
			metas = append(metas, n)
		case "storage":
			n.IsLeader = storeLeaders[n.Addr] || storeLeaders[n.Name]
			if st, ok := metaStateByID[n.Name]; ok {
				n.ClusterState = st
				n.StateLabel = stateLabel(st)
			}
			stores = append(stores, n)
		case "gateway":
			gws = append(gws, n)
		}
	}

	sumRate := func(list []node, pick func(rates) float64) float64 {
		var s float64
		for _, n := range list {
			s += pick(n.Rates)
		}
		return s
	}
	sumCount := func(list []node, pick func(cluster.NodeStats) uint64) uint64 {
		var s uint64
		for _, n := range list {
			s += pick(n.Counts)
		}
		return s
	}

	// name / addr → 规范节点名
	byRef := map[string]string{}
	for _, n := range nodes {
		byRef[n.Name] = n.Name
		if n.Addr != "" {
			byRef[n.Addr] = n.Name
		}
	}
	resolve := func(ref string) string {
		if ref == "" {
			return ""
		}
		if n, ok := byRef[ref]; ok {
			return n
		}
		return ref
	}

	type linkKey struct{ from, to, kind string }
	accum := map[linkKey]uint64{}
	// gateway/meta：累加；raft/sync：取 max（入站+出站描述同一方向，避免双计）
	addLink := func(from, to, kind string, n uint64, useMax bool) {
		from, to = resolve(from), resolve(to)
		if from == "" || to == "" || from == to || n == 0 {
			return
		}
		k := linkKey{from, to, kind}
		if useMax {
			if old, ok := accum[k]; !ok || n > old {
				accum[k] = n
			}
			return
		}
		accum[k] += n
	}

	for _, n := range gws {
		addLink("client", n.Name, "gateway", n.Counts.Gw, false)
		for peer, c := range n.Counts.ToStorePeers {
			addLink(n.Name, peer, "gateway", c, false)
		}
		for peer, c := range n.Counts.ToMetaPeers {
			addLink(n.Name, peer, "meta", c, false)
		}
	}
	hasStoreMetaOut := false
	for _, n := range stores {
		// 出站优先（Leader 心跳），入站兜底
		for peer, c := range n.Counts.PeerRaftOut {
			addLink(n.Name, peer, "raft", c, true)
		}
		for peer, c := range n.Counts.PeerSyncOut {
			addLink(n.Name, peer, "sync", c, true)
		}
		for peer, c := range n.Counts.PeerRaft {
			addLink(peer, n.Name, "raft", c, true)
		}
		for peer, c := range n.Counts.PeerSync {
			addLink(peer, n.Name, "sync", c, true)
		}
		for peer, c := range n.Counts.MetaOutPeers {
			hasStoreMetaOut = true
			addLink(n.Name, peer, "meta", c, false)
		}
	}
	for _, n := range metas {
		for peer, c := range n.Counts.PeerRaftOut {
			addLink(n.Name, peer, "raft", c, true)
		}
		for peer, c := range n.Counts.PeerSyncOut {
			addLink(n.Name, peer, "sync", c, true)
		}
		for peer, c := range n.Counts.PeerRaft {
			addLink(peer, n.Name, "raft", c, true)
		}
		for peer, c := range n.Counts.PeerSync {
			addLink(peer, n.Name, "sync", c, true)
		}
		if !hasStoreMetaOut {
			for peer, c := range n.Counts.PeerStore {
				addLink(peer, n.Name, "meta", c, false)
			}
		}
	}

	kindLabel := map[string]string{
		"gateway": "网关流量",
		"meta":    "Meta 交互",
		"raft":    "Raft 交互",
		"sync":    "数据同步",
	}
	links := make([]cluster.NodeLink, 0, len(accum))
	for k, count := range accum {
		qps := rt.Observe("link:"+k.from+"→"+k.to+":"+k.kind, count)
		links = append(links, cluster.NodeLink{
			From: k.from, To: k.to, Kind: k.kind,
			Label: kindLabel[k.kind], QPS: qps, Count: count,
		})
	}
	// 稳定排序：kind → qps desc → from/to
	sort.Slice(links, func(i, j int) bool {
		if links[i].Kind != links[j].Kind {
			return links[i].Kind < links[j].Kind
		}
		if links[i].QPS != links[j].QPS {
			return links[i].QPS > links[j].QPS
		}
		if links[i].From != links[j].From {
			return links[i].From < links[j].From
		}
		return links[i].To < links[j].To
	})

	// 角色级汇总边（兼容）
	clientGwQPS := sumRate(gws, func(r rates) float64 { return r.Gw })
	gwStoreQPS := sumRate(gws, func(r rates) float64 { return r.ToStore })
	if gwStoreQPS == 0 {
		gwStoreQPS = sumRate(stores, func(r rates) float64 { return r.Gw })
	}
	gwMetaQPS := sumRate(gws, func(r rates) float64 { return r.ToMeta })
	if gwMetaQPS == 0 {
		gwMetaQPS = sumRate(metas, func(r rates) float64 { return r.Gw })
	}
	storeMetaQPS := sumRate(stores, func(r rates) float64 { return r.MetaOut })
	if storeMetaQPS == 0 {
		storeMetaQPS = sumRate(metas, func(r rates) float64 { return r.Store })
	}
	storeRaftQPS := sumRate(stores, func(r rates) float64 { return r.Raft })
	storeSyncQPS := sumRate(stores, func(r rates) float64 { return r.Sync })
	metaRaftQPS := sumRate(metas, func(r rates) float64 { return r.Raft + r.Sync })

	flows := []cluster.FlowEdge{
		{ID: "client_gw", From: "client", To: "gateway", Kind: "gateway", Label: "客户端→网关", QPS: clientGwQPS, Count: sumCount(gws, func(c cluster.NodeStats) uint64 { return c.Gw })},
		{ID: "gw_store", From: "gateway", To: "storage", Kind: "gateway", Label: "网关→Store", QPS: gwStoreQPS, Count: sumCount(gws, func(c cluster.NodeStats) uint64 { return c.ToStore })},
		{ID: "gw_meta", From: "gateway", To: "meta", Kind: "meta", Label: "网关↔Meta", QPS: gwMetaQPS, Count: sumCount(gws, func(c cluster.NodeStats) uint64 { return c.ToMeta })},
		{ID: "store_meta", From: "storage", To: "meta", Kind: "meta", Label: "Store↔Meta", QPS: storeMetaQPS, Count: sumCount(stores, func(c cluster.NodeStats) uint64 { return c.MetaOut })},
		{ID: "store_raft", From: "storage", To: "storage", Kind: "raft", Label: "Store Raft", QPS: storeRaftQPS, Count: sumCount(stores, func(c cluster.NodeStats) uint64 { return c.Raft })},
		{ID: "store_sync", From: "storage", To: "storage", Kind: "sync", Label: "Store 数据同步", QPS: storeSyncQPS, Count: sumCount(stores, func(c cluster.NodeStats) uint64 { return c.Sync })},
		{ID: "meta_raft", From: "meta", To: "meta", Kind: "raft", Label: "Meta Raft", QPS: metaRaftQPS, Count: sumCount(metas, func(c cluster.NodeStats) uint64 { return c.Raft + c.Sync })},
	}

	return map[string]any{
		"controllable": s.RunDir != "",
		"groups": []map[string]any{
			{"role": "gateway", "label": "Compute Gateway", "nodes": gws},
			{"role": "meta", "label": "Meta Raft（管控 / TSO）", "nodes": metas},
			{"role": "storage", "label": "Storage Multi-Raft", "nodes": stores},
		},
		"layers": []map[string]any{
			{"role": "gateway", "label": "Compute Gateway", "nodes": gws},
			{"role": "meta", "label": "Meta Raft（管控 / TSO）", "nodes": metas},
			{"role": "storage", "label": "Storage Multi-Raft", "nodes": stores},
		},
		"flows": flows,
		"edges": flows,
		"links": links,
	}
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.buildTopology())
}

func (s *Server) handleNodeStop(w http.ResponseWriter, r *http.Request) {
	s.handleNodeAction(w, r, "stop")
}

func (s *Server) handleNodeStart(w http.ResponseWriter, r *http.Request) {
	s.handleNodeAction(w, r, "start")
}

func (s *Server) handleNodeRestart(w http.ResponseWriter, r *http.Request) {
	s.handleNodeAction(w, r, "restart")
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	if !s.requireRunDir(w) {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		writeErr(w, 400, fmt.Errorf("name required"))
		return
	}
	root, run := s.root(), s.RunDir
	var err error
	switch action {
	case "stop":
		err = lifecycle.Stop(root, run, body.Name, true)
	case "start":
		err = lifecycle.Start(root, run, body.Name)
	case "restart":
		err = lifecycle.Restart(root, run, body.Name)
	default:
		writeErr(w, 400, fmt.Errorf("bad action"))
		return
	}
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	st := lifecycle.StatusOf(root, run, body.Name)
	writeJSON(w, 200, map[string]any{"ok": true, "action": action, "status": st, "topology": s.buildTopology()})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	table, routeErr := cluster.FetchRoute(s.MetaAddrs)
	safe, next, hint, safeErr := cluster.GetSafePoint(s.MetaAddrs)
	leaderResp, _ := cluster.MetaRPC(s.MetaAddrs, "WHO_LEADER")
	_, nodesResp, nodesErr := cluster.ListNodes(s.MetaAddrs, "")

	metaUp, storeUp := 0, 0
	for _, m := range s.MetaAddrs {
		if cluster.TCPUp(m, 400) {
			metaUp++
		}
	}
	for _, st := range s.Storages {
		if cluster.TCPUp(st, 400) {
			storeUp++
		}
	}
	gwUp := cluster.TCPUp(s.Gateway, 400)

	regions, ver := 0, uint64(0)
	if table != nil {
		regions = len(table.Regions)
		ver = table.Version
	}
	out := map[string]any{
		"meta_up": metaUp, "meta_total": len(s.MetaAddrs),
		"storage_up": storeUp, "storage_total": len(s.Storages),
		"gateway_up": gwUp, "gateway": s.Gateway,
		"route_version": ver, "region_count": regions,
		"gc_safe_ts": safe, "next_ts": next, "min_start_hint": hint,
		"who_leader": leaderResp,
		"fetched_at": time.Now().UTC().Format(time.RFC3339),
		"topology":   s.buildTopology(),
	}
	if routeErr != nil {
		out["route_error"] = routeErr.Error()
	}
	if safeErr != nil {
		out["safe_error"] = safeErr.Error()
	}
	if nodesErr != nil {
		out["nodes_error"] = nodesErr.Error()
	} else {
		out["nodes_raw"] = nodesResp
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	table, err := cluster.FetchRoute(s.MetaAddrs)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, table)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	role := strings.TrimSpace(r.URL.Query().Get("role"))
	nodes, raw, err := cluster.ListNodes(s.MetaAddrs, role)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	cluster.EnrichStorageStatus(s.MetaAddrs, nodes)
	writeJSON(w, 200, map[string]any{"nodes": nodes, "raw": raw})
}

func (s *Server) handleJoinNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		writeErr(w, 400, fmt.Errorf("node_id required"))
		return
	}
	// 预检：查地址 + Storage PING（Meta JOIN 侧仍会再检心跳/可达）
	nodes, _, err := cluster.ListNodes(s.MetaAddrs, "storage")
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	var addr, state string
	for _, n := range nodes {
		if n.ID == body.NodeID {
			addr, state = n.Address, n.State
			break
		}
	}
	if addr == "" {
		writeErr(w, 400, fmt.Errorf("unknown_node %s", body.NodeID))
		return
	}
	if strings.EqualFold(state, "joining") || strings.EqualFold(state, "offline") {
		pong, pingErr := cluster.TCPLine(addr, "PING", 2*time.Second)
		if pingErr != nil {
			writeJSON(w, 200, map[string]any{
				"ok": false, "response": "ERR unreachable " + pingErr.Error(),
				"node_id": body.NodeID, "address": addr, "state": state,
				"hint": "节点不可达，请确认进程已启动且端口监听后再加入",
			})
			return
		}
		if !strings.HasPrefix(pong, "OK") {
			writeJSON(w, 200, map[string]any{
				"ok": false, "response": "ERR unreachable ping=" + pong,
				"node_id": body.NodeID, "address": addr, "state": state,
				"hint": "节点 PING 失败",
			})
			return
		}
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "JOIN_NODE "+body.NodeID)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	ok := strings.HasPrefix(resp, "OK")
	hint := "Joining→Active; then balance/place can schedule replicas"
	if !ok {
		hint = "加入被拒绝（可达性/心跳/状态）；见 response"
		if strings.Contains(resp, "unreachable") {
			hint = "节点不可达"
		} else if strings.Contains(resp, "heartbeat_stale") || strings.Contains(resp, "no_heartbeat") {
			hint = "节点心跳过期或未上报，确认 Store 已连上 Meta"
		} else if strings.Contains(resp, "not_pending") {
			hint = "仅 Joining / Offline 可加入"
		}
	}
	writeJSON(w, 200, map[string]any{
		"ok": ok, "response": resp,
		"node_id": body.NodeID, "address": addr, "state": state,
		"hint": hint,
	})
}

func (s *Server) handleScaleIn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		writeErr(w, 400, fmt.Errorf("node_id required"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "STORE_DELETE "+body.NodeID)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":       strings.HasPrefix(resp, "OK"),
		"response": resp,
		"hint":     "Draining; RegionScheduler migrates replicas then marks Tombstone",
	})
}

func (s *Server) handleScaleInStatus(w http.ResponseWriter, r *http.Request) {
	nodeID := strings.TrimSpace(r.URL.Query().Get("node_id"))
	if nodeID == "" {
		writeErr(w, 400, fmt.Errorf("node_id required"))
		return
	}
	code, regions, leaders, raw, err := cluster.StoreStatus(s.MetaAddrs, nodeID)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":         strings.HasPrefix(raw, "OK"),
		"node_id":    nodeID,
		"state":      cluster.NodeStateName(code),
		"state_code": code,
		"regions":    regions,
		"leaders":    leaders,
		"raw":        raw,
	})
}

func (s *Server) handleRemoveTombstone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		writeErr(w, 400, fmt.Errorf("node_id required"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "KICK_NODE "+body.NodeID)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "response": resp})
}

func decodeNodeID(r *http.Request) (string, error) {
	var body struct {
		NodeID string `json:"node_id"`
		Name   string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return "", err
	}
	id := strings.TrimSpace(body.NodeID)
	if id == "" {
		id = strings.TrimSpace(body.Name)
	}
	if id == "" {
		return "", fmt.Errorf("node_id required")
	}
	return id, nil
}

// handleNodeOnline：上线 = 进程 start（若停）+ Joining/Offline → JOIN_NODE。
func (s *Server) handleNodeOnline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	nodeID, err := decodeNodeID(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	logs := []string{}
	if s.RunDir != "" {
		if err := lifecycle.Start(s.root(), s.RunDir, nodeID); err != nil {
			logs = append(logs, "start: "+err.Error())
		} else {
			logs = append(logs, "start: ok")
			time.Sleep(400 * time.Millisecond)
		}
	}
	// 查状态：Joining / Offline 则 JOIN
	code, _, _, stRaw, stErr := cluster.StoreStatus(s.MetaAddrs, nodeID)
	if stErr == nil {
		logs = append(logs, stRaw)
	}
	name := cluster.NodeStateName(code)
	joinResp := ""
	// 0=Joining 3=Offline
	if strings.EqualFold(name, "Joining") || strings.EqualFold(name, "Offline") || code == 0 || code == 3 {
		joinResp, err = cluster.MetaRPC(s.MetaAddrs, "JOIN_NODE "+nodeID)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		logs = append(logs, joinResp)
	}
	ok := true
	if joinResp != "" && !strings.HasPrefix(joinResp, "OK") {
		ok = false
	}
	writeJSON(w, 200, map[string]any{
		"ok": ok, "node_id": nodeID, "logs": logs,
		"hint": "上线：拉起进程；待加入/Offline 时 JOIN → Active",
	})
}

// handleNodeOffline：下线 = Active→STORE_DELETE(Draining)；Joining 仅停进程。
func (s *Server) handleNodeOffline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	nodeID, err := decodeNodeID(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	logs := []string{}
	code, regions, _, stRaw, stErr := cluster.StoreStatus(s.MetaAddrs, nodeID)
	if stErr == nil {
		logs = append(logs, stRaw)
	}
	name := cluster.NodeStateName(code)
	// 1=Active 3=Offline：进入 Draining 迁出副本（进程先不停）
	if strings.EqualFold(name, "Active") || strings.EqualFold(name, "Offline") || code == 1 || code == 3 {
		resp, e := cluster.MetaRPC(s.MetaAddrs, "STORE_DELETE "+nodeID)
		if e != nil {
			writeErr(w, 502, e)
			return
		}
		logs = append(logs, resp)
		writeJSON(w, 200, map[string]any{
			"ok": strings.HasPrefix(resp, "OK"), "node_id": nodeID, "logs": logs, "regions": regions,
			"hint": "已下线(Draining)：调度迁出副本 → Tombstone 后可踢出；勿强杀进程",
		})
		return
	}
	// Joining / Draining / Tombstone：停进程
	if s.RunDir == "" {
		writeErr(w, 400, fmt.Errorf("需要 -run-dir 才能停进程"))
		return
	}
	if err := lifecycle.Stop(s.root(), s.RunDir, nodeID, true); err != nil {
		writeErr(w, 500, err)
		return
	}
	logs = append(logs, "stop: ok")
	writeJSON(w, 200, map[string]any{
		"ok": true, "node_id": nodeID, "logs": logs,
		"hint": "进程已停；无副本后可踢出",
	})
}

// handleNodeKick：踢出 = Meta 擦除元数据（须非 Active 且无副本）+ 停进程。
func (s *Server) handleNodeKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	nodeID, err := decodeNodeID(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	code, regions, _, stRaw, stErr := cluster.StoreStatus(s.MetaAddrs, nodeID)
	logs := []string{}
	if stErr == nil {
		logs = append(logs, stRaw)
	}
	name := cluster.NodeStateName(code)
	if strings.EqualFold(name, "Active") || code == 1 {
		writeErr(w, 400, fmt.Errorf("仍在用，须先下线"))
		return
	}
	if regions > 0 {
		writeErr(w, 400, fmt.Errorf("仍有 %d 个 Region，等迁空后再踢出", regions))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "KICK_NODE "+nodeID)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	logs = append(logs, resp)
	if s.RunDir != "" {
		_ = lifecycle.Stop(s.root(), s.RunDir, nodeID, true)
		logs = append(logs, "stop: done")
	}
	writeJSON(w, 200, map[string]any{
		"ok": strings.HasPrefix(resp, "OK"), "node_id": nodeID, "logs": logs,
		"hint": "已踢出集群元数据",
	})
}

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		Count int `json:"count"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Count <= 0 {
		body.Count = 1
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("BALANCE %d", body.Count))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "response": resp})
}

func (s *Server) handleSafePoint(w http.ResponseWriter, r *http.Request) {
	safe, next, hint, err := cluster.GetSafePoint(s.MetaAddrs)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"gc_safe_ts": safe, "next_ts": next, "min_start_hint": hint})
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	type item struct {
		Role    string `json:"role"`
		Address string `json:"address"`
		OK      bool   `json:"ok"`
		Detail  string `json:"detail"`
	}
	var items []item
	for _, m := range s.MetaAddrs {
		resp, err := cluster.TCPLine(m, "PING", time.Second)
		if err != nil {
			items = append(items, item{"meta", m, false, err.Error()})
		} else {
			items = append(items, item{"meta", m, strings.HasPrefix(resp, "OK"), resp})
		}
	}
	for _, st := range s.Storages {
		resp, err := cluster.TCPLine(st, "PING", time.Second)
		if err != nil {
			items = append(items, item{"storage", st, false, err.Error()})
		} else {
			items = append(items, item{"storage", st, strings.HasPrefix(resp, "OK"), resp})
		}
	}
	gw := cluster.TCPUp(s.Gateway, 400)
	detail := "DOWN"
	if gw {
		detail = "UP"
	}
	items = append(items, item{"gateway", s.Gateway, gw, detail})
	writeJSON(w, 200, map[string]any{"items": items})
}

func (s *Server) handleLocate(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, 400, fmt.Errorf("key required"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "LOCATE "+hex.EncodeToString([]byte(key)))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"key": key, "response": resp})
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	stale := r.URL.Query().Get("stale") == "1"
	if key == "" {
		writeErr(w, 400, fmt.Errorf("key required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	cli, err := kv.Dial(ctx, s.Gateway, kv.WithTimeout(15*time.Second))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	defer cli.Close()
	var val []byte
	var found bool
	var rev uint64
	if stale {
		val, found, rev, err = cli.GetOpt(ctx, []byte(key), false)
	} else {
		val, found, rev, err = cli.Get(ctx, []byte(key))
	}
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"key": key, "found": found, "value": string(val), "revision": rev, "stale": stale})
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.Key == "" {
		writeErr(w, 400, fmt.Errorf("key required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	cli, err := kv.Dial(ctx, s.Gateway, kv.WithTimeout(25*time.Second))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	defer cli.Close()
	rev, err := cli.Put(ctx, []byte(body.Key), []byte(body.Value))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "revision": rev})
}

func (s *Server) handleSplit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		RegionID uint32 `json:"region_id"`
		SplitKey string `json:"split_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.SplitKey == "" {
		writeErr(w, 400, fmt.Errorf("split_key required"))
		return
	}
	hx := hex.EncodeToString([]byte(body.SplitKey))
	resp, err := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("SPLIT_REGION %d %s", body.RegionID, hx))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	logs := []string{resp}
	parts := strings.Fields(resp)
	// Split 仅打左 Region 当前副本（禁止对全量 Storages 广播，否则 3/4/5 会误建残缺 Raft）。
	if len(parts) >= 4 && parts[0] == "OK" && parts[1] == "SPLIT" {
		left, right := parts[2], parts[3]
		leftID64, _ := strconv.ParseUint(left, 10, 32)
		peerAddrs := s.regionPeerAddrs(uint32(leftID64))
		if len(peerAddrs) == 0 {
			peerAddrs = append([]string{}, s.Storages...)
			logs = append(logs, "warn: no left peers in route; fallback all storages")
		}
		prep := fmt.Sprintf("ADMIN_PREPARE_SPLIT %s %s", left, hx)
		admin := fmt.Sprintf("ADMIN_SPLIT %s %s %s", left, right, hx)
		if out, e := cluster.StorageRPC(peerAddrs, prep, 8*time.Second); e != nil {
			logs = append(logs, "prepare: "+e.Error())
		} else {
			logs = append(logs, "prepare: "+out)
		}
		if out, e := cluster.StorageRPC(peerAddrs, admin, 20*time.Second); e != nil {
			logs = append(logs, "split: "+e.Error())
		} else {
			logs = append(logs, "split: "+out)
		}
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "logs": logs})
}

func (s *Server) regionPeerAddrs(regionID uint32) []string {
	table, err := cluster.FetchRoute(s.MetaAddrs)
	if err != nil {
		return nil
	}
	for _, reg := range table.Regions {
		if reg.ID != regionID {
			continue
		}
		addrs := make([]string, 0, len(reg.Replicas))
		// Leader 优先
		for _, rep := range reg.Replicas {
			if rep.IsLeader && rep.Address != "" {
				addrs = append(addrs, rep.Address)
			}
		}
		for _, rep := range reg.Replicas {
			if rep.Address == "" || rep.IsLeader {
				continue
			}
			addrs = append(addrs, rep.Address)
		}
		return addrs
	}
	return nil
}

func (s *Server) regionPeerIDs(regionID uint32) []string {
	table, err := cluster.FetchRoute(s.MetaAddrs)
	if err != nil {
		return nil
	}
	for _, reg := range table.Regions {
		if reg.ID != regionID {
			continue
		}
		ids := make([]string, 0, len(reg.Replicas))
		for _, rp := range reg.Replicas {
			ids = append(ids, rp.NodeID)
		}
		return ids
	}
	return nil
}

// regionPeerIsVoter：ok=false 表示路由里没有该 peer；ok=true 且 isVoter=false 表示仅 Learner。
func (s *Server) regionPeerIsVoter(regionID uint32, nodeID string) (isVoter bool, ok bool) {
	table, err := cluster.FetchRoute(s.MetaAddrs)
	if err != nil {
		return false, false
	}
	for _, reg := range table.Regions {
		if reg.ID != regionID {
			continue
		}
		for _, rp := range reg.Replicas {
			if rp.NodeID != nodeID {
				continue
			}
			return !rp.IsLearner, true
		}
		return false, false
	}
	return false, false
}

func (s *Server) suggestPeers(avoid []string) ([]string, string, error) {
	req := "SUGGEST_PLACE"
	if len(avoid) > 0 {
		req += " avoid=" + strings.Join(avoid, ",")
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, req)
	if err != nil {
		return nil, "", err
	}
	parts := strings.Fields(resp)
	peers := []string{}
	if len(parts) >= 3 && parts[0] == "OK" {
		n, _ := strconv.Atoi(parts[2])
		for i := 0; i < n && 3+i < len(parts); i++ {
			peers = append(peers, parts[3+i])
		}
	}
	return peers, resp, nil
}

func (s *Server) placeRegion(regionID uint32, peers []string) (string, error) {
	if len(peers) == 0 {
		return "", fmt.Errorf("peers required")
	}
	line := fmt.Sprintf("ADMIN_PLACE_REGION %d %s", regionID, strings.Join(peers, ","))
	var last string
	for _, addr := range s.Storages {
		out, err := cluster.TCPLine(addr, line, 60*time.Second)
		if err != nil {
			last = err.Error()
			continue
		}
		last = out
		if strings.HasPrefix(out, "OK") {
			return out, nil
		}
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("PLACE_REGION %d %s", regionID, strings.Join(peers, ",")))
	if err != nil {
		return last, fmt.Errorf("place failed: %s / meta: %v", last, err)
	}
	return resp + " (meta-only fallback)", nil
}

func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		Left  uint32 `json:"left_id"`
		Right uint32 `json:"right_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	logs := []string{}
	admin := fmt.Sprintf("ADMIN_MERGE %d %d", body.Left, body.Right)
	for _, addr := range s.Storages {
		if out, e := cluster.TCPLine(addr, admin, 15*time.Second); e != nil {
			logs = append(logs, addr+": "+e.Error())
		} else {
			logs = append(logs, addr+": "+out)
		}
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("MERGE_REGIONS %d %d", body.Left, body.Right))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	logs = append(logs, resp)
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "logs": logs})
}

func (s *Server) handlePlace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		RegionID uint32   `json:"region_id"`
		Peers    []string `json:"peers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	detail, err := s.placeRegion(body.RegionID, body.Peers)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(detail, "OK"), "detail": detail})
}

func (s *Server) handleSuggestPlace(w http.ResponseWriter, r *http.Request) {
	avoidQ := r.URL.Query().Get("avoid")
	var avoid []string
	if avoidQ != "" {
		for _, p := range strings.Split(avoidQ, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				avoid = append(avoid, p)
			}
		}
	}
	if rid := r.URL.Query().Get("region_id"); rid != "" {
		if id64, e := strconv.ParseUint(rid, 10, 32); e == nil {
			avoid = append(avoid, s.regionPeerIDs(uint32(id64))...)
		}
	}
	peers, raw, err := s.suggestPeers(avoid)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"peers": peers, "raw": raw, "avoid": avoid})
}

// handleSuggestReplica：给 Region 推荐可加副本的 Active Store（排除已有副本）。
func (s *Server) handleSuggestReplica(w http.ResponseWriter, r *http.Request) {
	ridQ := strings.TrimSpace(r.URL.Query().Get("region_id"))
	if ridQ == "" {
		writeErr(w, 400, fmt.Errorf("region_id required"))
		return
	}
	id64, err := strconv.ParseUint(ridQ, 10, 32)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("bad region_id"))
		return
	}
	have := map[string]bool{}
	for _, id := range s.regionPeerIDs(uint32(id64)) {
		have[id] = true
	}
	nodes, _, err := cluster.ListNodes(s.MetaAddrs, "storage")
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	candidates := make([]map[string]any, 0)
	for _, n := range nodes {
		if !strings.EqualFold(n.State, "Active") {
			continue
		}
		if have[n.ID] {
			continue
		}
		candidates = append(candidates, map[string]any{
			"id": n.ID, "address": n.Address, "up": n.Up,
		})
	}
	suggested := ""
	if len(candidates) > 0 {
		suggested = candidates[0]["id"].(string)
	}
	writeJSON(w, 200, map[string]any{
		"region_id":  uint32(id64),
		"candidates": candidates,
		"suggested":  suggested,
		"have_peers": s.regionPeerIDs(uint32(id64)),
	})
}

// scaleStateLabel maps Scaler ScaleTaskState int → UI text.
func scaleStateLabel(state int) string {
	switch state {
	case 0:
		return "Pending"
	case 1:
		return "AddingLearner"
	case 2:
		return "CatchingUp"
	case 3:
		return "Promoting"
	case 4:
		return "UpdatingRoute"
	case 5:
		return "Completed"
	case 6:
		return "Failed"
	default:
		return fmt.Sprintf("state_%d", state)
	}
}

func scaleStateZh(state int) string {
	switch state {
	case 0:
		return "排队中"
	case 1:
		return "添加 Learner"
	case 2:
		return "追平中"
	case 3:
		return "提升 Voter"
	case 4:
		return "更新路由"
	case 5:
		return "已完成"
	case 6:
		return "失败"
	default:
		return scaleStateLabel(state)
	}
}

// parseScaleKV extracts key=value tokens and positional OK SCALE fields.
func parseGetScale(raw string) map[string]any {
	out := map[string]any{"raw": raw, "ok": strings.HasPrefix(raw, "OK")}
	parts := strings.Fields(raw)
	if len(parts) < 3 || parts[0] != "OK" || parts[1] != "SCALE" {
		return out
	}
	out["task_id"] = parts[2]
	if len(parts) >= 4 {
		if st, err := strconv.Atoi(parts[3]); err == nil {
			out["state"] = st
			out["state_name"] = scaleStateLabel(st)
			out["state_zh"] = scaleStateZh(st)
			out["done"] = st == 5 || st == 6
			out["failed"] = st == 6
			out["catching_up"] = st == 2
		}
	}
	if len(parts) >= 5 {
		if p, err := strconv.ParseFloat(parts[4], 64); err == nil {
			out["progress"] = p
			out["progress_pct"] = int(p*100 + 0.5)
		}
	}
	for _, tok := range parts[5:] {
		kv := strings.SplitN(tok, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k, v := kv[0], kv[1]
		switch k {
		case "node":
			out["node_id"] = v
		case "shards":
			var shards []uint32
			for _, s := range strings.Split(v, ",") {
				if s == "" {
					continue
				}
				if n, e := strconv.ParseUint(s, 10, 32); e == nil {
					shards = append(shards, uint32(n))
				}
			}
			out["shards"] = shards
		case "match":
			if n, e := strconv.ParseUint(v, 10, 64); e == nil {
				out["match"] = n
			}
		case "commit":
			if n, e := strconv.ParseUint(v, 10, 64); e == nil {
				out["commit"] = n
			}
		case "caught":
			out["caught"] = v == "1"
		case "error":
			out["error"] = v
		}
	}
	// 追平百分比：有 match/commit 时用日志进度，否则用任务 progress
	if match, okM := out["match"].(uint64); okM {
		if commit, okC := out["commit"].(uint64); okC && commit > 0 {
			pct := int(float64(match) / float64(commit) * 100)
			if pct > 100 {
				pct = 100
			}
			out["catchup_pct"] = pct
		}
	}
	if caught, ok := out["caught"].(bool); ok && caught {
		out["catchup_pct"] = 100
	}
	return out
}

func (s *Server) handleScaleTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("task_id"))
	if id == "" {
		id = strings.TrimSpace(r.URL.Query().Get("id"))
	}
	if id == "" {
		writeErr(w, 400, fmt.Errorf("task_id required"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "GET_SCALE "+id)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, parseGetScale(resp))
}

func (s *Server) handleScaleTasks(w http.ResponseWriter, r *http.Request) {
	resp, err := cluster.MetaRPC(s.MetaAddrs, "LIST_SCALE")
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	parts := strings.Fields(resp)
	tasks := make([]map[string]any, 0)
	// OK SCALE_LIST n id:node:state:progress[:shards] …
	if len(parts) >= 3 && parts[0] == "OK" {
		for _, tok := range parts[3:] {
			fields := strings.Split(tok, ":")
			if len(fields) < 3 {
				continue
			}
			st, _ := strconv.Atoi(fields[2])
			item := map[string]any{
				"task_id":    fields[0],
				"node_id":    fields[1],
				"state":      st,
				"state_name": scaleStateLabel(st),
				"state_zh":   scaleStateZh(st),
				"done":       st == 5 || st == 6,
				"failed":     st == 6,
				"active":     st < 5,
			}
			if st == 5 {
				item["progress"] = 1.0
				item["progress_pct"] = 100
			}
			if len(fields) >= 4 {
				if p, e := strconv.ParseFloat(fields[3], 64); e == nil {
					item["progress"] = p
					item["progress_pct"] = int(p*100 + 0.5)
				}
			}
			if len(fields) >= 5 && fields[4] != "" {
				var shards []uint32
				for _, s := range strings.Split(fields[4], ",") {
					if n, e := strconv.ParseUint(s, 10, 32); e == nil {
						shards = append(shards, uint32(n))
					}
				}
				item["shards"] = shards
			}
			tasks = append(tasks, item)
		}
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "raw": resp, "tasks": tasks})
}

func (s *Server) findRegion(regionID uint32) (*cluster.Region, error) {
	table, err := cluster.FetchRoute(s.MetaAddrs)
	if err != nil {
		return nil, err
	}
	for i := range table.Regions {
		if table.Regions[i].ID == regionID {
			return &table.Regions[i], nil
		}
	}
	return nil, fmt.Errorf("region %d not found", regionID)
}

func (s *Server) storageRPC(addrs []string, line string, timeout time.Duration) (string, error) {
	var last error
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		out, err := cluster.TCPLine(addr, line, timeout)
		if err != nil {
			last = err
			continue
		}
		if strings.HasPrefix(out, "OK") {
			return out, nil
		}
		last = fmt.Errorf("%s", out)
		if strings.Contains(out, "not_leader") {
			continue
		}
	}
	if last == nil {
		last = fmt.Errorf("no storage")
	}
	return "", last
}

func decodeReplicaBody(r *http.Request) (regionID uint32, nodeID, address string, err error) {
	var body struct {
		RegionID uint32 `json:"region_id"`
		NodeID   string `json:"node_id"`
		Address  string `json:"address"`
	}
	if err = json.NewDecoder(r.Body).Decode(&body); err != nil {
		return 0, "", "", err
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		return 0, "", "", fmt.Errorf("node_id required")
	}
	return body.RegionID, body.NodeID, strings.TrimSpace(body.Address), nil
}

// handleReplicaOnline：给本 Region 在指定 Store 上增加副本（≠ 整机上线）。
func (s *Server) handleReplicaOnline(w http.ResponseWriter, r *http.Request) {
	s.handleAddReplica(w, r)
}

// handleReplicaOffline：从本 Region 成员中摘掉该副本（REMOVE_PEER + Meta PLACE），不 DESTROY、不动整机。
// 快速「加副本→立刻下线」：先 CANCEL_SCALE，再短等 Learner 进路由（含 *N），再 REMOVE。
func (s *Server) handleReplicaOffline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	regionID, nodeID, _, err := decodeReplicaBody(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	logs := []string{}
	// 取消该节点在本 Region 上未完成的 SCALE_OUT，避免追平中途再 Promote
	if cancelOut, ce := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("CANCEL_SCALE %s %d", nodeID, regionID)); ce != nil {
		logs = append(logs, "cancel_scale: "+ce.Error())
	} else {
		logs = append(logs, "cancel_scale: "+cancelOut)
	}
	// 短等路由出现目标（voter 或 learner）；SCALE_OUT 异步写 Meta
	var reg *cluster.Region
	var target *cluster.Replica
	var targetCopy cluster.Replica
	deadline := time.Now().Add(5 * time.Second)
	for {
		reg, err = s.findRegion(regionID)
		if err != nil {
			writeErr(w, 404, err)
			return
		}
		target = nil
		for i := range reg.Replicas {
			if reg.Replicas[i].NodeID == nodeID {
				targetCopy = reg.Replicas[i]
				target = &targetCopy
				break
			}
		}
		if target != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	var remaining []string
	var seedAddrs []string
	leaderAddr := ""
	for i := range reg.Replicas {
		rep := &reg.Replicas[i]
		seedAddrs = append(seedAddrs, rep.Address)
		if rep.IsLeader {
			leaderAddr = rep.Address
		}
		if rep.NodeID == nodeID {
			continue
		}
		remaining = append(remaining, rep.NodeID)
	}
	if target == nil {
		// 路由从未出现（扩容刚入队就被下线）：强制 REMOVE + PLACE 清半吊子
		logs = append(logs, fmt.Sprintf("warn: %s 未在 R%d 路由中，尝试强制 REMOVE", nodeID, regionID))
		if len(remaining) < 1 {
			writeJSON(w, 200, map[string]any{
				"ok": false, "logs": logs,
				"hint":      fmt.Sprintf("%s 不是 R%d 的副本且无其它成员可 PLACE", nodeID, regionID),
				"region_id": regionID, "node_id": nodeID,
			})
			return
		}
		rm := fmt.Sprintf("ADMIN_REMOVE_PEER %d %s", regionID, nodeID)
		addrs := append(append([]string{}, seedAddrs...), s.Storages...)
		if out, e := s.storageRPC(addrs, rm, 8*time.Second); e != nil {
			logs = append(logs, "remove: "+e.Error())
		} else {
			logs = append(logs, "remove: "+out)
		}
		place := fmt.Sprintf("PLACE_REGION %d %s", regionID, strings.Join(remaining, ","))
		if metaOut, metaErr := cluster.MetaRPC(s.MetaAddrs, place); metaErr != nil {
			logs = append(logs, "meta: "+metaErr.Error())
		} else {
			logs = append(logs, "meta: "+metaOut)
		}
		writeJSON(w, 200, map[string]any{
			"ok": true, "logs": logs, "region_id": regionID, "node_id": nodeID,
			"remaining": remaining, "pending_kick": true,
			"hint": "已取消扩容并强制摘除；请再「踢出」清本地数据",
		})
		return
	}
	if len(remaining) < 1 {
		writeErr(w, 400, fmt.Errorf("不能下线唯一副本；请先增加其他副本"))
		return
	}
	// Leader 被摘 → 先转主
	if target.IsLeader {
		xfer := fmt.Sprintf("ADMIN_TRANSFER_LEADER %d %s", regionID, remaining[0])
		out, e := s.storageRPC(seedAddrs, xfer, 8*time.Second)
		if e != nil {
			logs = append(logs, "transfer: "+e.Error())
		} else {
			logs = append(logs, "transfer: "+out)
		}
		camp := fmt.Sprintf("ADMIN_CAMPAIGN %d", regionID)
		for _, rep := range reg.Replicas {
			if rep.NodeID == remaining[0] {
				if o, e2 := cluster.TCPLine(rep.Address, camp, 3*time.Second); e2 != nil {
					logs = append(logs, "campaign: "+e2.Error())
				} else {
					logs = append(logs, "campaign: "+o)
				}
				break
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
	rm := fmt.Sprintf("ADMIN_REMOVE_PEER %d %s", regionID, nodeID)
	addrs := seedAddrs
	if leaderAddr != "" {
		addrs = append([]string{leaderAddr}, seedAddrs...)
	}
	var out string
	var e error
	for attempt := 0; attempt < 12; attempt++ {
		out, e = s.storageRPC(addrs, rm, 15*time.Second)
		if e == nil && strings.HasPrefix(out, "OK") {
			break
		}
		errMsg := ""
		if e != nil {
			errMsg = e.Error()
		} else {
			errMsg = out
		}
		if strings.Contains(errMsg, "busy") || strings.Contains(errMsg, "joint") ||
			strings.Contains(errMsg, "not_leader") {
			logs = append(logs, fmt.Sprintf("remove retry %d: %s", attempt+1, errMsg))
			time.Sleep(400 * time.Millisecond)
			continue
		}
		break
	}
	if e != nil || !strings.HasPrefix(out, "OK") {
		msg := out
		if e != nil {
			msg = e.Error()
		}
		writeJSON(w, 200, map[string]any{
			"ok": false, "logs": append(logs, "remove: "+msg),
			"hint": "REMOVE_PEER 失败", "region_id": regionID, "node_id": nodeID,
		})
		return
	}
	logs = append(logs, "remove: "+out)
	time.Sleep(500 * time.Millisecond) // 等 LeaveJoint
	// Meta 发布剩余成员（不 DESTROY，本地数据仍在，可「踢出」清盘）
	place := fmt.Sprintf("PLACE_REGION %d %s", regionID, strings.Join(remaining, ","))
	metaOut, metaErr := cluster.MetaRPC(s.MetaAddrs, place)
	if metaErr != nil {
		logs = append(logs, "meta: "+metaErr.Error())
	} else {
		logs = append(logs, "meta: "+metaOut)
	}
	writeJSON(w, 200, map[string]any{
		"ok": true, "logs": logs,
		"region_id": regionID, "node_id": nodeID, "address": target.Address,
		"remaining": remaining, "pending_kick": true,
		"hint": "已从 R" + fmt.Sprint(regionID) + " 成员摘除 " + nodeID + "；本地数据仍在，请再「踢出」销毁该 Region 副本",
	})
}

// handleReplicaKick：销毁本机该 Region 数据（须已不在成员表）；≠ 踢出整机。
func (s *Server) handleReplicaKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	regionID, nodeID, addr, err := decodeReplicaBody(r)
	if err != nil {
		writeErr(w, 400, err)
		return
	}
	logs := []string{}
	if reg, e := s.findRegion(regionID); e == nil {
		for _, rep := range reg.Replicas {
			if rep.NodeID == nodeID {
				writeErr(w, 400, fmt.Errorf("仍是 R%d 成员，须先下线该副本", regionID))
				return
			}
		}
	}
	if addr == "" {
		nodes, _, _ := cluster.ListNodes(s.MetaAddrs, "storage")
		for _, n := range nodes {
			if n.ID == nodeID {
				addr = n.Address
				break
			}
		}
	}
	if addr == "" {
		writeErr(w, 400, fmt.Errorf("找不到 %s 地址", nodeID))
		return
	}
	out, e := cluster.TCPLine(addr, fmt.Sprintf("ADMIN_DESTROY_REGION %d", regionID), 8*time.Second)
	if e != nil {
		writeJSON(w, 200, map[string]any{
			"ok": false, "logs": []string{"destroy: " + e.Error()},
			"region_id": regionID, "node_id": nodeID,
		})
		return
	}
	logs = append(logs, "destroy: "+out)
	writeJSON(w, 200, map[string]any{
		"ok": strings.HasPrefix(out, "OK"), "logs": logs,
		"region_id": regionID, "node_id": nodeID,
		"hint": "已销毁该 Store 上 R" + fmt.Sprint(regionID) + " 的本地副本（整机仍在集群）",
	})
}

// handleAddReplica：SCALE_OUT <node> <region_id> — 给指定 Region 增加一个副本（抬 RF）。
func (s *Server) handleAddReplica(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		RegionID uint32 `json:"region_id"`
		NodeID   string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		writeErr(w, 400, fmt.Errorf("node_id required"))
		return
	}
	// 仅拒绝已是 voter 的重复加副本；失败任务留下的 *N Learner 允许再走 SCALE_OUT 补 ConfChange。
	if isVoter, ok := s.regionPeerIsVoter(body.RegionID, body.NodeID); ok && isVoter {
		writeErr(w, 400, fmt.Errorf("%s already has region %d as voter", body.NodeID, body.RegionID))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, fmt.Sprintf("SCALE_OUT %s %d", body.NodeID, body.RegionID))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	ok := strings.HasPrefix(resp, "OK")
	hint := "SCALE_OUT 已入队：Learner → 追平 → Promote（会抬副本数）；忙时自动排队，用 /api/scale-task 看进度"
	state := -1
	taskID := ""
	// OK SCALE <task_id> <state> [error]
	parts := strings.Fields(resp)
	if len(parts) >= 3 && parts[0] == "OK" && parts[1] == "SCALE" {
		taskID = parts[2]
		if len(parts) >= 4 {
			if st, e := strconv.Atoi(parts[3]); e == nil {
				state = st
			}
		}
		// state=6 Failed（duplicate_inflight / unknown_node…）不能当成功
		if state == 6 {
			ok = false
			errMsg := ""
			if len(parts) >= 5 {
				errMsg = parts[4]
			}
			hint = "加副本失败"
			if errMsg != "" {
				hint += ": " + errMsg
			}
		}
	}
	if !ok && state < 0 {
		hint = "加副本失败；确认目标 Store 为 Active"
	}
	out := map[string]any{
		"ok": ok, "response": resp,
		"region_id": body.RegionID, "node_id": body.NodeID,
		"hint": hint,
	}
	if taskID != "" {
		out["task_id"] = taskID
	}
	if state >= 0 {
		out["state"] = state
		out["state_zh"] = scaleStateZh(state)
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleFindMergeRight(w http.ResponseWriter, r *http.Request) {
	left := r.URL.Query().Get("left_id")
	if left == "" {
		writeErr(w, 400, fmt.Errorf("left_id required"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "FIND_MERGE_RIGHT "+left)
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	parts := strings.Fields(resp)
	out := map[string]any{"raw": resp, "ok": strings.HasPrefix(resp, "OK")}
	if len(parts) >= 4 && parts[0] == "OK" {
		lid, _ := strconv.ParseUint(parts[2], 10, 32)
		rid, _ := strconv.ParseUint(parts[3], 10, 32)
		out["left_id"] = uint32(lid)
		out["right_id"] = uint32(rid)
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleAdvanceSafe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	resp, err := cluster.MetaRPC(s.MetaAddrs, "ADVANCE_SAFE_POINT")
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": strings.HasPrefix(resp, "OK"), "response": resp})
}

func (s *Server) handleProcesses(w http.ResponseWriter, r *http.Request) {
	if s.RunDir == "" {
		writeJSON(w, 200, map[string]any{"available": false, "processes": []any{}})
		return
	}
	names := []string{"meta-0", "meta-1", "meta-2", "storage-0", "storage-1", "storage-2", "gateway", "web"}
	type proc struct {
		Name  string `json:"name"`
		PID   string `json:"pid"`
		Alive bool   `json:"alive"`
	}
	var list []proc
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(s.RunDir, n+".pid"))
		p := proc{Name: n}
		if err == nil {
			p.PID = strings.TrimSpace(string(b))
			p.Alive = tcpProbeFor(n)
		}
		list = append(list, p)
	}
	writeJSON(w, 200, map[string]any{"available": true, "run_dir": s.RunDir, "processes": list})
}

func tcpProbeFor(name string) bool {
	ports := map[string]string{
		"meta-0": "127.0.0.1:8500", "meta-1": "127.0.0.1:8501", "meta-2": "127.0.0.1:8502",
		"storage-0": "127.0.0.1:9100", "storage-1": "127.0.0.1:9101", "storage-2": "127.0.0.1:9102",
		"gateway": "127.0.0.1:19000", "web": "127.0.0.1:19100",
	}
	if a, ok := ports[name]; ok {
		return cluster.TCPUp(a, 300)
	}
	return false
}
