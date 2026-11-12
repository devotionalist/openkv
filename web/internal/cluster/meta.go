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

// Package cluster：Web 管控台访问 Meta / Storage（类型化 gRPC）。
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

	metav1 "github.com/devotionalist/openkv/client/go/gen/meta/v1"
	storagev1 "github.com/devotionalist/openkv/client/go/gen/storage/v1"
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

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func followRedirect(body string) (string, bool) {
	if !strings.Contains(body, "not_leader") {
		return "", false
	}
	i := strings.Index(body, "redirect=")
	if i < 0 {
		return "", false
	}
	cur := strings.TrimSpace(body[i+9:])
	if j := strings.IndexAny(cur, " \t"); j >= 0 {
		cur = cur[:j]
	}
	return cur, cur != ""
}

func simpleMsg(ok bool, msg, err string) string {
	if msg != "" {
		return msg
	}
	if err != "" {
		return err
	}
	if ok {
		return "OK"
	}
	return "ERR"
}

// TCPLine / StorageRPC：运维行命令 → AdminService 类型化 RPC（兼容历史调用方）。
func TCPLine(addr, req string, timeout time.Duration) (string, error) {
	return storageTyped(addr, req, timeout)
}

func storageTyped(addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = withAdminToken(ctx)
	conn, err := dial(addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	cli := storagev1.NewAdminServiceClient(conn)
	parts := strings.Fields(req)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty")
	}
	op := parts[0]
	switch op {
	case "PING":
		_, err := cli.Ping(ctx, &storagev1.PingRequest{})
		if err != nil {
			return "", err
		}
		return "OK PONG", nil
	case "STATS":
		resp, err := cli.Stats(ctx, &storagev1.StatsRequest{})
		if err != nil {
			return "", err
		}
		return formatStorageStats(resp), nil
	case "ADMIN_CAMPAIGN":
		if len(parts) < 2 {
			return "", fmt.Errorf("usage")
		}
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.Campaign(ctx, &storagev1.CampaignRequest{RegionId: uint32(rid)})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_DESTROY_REGION":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.DestroyRegion(ctx, &storagev1.DestroyRegionRequest{RegionId: uint32(rid)})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_PROMOTE":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.Promote(ctx, &storagev1.PeerOpRequest{RegionId: uint32(rid), NodeId: parts[2]})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_REMOVE_PEER":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.RemovePeer(ctx, &storagev1.PeerOpRequest{RegionId: uint32(rid), NodeId: parts[2]})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_PEER_PROGRESS":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.PeerProgress(ctx, &storagev1.PeerOpRequest{RegionId: uint32(rid), NodeId: parts[2]})
		if err != nil {
			return "", err
		}
		if resp.Message != "" {
			return resp.Message, nil
		}
		caught, learner := 0, 0
		if resp.CaughtUp {
			caught = 1
		}
		if resp.IsLearner {
			learner = 1
		}
		return fmt.Sprintf("OK PROGRESS %d %d %d %d %d", resp.MatchIndex, resp.CommitIndex, resp.LastIndex, caught, learner), nil
	case "ADMIN_TRANSFER_LEADER":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.TransferLeader(ctx, &storagev1.TransferRequest{ShardId: uint32(rid), TargetPeer: parts[2]})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_ADD_LEARNER":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		tok := parts[2]
		at := strings.Index(tok, "@")
		if at < 0 {
			return "", fmt.Errorf("bad peer")
		}
		resp, err := cli.AddLearner(ctx, &storagev1.PeerOpRequest{
			RegionId: uint32(rid), NodeId: tok[:at], Address: tok[at+1:],
		})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_ENSURE_REGION":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		r := &storagev1.EnsureRegionRequest{RegionId: uint32(rid)}
		if len(parts) >= 3 {
			for _, item := range strings.Split(parts[2], ",") {
				if item == "" {
					continue
				}
				learner := strings.Contains(item, "*N")
				item = strings.ReplaceAll(strings.ReplaceAll(item, "*N", ""), "*L", "")
				at := strings.Index(item, "@")
				if at < 0 {
					continue
				}
				r.Peers = append(r.Peers, &storagev1.RaftPeer{
					NodeId: item[:at], Address: item[at+1:], Learner: learner,
				})
			}
		}
		resp, err := cli.EnsureRegion(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_PLACE_REGION":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		r := &storagev1.PlaceRegionRequest{RegionId: uint32(rid)}
		for _, id := range strings.Split(parts[2], ",") {
			if id != "" {
				r.DesiredPeers = append(r.DesiredPeers, id)
			}
		}
		resp, err := cli.PlaceRegion(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_PREPARE_SPLIT":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		key, _ := hex.DecodeString(parts[2])
		resp, err := cli.PrepareSplit(ctx, &storagev1.PrepareSplitRequest{LeftRegion: uint32(rid), SplitKey: key})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_SPLIT":
		left, _ := strconv.ParseUint(parts[1], 10, 32)
		right, _ := strconv.ParseUint(parts[2], 10, 32)
		key, _ := hex.DecodeString(parts[3])
		resp, err := cli.Split(ctx, &storagev1.SplitRequest{LeftRegion: uint32(left), RightRegion: uint32(right), SplitKey: key})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_MERGE":
		left, _ := strconv.ParseUint(parts[1], 10, 32)
		right, _ := strconv.ParseUint(parts[2], 10, 32)
		resp, err := cli.Merge(ctx, &storagev1.MergeRequest{LeftRegion: uint32(left), RightRegion: uint32(right)})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, redirectErr(resp.RedirectTo)), nil
	case "ADMIN_REGION_STATS":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.RegionStats(ctx, &storagev1.RegionStatsRequest{RegionId: uint32(rid)})
		if err != nil {
			return "", err
		}
		if resp.Message != "" && resp.Message != "OK" {
			return resp.Message, nil
		}
		return fmt.Sprintf("OK REGION_STATS %d %d %d", resp.KeyCount, resp.ApproxBytes, resp.Epoch), nil
	default:
		return "", fmt.Errorf("unsupported storage op %s", op)
	}
}

func redirectErr(redir string) string {
	if redir == "" {
		return ""
	}
	return "ERR not_leader redirect=" + redir
}

func formatStorageStats(s *storagev1.StatsResponse) string {
	var b strings.Builder
	fmt.Fprintf(&b, "OK STATS requests=%d gw=%d raft=%d sync=%d admin=%d meta_out=%d client=%d qdepth=%d qdrop=%d",
		s.Requests, s.Gw, s.Raft, s.Sync, s.Admin, s.MetaOut, s.Client, s.Qdepth, s.Qdrop)
	emit := func(key string, m map[string]uint64) {
		if len(m) == 0 {
			return
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		first := true
		for k, v := range m {
			if !first {
				b.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&b, "%s:%d", k, v)
		}
	}
	emit("peer_raft", s.PeerRaft)
	emit("peer_sync", s.PeerSync)
	emit("peer_raft_out", s.PeerRaftOut)
	emit("peer_sync_out", s.PeerSyncOut)
	emit("meta_out_peers", s.MetaOutPeers)
	return b.String()
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
			if next, ok := followRedirect(resp); ok {
				cur = next
				continue
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

// MetaRPC 跟随 not_leader redirect（行命令 → MetaService 类型化）。
func MetaRPC(metaAddrs []string, req string) (string, error) {
	var last error
	for _, addr := range metaAddrs {
		cur := addr
		for i := 0; i < 4; i++ {
			resp, e := metaTyped(cur, req, 3*time.Second)
			if e != nil {
				last = e
				break
			}
			if next, ok := followRedirect(resp); ok {
				cur = next
				continue
			}
			return resp, nil
		}
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return "", last
}

func metaTyped(addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = withAdminToken(ctx)
	conn, err := dial(addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	cli := metav1.NewMetaServiceClient(conn)
	parts := strings.Fields(req)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty")
	}
	op := parts[0]
	switch op {
	case "PING":
		resp, err := cli.Ping(ctx, &metav1.PingRequest{})
		if err != nil {
			return "", err
		}
		if resp.Message != "" {
			return resp.Message, nil
		}
		return "OK PONG", nil
	case "WHO_LEADER":
		resp, err := cli.WhoLeader(ctx, &metav1.WhoLeaderRequest{})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("OK LEADER %s %s", resp.LeaderId, resp.Address), nil
	case "GET_ROUTE":
		resp, err := cli.GetRoutingTable(ctx, &metav1.GetRoutingTableRequest{})
		if err != nil {
			return "", err
		}
		return formatRouteLine(resp), nil
	case "GET_SAFE_POINT", "GET_SAFE":
		resp, err := cli.GetSafePoint(ctx, &metav1.GetSafePointRequest{})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("OK SAFE %d %d %d", resp.SafePoint, resp.NextTs, resp.MinStartHint), nil
	case "STATS":
		resp, err := cli.Stats(ctx, &metav1.StatsRequest{})
		if err != nil {
			return "", err
		}
		return formatMetaStats(resp), nil
	case "LIST_NODES":
		r := &metav1.ListNodesRequest{}
		if len(parts) >= 2 {
			if parts[1] == "storage" {
				r.Role = metav1.NodeRole_STORAGE
			} else if parts[1] == "compute" {
				r.Role = metav1.NodeRole_COMPUTE
			}
		}
		resp, err := cli.ListNodes(ctx, r)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "OK NODES %d", len(resp.Nodes))
		for _, n := range resp.Nodes {
			role := "C"
			if n.Role == metav1.NodeRole_STORAGE {
				role = "S"
			}
			state := int(n.State)
			// proto: JOINING=1 ACTIVE=2 …；行协议 0=Joining
			if state > 0 {
				state--
			}
			fmt.Fprintf(&b, " %s@%s:%s:%d", n.NodeId, n.Address, role, state)
		}
		return b.String(), nil
	case "JOIN_NODE", "KICK_NODE", "STORE_DELETE", "DRAIN_NODE", "ACTIVATE_NODE", "OFFLINE_NODE", "REMOVE_TOMBSTONE":
		if len(parts) < 2 {
			return "", fmt.Errorf("usage")
		}
		r := &metav1.NodeIdRequest{NodeId: parts[1]}
		var resp *metav1.SimpleResponse
		var err error
		switch op {
		case "JOIN_NODE":
			resp, err = cli.JoinNode(ctx, r)
		case "KICK_NODE":
			resp, err = cli.KickNode(ctx, r)
		case "STORE_DELETE":
			resp, err = cli.StoreDelete(ctx, r)
		case "DRAIN_NODE":
			resp, err = cli.DrainNode(ctx, r)
		case "ACTIVATE_NODE":
			resp, err = cli.ActivateNode(ctx, r)
		case "OFFLINE_NODE":
			resp, err = cli.OfflineNode(ctx, r)
		case "REMOVE_TOMBSTONE":
			resp, err = cli.RemoveTombstone(ctx, r)
		}
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "BALANCE":
		r := &metav1.BalanceRequest{}
		if len(parts) >= 2 {
			n, _ := strconv.ParseUint(parts[1], 10, 32)
			r.Count = uint32(n)
		}
		resp, err := cli.Balance(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "SPLIT_REGION":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		key, _ := hex.DecodeString(parts[2])
		resp, err := cli.SplitRegion(ctx, &metav1.SplitRegionRequest{RegionId: uint32(rid), SplitKey: key})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return simpleMsg(false, "", resp.Error), nil
		}
		return fmt.Sprintf("OK SPLIT %d %d", rid, resp.RightRegionId), nil
	case "MERGE_REGIONS":
		left, _ := strconv.ParseUint(parts[1], 10, 32)
		right, _ := strconv.ParseUint(parts[2], 10, 32)
		resp, err := cli.MergeRegions(ctx, &metav1.MergeRegionsRequest{LeftRegionId: uint32(left), RightRegionId: uint32(right)})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "PLACE_REGION":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		r := &metav1.PlaceRegionRequest{RegionId: uint32(rid)}
		for _, id := range strings.Split(parts[2], ",") {
			if id != "" {
				r.PeerNodeIds = append(r.PeerNodeIds, id)
			}
		}
		resp, err := cli.PlaceRegion(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "FIND_MERGE_RIGHT":
		left, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.FindMergeRight(ctx, &metav1.FindMergeRightRequest{LeftRegionId: uint32(left)})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return simpleMsg(false, "", resp.Error), nil
		}
		return fmt.Sprintf("OK MERGE_RIGHT %d %d", left, resp.RightRegionId), nil
	case "STORE_STATUS":
		resp, err := cli.StoreStatus(ctx, &metav1.StoreStatusRequest{NodeId: parts[1]})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return simpleMsg(false, "", resp.Error), nil
		}
		if resp.Status != "" {
			return resp.Status, nil
		}
		return "OK STORE " + parts[1], nil
	case "LOCATE":
		key, err := hex.DecodeString(parts[1])
		if err != nil {
			key = []byte(parts[1])
		}
		resp, err := cli.Locate(ctx, &metav1.LocateRequest{Key: key})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return simpleMsg(false, "", resp.Error), nil
		}
		sh := resp.Shard
		return fmt.Sprintf("OK LOCATE %d %s %s %d", sh.ShardId, hex.EncodeToString(sh.StartKey), hex.EncodeToString(sh.EndKey), sh.Epoch), nil
	case "ADVANCE_SAFE_POINT":
		r := &metav1.AdvanceSafePointRequest{}
		if len(parts) >= 2 {
			sp, _ := strconv.ParseUint(parts[1], 10, 64)
			r.SafePoint = sp
		}
		resp, err := cli.AdvanceSafePoint(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "SCALE_OUT":
		r := &metav1.ScaleOutRequest{NewStorageNodeId: parts[1]}
		if len(parts) >= 3 {
			for _, s := range strings.Split(parts[2], ",") {
				if s == "" {
					continue
				}
				id, _ := strconv.ParseUint(s, 10, 32)
				r.ShardIds = append(r.ShardIds, uint32(id))
			}
		}
		resp, err := cli.ScaleOut(ctx, r)
		if err != nil {
			return "", err
		}
		if resp.Message != "" {
			return resp.Message, nil
		}
		if resp.Accepted {
			return "OK SCALE", nil
		}
		return "ERR", nil
	case "CANCEL_SCALE":
		r := &metav1.CancelScaleRequest{NodeId: parts[1]}
		if len(parts) >= 3 {
			rid, _ := strconv.ParseUint(parts[2], 10, 32)
			r.RegionId = uint32(rid)
		}
		resp, err := cli.CancelScale(ctx, r)
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	case "GET_SCALE":
		resp, err := cli.GetScaleTask(ctx, &metav1.GetScaleTaskRequest{TaskId: parts[1]})
		if err != nil {
			return "", err
		}
		var shards []string
		for _, id := range resp.ShardIds {
			shards = append(shards, strconv.FormatUint(uint64(id), 10))
		}
		out := fmt.Sprintf("OK SCALE %s %d %g node=%s shards=%s", resp.TaskId, int(resp.State), resp.Progress, resp.NewNodeId, strings.Join(shards, ","))
		if resp.Error != "" {
			out += " error=" + resp.Error
		}
		return out, nil
	case "LIST_SCALE":
		resp, err := cli.ListScaleTasks(ctx, &metav1.ListScaleTasksRequest{})
		if err != nil {
			return "", err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "OK SCALE_LIST %d", len(resp.Tasks))
		for _, t := range resp.Tasks {
			fmt.Fprintf(&b, " %s:%s:%d:%g", t.TaskId, t.NewNodeId, int(t.State), t.Progress)
			if len(t.ShardIds) > 0 {
				b.WriteByte(':')
				for i, id := range t.ShardIds {
					if i > 0 {
						b.WriteByte(',')
					}
					b.WriteString(strconv.FormatUint(uint64(id), 10))
				}
			}
		}
		return b.String(), nil
	case "BUMP_ROUTE":
		resp, err := cli.BumpRoute(ctx, &metav1.BumpRouteRequest{})
		if err != nil {
			return "", err
		}
		return simpleMsg(resp.Ok, resp.Message, resp.Error), nil
	default:
		return "", fmt.Errorf("unsupported meta op %s", op)
	}
}

func formatRouteLine(rt *metav1.RoutingTable) string {
	var b strings.Builder
	fmt.Fprintf(&b, "OK ROUTE %d %d 0", rt.Version, len(rt.Shards))
	for _, sh := range rt.Shards {
		fmt.Fprintf(&b, " %d:%s:%s:%d:", sh.ShardId, hex.EncodeToString(sh.StartKey), hex.EncodeToString(sh.EndKey), sh.Epoch)
		for i, r := range sh.Replicas {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s@%s", r.NodeId, r.Address)
			if r.IsLeader {
				b.WriteString("*L")
			}
			if r.IsLearner {
				b.WriteString("*N")
			}
		}
	}
	return b.String()
}

func formatMetaStats(s *metav1.StatsResponse) string {
	var b strings.Builder
	fmt.Fprintf(&b, "OK STATS requests=%d gw=%d store=%d raft=%d sync=%d admin=%d client=%d route_ver=%d next_ts=%d tso_alloc=%d watch_waits=%d migrates=%d",
		s.Requests, s.Gw, s.Store, s.Raft, s.Sync, s.Admin, s.Client, s.RouteVer, s.NextTs, s.TsoAlloc, s.WatchWaits, s.Migrates)
	emit := func(key string, m map[string]uint64) {
		if len(m) == 0 {
			return
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		first := true
		for k, v := range m {
			if !first {
				b.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&b, "%s:%d", k, v)
		}
	}
	emit("peer_raft", s.PeerRaft)
	emit("peer_sync", s.PeerSync)
	emit("peer_raft_out", s.PeerRaftOut)
	emit("peer_sync_out", s.PeerSyncOut)
	emit("peer_store", s.PeerStore)
	return b.String()
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
	var last error
	for _, addr := range metaAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = withAdminToken(ctx)
		conn, err := dial(addr)
		if err != nil {
			cancel()
			last = err
			continue
		}
		resp, err := metav1.NewMetaServiceClient(conn).GetRoutingTable(ctx, &metav1.GetRoutingTableRequest{})
		conn.Close()
		cancel()
		if err != nil {
			last = err
			continue
		}
		return routingFromPB(resp), nil
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return nil, last
}

func routingFromPB(rt *metav1.RoutingTable) *RouteTable {
	t := &RouteTable{Version: rt.Version}
	for _, sh := range rt.Shards {
		reg := Region{
			ID: sh.ShardId, Epoch: sh.Epoch,
			StartKey: displayKey(sh.StartKey), EndKey: displayKey(sh.EndKey),
			StartHex: hex.EncodeToString(sh.StartKey), EndHex: hex.EncodeToString(sh.EndKey),
		}
		for _, r := range sh.Replicas {
			reg.Replicas = append(reg.Replicas, Replica{
				NodeID: r.NodeId, Address: r.Address, IsLeader: r.IsLeader, IsLearner: r.IsLearner,
			})
		}
		t.Regions = append(t.Regions, reg)
	}
	if len(t.Regions) == 0 {
		t.Regions = []Region{{ID: 0, StartKey: "−∞", EndKey: "+∞", Epoch: 1}}
	}
	return t
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
	var last error
	for _, addr := range metaAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = withAdminToken(ctx)
		conn, e := dial(addr)
		if e != nil {
			cancel()
			last = e
			continue
		}
		resp, e := metav1.NewMetaServiceClient(conn).GetSafePoint(ctx, &metav1.GetSafePointRequest{})
		conn.Close()
		cancel()
		if e != nil {
			last = e
			continue
		}
		return resp.SafePoint, resp.NextTs, resp.MinStartHint, nil
	}
	if last == nil {
		last = fmt.Errorf("no meta")
	}
	return 0, 0, 0, last
}

type NodeInfo struct {
	ID              string   `json:"id"`
	Address         string   `json:"address"`
	Role            string   `json:"role"`
	State           string   `json:"state"`
	StateCode       int      `json:"state_code"`
	Up              bool     `json:"up"`
	Regions         *int     `json:"regions,omitempty"`
	Leaders         *int     `json:"leaders,omitempty"`
	RegionIDs       []uint32 `json:"region_ids,omitempty"`
	LeaderRegionIDs []uint32 `json:"leader_region_ids,omitempty"`
}

// NodeStateName 对齐 Meta NodeState 枚举（行协议 0-based）。
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

// EnrichNodeRegions 按路由填充每个 Store 上的 Region / Leader Region 列表。
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
