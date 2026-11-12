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

// line-rpc：IT/脚本用一行命令 → 类型化 Meta/Admin gRPC（输出仍兼容 OK … 行）。
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
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

func main() {
	kind := flag.String("kind", "meta", "meta|storage")
	addrs := flag.String("addrs", "127.0.0.1:8500", "comma-separated addresses")
	req := flag.String("req", "PING", "line request without trailing newline")
	timeout := flag.Duration("timeout", 3*time.Second, "per-hop timeout")
	flag.Parse()

	var last error
	for _, addr := range strings.Split(*addrs, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		cur := addr
		for hop := 0; hop < 6; hop++ {
			body, err := call(*kind, cur, *req, *timeout)
			if err != nil {
				last = err
				break
			}
			if strings.Contains(body, "not_leader") {
				if i := strings.Index(body, "redirect="); i >= 0 {
					cur = strings.TrimSpace(body[i+9:])
					if j := strings.IndexAny(cur, " \t"); j >= 0 {
						cur = cur[:j]
					}
					continue
				}
			}
			fmt.Println(body)
			return
		}
	}
	if last != nil {
		fmt.Fprintf(os.Stderr, "ERR %v\n", last)
	} else {
		fmt.Fprintln(os.Stderr, "ERR no peer")
	}
	os.Exit(1)
}

func withToken(ctx context.Context) context.Context {
	if tok := os.Getenv("SKV_ADMIN_TOKEN"); tok != "" {
		return metadata.AppendToOutgoingContext(ctx, "x-skv-admin-token", tok)
	}
	return ctx
}

func call(kind, addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = withToken(ctx)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	parts := strings.Fields(req)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty")
	}
	if kind == "storage" {
		return callStorage(ctx, storagev1.NewAdminServiceClient(conn), parts)
	}
	return callMeta(ctx, metav1.NewMetaServiceClient(conn), parts)
}

func callStorage(ctx context.Context, cli storagev1.AdminServiceClient, parts []string) (string, error) {
	switch parts[0] {
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
		return fmt.Sprintf("OK STATS requests=%d gw=%d raft=%d sync=%d admin=%d meta_out=%d",
			resp.Requests, resp.Gw, resp.Raft, resp.Sync, resp.Admin, resp.MetaOut), nil
	case "ADMIN_CAMPAIGN":
		rid, _ := strconv.ParseUint(parts[1], 10, 32)
		resp, err := cli.Campaign(ctx, &storagev1.CampaignRequest{RegionId: uint32(rid)})
		if err != nil {
			return "", err
		}
		return msgOrOK(resp.Ok, resp.Message, resp.RedirectTo), nil
	default:
		return "", fmt.Errorf("unsupported storage op %s (use typed Admin RPC)", parts[0])
	}
}

func callMeta(ctx context.Context, cli metav1.MetaServiceClient, parts []string) (string, error) {
	switch parts[0] {
	case "PING":
		resp, err := cli.Ping(ctx, &metav1.PingRequest{})
		if err != nil {
			return "", err
		}
		if resp.Message != "" {
			return resp.Message, nil
		}
		return "OK PONG", nil
	case "GET_ROUTE":
		resp, err := cli.GetRoutingTable(ctx, &metav1.GetRoutingTableRequest{})
		if err != nil {
			return "", err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "OK ROUTE %d %d 0", resp.Version, len(resp.Shards))
		for _, sh := range resp.Shards {
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
		return b.String(), nil
	case "WHO_LEADER":
		resp, err := cli.WhoLeader(ctx, &metav1.WhoLeaderRequest{})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("OK LEADER %s %s", resp.LeaderId, resp.Address), nil
	case "GET_TS":
		r := &metav1.GetTsRequest{}
		if len(parts) >= 2 {
			n, _ := strconv.ParseUint(parts[1], 10, 32)
			r.Count = uint32(n)
		}
		resp, err := cli.GetTs(ctx, r)
		if err != nil {
			return "", err
		}
		got := resp.Count
		if got == 0 {
			got = 1
		}
		return fmt.Sprintf("OK TS %d %d", resp.Ts, got), nil
	case "STATS":
		resp, err := cli.Stats(ctx, &metav1.StatsRequest{})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("OK STATS requests=%d gw=%d store=%d raft=%d sync=%d",
			resp.Requests, resp.Gw, resp.Store, resp.Raft, resp.Sync), nil
	case "LIST_NODES":
		r := &metav1.ListNodesRequest{}
		if len(parts) >= 2 && parts[1] == "storage" {
			r.Role = metav1.NodeRole_STORAGE
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
			if state > 0 {
				state--
			}
			fmt.Fprintf(&b, " %s@%s:%s:%d", n.NodeId, n.Address, role, state)
		}
		return b.String(), nil
	default:
		return "", fmt.Errorf("unsupported meta op %s", parts[0])
	}
}

func msgOrOK(ok bool, msg, redir string) string {
	if msg != "" {
		return msg
	}
	if redir != "" {
		return "ERR not_leader redirect=" + redir
	}
	if ok {
		return "OK"
	}
	return "ERR"
}
