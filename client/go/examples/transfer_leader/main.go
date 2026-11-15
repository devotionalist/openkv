// Copyright 2026 devotionalist
//
// 运维/演练用：向当前 Region Leader 发起 TransferLeader，再可选 CAMPAIGN 目标。
// 用法：transfer_leader <leader_addr> <region_id> <target_peer_id>
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	storagev1 "github.com/devotionalist/openkv/client/go/gen/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s <leader_addr> <region_id> <target_peer>\n", os.Args[0])
		os.Exit(2)
	}
	addr := os.Args[1]
	rid, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad region_id: %v\n", err)
		os.Exit(2)
	}
	target := os.Args[3]

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	cli := storagev1.NewAdminServiceClient(conn)
	resp, err := cli.TransferLeader(ctx, &storagev1.TransferRequest{
		ShardId:    uint32(rid),
		TargetPeer: target,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "TransferLeader: %v\n", err)
		os.Exit(1)
	}
	if !resp.GetOk() {
		fmt.Fprintf(os.Stderr, "TransferLeader not ok: %s\n", resp.GetMessage())
		os.Exit(1)
	}
	fmt.Printf("OK TRANSFER %s -> %s msg=%s\n", addr, target, resp.GetMessage())
}
