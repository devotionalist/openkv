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

// line-rpc：IT/脚本用一行 RPC（gRPC ExecLine）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	metav1 "github.com/amydemo/opskv/client/go/gen/meta/v1"
	storagev1 "github.com/amydemo/opskv/client/go/gen/storage/v1"
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

func call(kind, addr, req string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if tok := os.Getenv("SKV_ADMIN_TOKEN"); tok != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-skv-admin-token", tok)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	switch kind {
	case "storage":
		resp, err := storagev1.NewAdminServiceClient(conn).ExecLine(ctx, &storagev1.ExecLineRequest{Line: req})
		if err != nil {
			return "", err
		}
		return resp.GetBody(), nil
	default:
		resp, err := metav1.NewMetaServiceClient(conn).ExecLine(ctx, &metav1.ExecLineRequest{Line: req})
		if err != nil {
			return "", err
		}
		return resp.GetBody(), nil
	}
}
