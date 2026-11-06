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

// txn_gate：I20–I22 硬断言（提交可见 / 冲突 / Rollback）。失败 exit 1。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/amydemo/opskv/client/go/kv"
)

func main() {
	addr := "127.0.0.1:19000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cli, err := kv.Dial(ctx, addr, kv.WithTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	pfx := fmt.Sprintf("tg-%d/", time.Now().UnixNano())
	mustCommitVisible(ctx, cli, pfx)
	mustConflict(ctx, cli, pfx)
	mustRollback(ctx, cli, pfx)
	fmt.Println("ALL PASS: txn_gate")
}

func mustCommitVisible(ctx context.Context, cli *kv.Client, pfx string) {
	k := []byte(pfx + "c")
	txn, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("I20 begin: %v", err)
	}
	txn.Put(k, []byte("v1"))
	if _, err := txn.Commit(ctx); err != nil {
		log.Fatalf("I20 commit: %v", err)
	}
	got, found, _, err := cli.Get(ctx, k)
	if err != nil || !found || string(got) != "v1" {
		log.Fatalf("I20 visible: found=%v val=%q err=%v", found, got, err)
	}
	fmt.Println("OK I20 commit visible")
}

func mustConflict(ctx context.Context, cli *kv.Client, pfx string) {
	k := []byte(pfx + "x")
	t1, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("I21 begin1: %v", err)
	}
	t2, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("I21 begin2: %v", err)
	}
	t1.Put(k, []byte("a"))
	t2.Put(k, []byte("b"))
	if _, err := t1.Commit(ctx); err != nil {
		log.Fatalf("I21 first commit: %v", err)
	}
	_, err = t2.Commit(ctx)
	if err == nil {
		log.Fatal("I21 expected second commit to fail (conflict/locked)")
	}
	s := strings.ToLower(err.Error())
	if !(strings.Contains(s, "conflict") || strings.Contains(s, "locked") ||
		strings.Contains(s, "prewrite") || strings.Contains(s, "write")) {
		log.Fatalf("I21 unexpected error (want conflict-like): %v", err)
	}
	fmt.Printf("OK I21 conflict: %v\n", err)
}

func mustRollback(ctx context.Context, cli *kv.Client, pfx string) {
	k := []byte(pfx + "r")
	txn, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("I22 begin: %v", err)
	}
	txn.Put(k, []byte("tmp"))
	// Prewrite via Commit path would finalize; exercise Rollback on buffered txn:
	// put primary then Rollback before Commit — clears client intent; ensure key absent.
	if err := txn.Rollback(ctx); err != nil {
		log.Fatalf("I22 rollback: %v", err)
	}
	// After rollback without successful prewrite, key should not exist (or not hold our value).
	got, found, _, err := cli.Get(ctx, k)
	if err != nil {
		log.Fatalf("I22 get: %v", err)
	}
	if found && string(got) == "tmp" {
		log.Fatal("I22 rollback left value visible")
	}
	fmt.Println("OK I22 rollback")
}
