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

func transient(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "DeadlineExceeded") ||
		strings.Contains(s, "Unavailable") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "transport is closing") ||
		strings.Contains(s, "not_leader")
}

func retry(opTimeout time.Duration, attempts int, name string, fn func(context.Context) error) error {
	var last error
	for i := 1; i <= attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		last = fn(ctx)
		cancel()
		if last == nil {
			return nil
		}
		if !transient(last) || i == attempts {
			return fmt.Errorf("%s: %w", name, last)
		}
		log.Printf("%s attempt %d/%d: %v — retry", name, i, attempts, last)
		time.Sleep(time.Duration(i) * 400 * time.Millisecond)
	}
	return last
}

func main() {
	addr := "127.0.0.1:19000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	const opTimeout = 8 * time.Second
	const attempts = 5

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	cli, err := kv.Dial(dialCtx, addr, kv.WithTimeout(opTimeout))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	pfx := fmt.Sprintf("txn-%d/", time.Now().UnixNano())
	ka, kb := []byte(pfx+"a"), []byte(pfx+"b")

	var txn *kv.Txn
	if err := retry(opTimeout, attempts, "begin", func(ctx context.Context) error {
		var e error
		txn, e = cli.Begin(ctx)
		return e
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("start_ts=%d\n", txn.StartTs())

	txn.Put(ka, []byte("1"))
	txn.Put(kb, []byte("2"))
	var cts uint64
	if err := retry(opTimeout, attempts, "commit", func(ctx context.Context) error {
		// 超时后重建事务，避免半提交锁卡住。
		t2, e := cli.Begin(ctx)
		if e != nil {
			return e
		}
		t2.Put(ka, []byte("1"))
		t2.Put(kb, []byte("2"))
		cts, e = t2.Commit(ctx)
		if e == nil {
			txn = t2
		}
		return e
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("committed commit_ts=%d\n", cts)

	var va, vb []byte
	var ok, okb bool
	if err := retry(opTimeout, attempts, "snap get", func(ctx context.Context) error {
		t2, e := cli.Begin(ctx)
		if e != nil {
			return e
		}
		va, ok, e = t2.Get(ctx, ka)
		if e != nil {
			return e
		}
		vb, okb, e = t2.Get(ctx, kb)
		return e
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("snap get a found=%v %s; b found=%v %s\n", ok, va, okb, vb)
}
