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
	"flag"
	"fmt"
	"log"
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
	addr := flag.String("addr", "127.0.0.1:19000", "compute gateway address")
	opTimeout := flag.Duration("op-timeout", 8*time.Second, "per-op RPC timeout")
	attempts := flag.Int("retries", 5, "retries on transient RPC errors")
	flag.Parse()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	cli, err := kv.Dial(dialCtx, *addr, kv.WithTimeout(*opTimeout))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	// 独立前缀，避免与上一轮残留 / 并发 chaos 抢同一 key。
	pfx := fmt.Sprintf("e2e-%d:", time.Now().UnixNano())
	kUser := []byte(pfx + "user:1")
	k1, k2, k3 := []byte(pfx+"k1"), []byte(pfx+"k2"), []byte(pfx+"k3")

	if err := retry(*opTimeout, *attempts, "put", func(ctx context.Context) error {
		_, e := cli.Put(ctx, kUser, []byte("alice"))
		return e
	}); err != nil {
		log.Fatal(err)
	}

	var val []byte
	var found bool
	var rev uint64
	if err := retry(*opTimeout, *attempts, "get", func(ctx context.Context) error {
		var e error
		val, found, rev, e = cli.Get(ctx, kUser)
		if e != nil {
			return e
		}
		if !found || string(val) != "alice" {
			return fmt.Errorf("mismatch found=%v val=%q rev=%d", found, val, rev)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("OK get user:1=%s rev=%d\n", val, rev)

	if err := retry(*opTimeout, *attempts, "batch put", func(ctx context.Context) error {
		return cli.BatchPut(ctx, map[string][]byte{
			string(k1): []byte("v1"),
			string(k2): []byte("v2"),
			string(k3): []byte("v3"),
		})
	}); err != nil {
		log.Fatal(err)
	}
	for _, k := range [][]byte{k1, k2, k3} {
		kk := k
		if err := retry(*opTimeout, *attempts, "batch get "+string(kk), func(ctx context.Context) error {
			v, f, _, e := cli.Get(ctx, kk)
			if e != nil {
				return e
			}
			if !f {
				return fmt.Errorf("not found")
			}
			fmt.Printf("OK batch %s=%s\n", kk, v)
			return nil
		}); err != nil {
			log.Fatal(err)
		}
	}

	if err := retry(*opTimeout, *attempts, "delete", func(ctx context.Context) error {
		deleted, _, e := cli.Delete(ctx, kUser)
		if e != nil {
			return e
		}
		if !deleted {
			return fmt.Errorf("deleted=false")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := retry(*opTimeout, *attempts, "delete verify", func(ctx context.Context) error {
		_, f, _, e := cli.Get(ctx, kUser)
		if e != nil {
			return e
		}
		if f {
			return fmt.Errorf("still found")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("OK delete user:1")

	if err := retry(*opTimeout, *attempts, "missing key", func(ctx context.Context) error {
		_, f, _, e := cli.Get(ctx, []byte(pfx+"no-such-key"))
		if e != nil {
			return e
		}
		if f {
			return fmt.Errorf("unexpectedly found")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("OK missing key")
	fmt.Println("e2e passed")
}
