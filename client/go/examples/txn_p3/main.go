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
	"time"

	"github.com/amydemo/opskv/client/go/kv"
)

func main() {
	addr := "127.0.0.1:19000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cli, err := kv.Dial(ctx, addr, kv.WithTimeout(8*time.Second))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	// 悲观事务
	txn, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("begin: %v", err)
	}
	if err := txn.LockKeys(ctx, []byte("p3/x"), []byte("p3/y")); err != nil {
		log.Fatalf("lock: %v", err)
	}
	txn.Put([]byte("p3/x"), []byte("px"))
	txn.Put([]byte("p3/y"), []byte("py"))
	cts, err := txn.Commit(ctx)
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
	fmt.Printf("pessimistic committed ts=%d\n", cts)

	txn2, err := cli.Begin(ctx)
	if err != nil {
		log.Fatalf("begin2: %v", err)
	}
	vx, _, err := txn2.Get(ctx, []byte("p3/x"))
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("snap p3/x=%s\n", vx)
}
