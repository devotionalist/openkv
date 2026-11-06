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

// 读副本示例：强一致 Get vs linearizable=false 的副本读。
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := kv.Dial(ctx, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	key := []byte("stale/demo")
	val := []byte("hello-replica")
	if _, err := cli.Put(ctx, key, val); err != nil {
		log.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	v1, ok1, _, err := cli.Get(ctx, key)
	if err != nil || !ok1 {
		log.Fatalf("linear get: %v found=%v", err, ok1)
	}
	v2, ok2, _, err := cli.GetOpt(ctx, key, false)
	if err != nil || !ok2 {
		log.Fatalf("stale get: %v found=%v", err, ok2)
	}
	fmt.Printf("linear=%q stale=%q\n", v1, v2)
	if string(v1) != string(val) || string(v2) != string(val) {
		log.Fatal("value mismatch")
	}
	fmt.Println("stale_read ok")
}
