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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := kv.Dial(ctx, addr, kv.WithTimeout(2*time.Second))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	if _, err := cli.Put(ctx, []byte("hello"), []byte("opskv")); err != nil {
		log.Fatalf("put: %v", err)
	}
	val, found, rev, err := cli.Get(ctx, []byte("hello"))
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("get found=%v rev=%d value=%s\n", found, rev, string(val))
}
