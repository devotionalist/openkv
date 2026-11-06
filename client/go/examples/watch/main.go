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

// Watch 示例：订阅 key 前缀，另起协程写入并打印事件。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/amydemo/opskv/client/go/kv"
	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
)

func main() {
	addr := "127.0.0.1:19000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cli, err := kv.Dial(ctx, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	prefix := []byte("watch/")
	evCh, errCh, err := cli.Watch(ctx, nil, prefix, 0)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 3; i++ {
			k := []byte(fmt.Sprintf("watch/k%d", i))
			if _, err := cli.Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
				log.Printf("put: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	got := 0
	for got < 3 {
		select {
		case <-ctx.Done():
			log.Fatal("timeout waiting watch events")
		case err := <-errCh:
			if err != nil {
				log.Fatal(err)
			}
			return
		case ev := <-evCh:
			if ev == nil {
				continue
			}
			kind := "PUT"
			if ev.Type == kvv1.WatchEvent_DELETE {
				kind = "DELETE"
			}
			fmt.Printf("event %s key=%q value=%q rev=%d\n", kind, ev.Key, ev.Value, ev.Revision)
			got++
		}
	}
	fmt.Println("watch ok")
}
