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

// 持续用户读写负载：多 worker Put/Get，周期性输出成功率。
// 供 fault-drill / 集成测试在故障注入全程模拟真实流量。
//
// 用法：
//   kv-chaos -addr 127.0.0.1:19000 -duration 30s -qps 50
//   kv-chaos -addr … -duration 0 -workers 4 -qps 40   # 直到 SIGTERM（后台负载）
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"strings"

	"github.com/amydemo/opskv/client/go/kv"
)

func isConnDead(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "Broken pipe") ||
		strings.Contains(s, "transport is closing") ||
		strings.Contains(s, "Unavailable")
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "compute gateway")
	duration := flag.Duration("duration", 30*time.Second, "run duration; 0 = until SIGINT/SIGTERM")
	qps := flag.Int("qps", 50, "approx total ops/sec across workers (put+get each count)")
	workers := flag.Int("workers", 2, "concurrent client goroutines (simulated users)")
	writeRatio := flag.Float64("write-ratio", 0.5, "fraction of ops that are Put (rest Get)")
	keyPrefix := flag.String("prefix", "chaos", "key prefix")
	keySpace := flag.Int("keys", 512, "distinct key cardinality")
	reportEvery := flag.Duration("report", 2*time.Second, "stats interval")
	staleRead := flag.Bool("stale", false, "use follower/stale Get (linearizable=false)")
	opTimeout := flag.Duration("op-timeout", 3*time.Second, "per-op timeout (Put= TSO+prewrite+commit)")
	flag.Parse()

	if *workers < 1 {
		*workers = 1
	}
	if *keySpace < 1 {
		*keySpace = 1
	}
	if *writeRatio < 0 {
		*writeRatio = 0
	}
	if *writeRatio > 1 {
		*writeRatio = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	var okPut, failPut, okGet, failGet atomic.Uint64

	ticker := time.NewTicker(*reportEvery)
	defer ticker.Stop()
	go func() {
		var lastOk, lastFail uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				op := okPut.Load() + okGet.Load()
				fp := failPut.Load() + failGet.Load()
				dOk := op - lastOk
				dFail := fp - lastFail
				lastOk, lastFail = op, fp
				total := dOk + dFail
				rate := 100.0
				if total > 0 {
					rate = 100.0 * float64(dOk) / float64(total)
				}
				fmt.Printf("[chaos] window ok=%d fail=%d success=%.1f%%  total_ok=%d total_fail=%d workers=%d\n",
					dOk, dFail, rate, op, fp, *workers)
			}
		}
	}()

	perWorkerQPS := float64(*qps) / float64(*workers)
	if perWorkerQPS < 1 {
		perWorkerQPS = 1
	}
	interval := time.Duration(float64(time.Second) / perWorkerQPS)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	var deadline time.Time
	if *duration > 0 {
		deadline = time.Now().Add(*duration)
	}

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*997))
			cli, err := kv.Dial(ctx, *addr, kv.WithTimeout(*opTimeout))
			if err != nil {
				log.Printf("worker-%d dial: %v (will retry)", workerID, err)
			}
			defer func() {
				if cli != nil {
					cli.Close()
				}
			}()

			i := 0
			for {
				if ctx.Err() != nil {
					return
				}
				if *duration > 0 && time.Now().After(deadline) {
					return
				}

				if cli == nil {
					c2, err := kv.Dial(ctx, *addr, kv.WithTimeout(*opTimeout))
					if err != nil {
						failPut.Add(1)
						time.Sleep(200 * time.Millisecond)
						continue
					}
					cli = c2
				}

				key := fmt.Sprintf("%s:w%d:%d", *keyPrefix, workerID, rng.Intn(*keySpace))
				doWrite := rng.Float64() < *writeRatio

				opCtx, c := context.WithTimeout(ctx, *opTimeout)
				if doWrite {
					val := fmt.Sprintf("v-w%d-%d", workerID, i)
					if _, err := cli.Put(opCtx, []byte(key), []byte(val)); err != nil {
						failPut.Add(1)
						// 仅在明显断连时重拨，避免打爆本机 ephemeral port
						if isConnDead(err) {
							cli.Close()
							cli = nil
						}
					} else {
						okPut.Add(1)
					}
				} else {
					var err error
					if *staleRead {
						_, _, _, err = cli.GetOpt(opCtx, []byte(key), false)
					} else {
						_, _, _, err = cli.Get(opCtx, []byte(key))
					}
					if err != nil {
						failGet.Add(1)
						if isConnDead(err) {
							cli.Close()
							cli = nil
						}
					} else {
						okGet.Add(1)
					}
				}
				c()
				i++
				time.Sleep(interval)
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("[chaos] DONE put_ok=%d put_fail=%d get_ok=%d get_fail=%d\n",
		okPut.Load(), failPut.Load(), okGet.Load(), failGet.Load())
	total := okPut.Load() + failPut.Load() + okGet.Load() + failGet.Load()
	ok := okPut.Load() + okGet.Load()
	if total == 0 {
		os.Exit(2)
	}
	fmt.Printf("[chaos] overall_success=%.2f%%\n", 100.0*float64(ok)/float64(total))
}
