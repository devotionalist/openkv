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

	"github.com/devotionalist/openkv-client-go/kv"
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

// 短暂选主 / 换副本可恢复的错误：同一 op 内重试，按「服务是否可用」计，不把一次 RPC 抖动算成不可用。
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if isConnDead(err) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "not_leader") ||
		strings.Contains(s, "retries_exhausted") ||
		strings.Contains(s, "DeadlineExceeded") ||
		strings.Contains(s, "deadline exceeded") ||
		strings.Contains(s, "apply_timeout") ||
		strings.Contains(s, "Unavailable") ||
		strings.Contains(s, "Aborted")
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

	// 成功率：有采样才算；0 次采样 = 0%（不可用），禁止把「全挂死未计数」当成 100%。
	pct := func(ok, fail uint64) float64 {
		t := ok + fail
		if t == 0 {
			return 0
		}
		return 100.0 * float64(ok) / float64(t)
	}

	ticker := time.NewTicker(*reportEvery)
	defer ticker.Stop()
	go func() {
		var lastPutOk, lastPutFail, lastGetOk, lastGetFail uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				po, pf := okPut.Load(), failPut.Load()
				go_, gf := okGet.Load(), failGet.Load()
				dPo, dPf := po-lastPutOk, pf-lastPutFail
				dGo, dGf := go_-lastGetOk, gf-lastGetFail
				lastPutOk, lastPutFail, lastGetOk, lastGetFail = po, pf, go_, gf
				dOk, dFail := dPo+dGo, dPf+dGf
				fmt.Printf("[chaos] window ok=%d fail=%d success=%.1f%% write_success=%.1f%% read_success=%.1f%%  put_ok=%d put_fail=%d get_ok=%d get_fail=%d workers=%d\n",
					dOk, dFail, pct(dOk, dFail), pct(dPo, dPf), pct(dGo, dGf),
					po, pf, go_, gf, *workers)
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
				const maxTry = 4
				var lastErr error
				ok := false
				for try := 0; try < maxTry; try++ {
					if cli == nil {
						c2, err := kv.Dial(opCtx, *addr, kv.WithTimeout(*opTimeout))
						if err != nil {
							lastErr = err
							time.Sleep(50 * time.Millisecond)
							continue
						}
						cli = c2
					}
					if doWrite {
						val := fmt.Sprintf("v-w%d-%d", workerID, i)
						_, lastErr = cli.Put(opCtx, []byte(key), []byte(val))
					} else if *staleRead {
						_, _, _, lastErr = cli.GetOpt(opCtx, []byte(key), false)
					} else {
						_, _, _, lastErr = cli.Get(opCtx, []byte(key))
					}
					if lastErr == nil {
						ok = true
						break
					}
					if isConnDead(lastErr) {
						cli.Close()
						cli = nil
					}
					if !isRetryable(lastErr) || opCtx.Err() != nil {
						break
					}
					time.Sleep(time.Duration(30*(try+1)) * time.Millisecond)
				}
				c()
				if ok {
					if doWrite {
						okPut.Add(1)
					} else {
						okGet.Add(1)
					}
				} else if ctx.Err() != nil && *duration == 0 {
					// 持续负载被 SIGTERM 打断：收尾噪声不算失败
				} else {
					// 含固定时长窗口到点仍未返回：算失败（否则多数派丢失时写全挂死会虚报 write_success=100%）
					if lastErr != nil {
						log.Printf("worker-%d op fail after retries: %v", workerID, lastErr)
					}
					if doWrite {
						failPut.Add(1)
					} else {
						failGet.Add(1)
					}
				}
				i++
				time.Sleep(interval)
			}
		}(w)
	}
	wg.Wait()

	po, pf := okPut.Load(), failPut.Load()
	go_, gf := okGet.Load(), failGet.Load()
	fmt.Printf("[chaos] DONE put_ok=%d put_fail=%d get_ok=%d get_fail=%d\n", po, pf, go_, gf)
	total := po + pf + go_ + gf
	if total == 0 {
		os.Exit(2)
	}
	// overall 兼容旧脚本；写/读分开才是故障可用性判定口径。
	fmt.Printf("[chaos] overall_success=%.2f%% write_success=%.2f%% read_success=%.2f%%\n",
		pct(po+go_, pf+gf), pct(po, pf), pct(go_, gf))
}
