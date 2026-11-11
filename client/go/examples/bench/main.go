// Copyright 2026 devotionalist
//
// Gateway 全接口压测：打满吞吐，输出各 API 的 ops/s 与延迟分位。
//
//	go run ./examples/bench -addr 127.0.0.1:19000 -mode all -workers 16 -duration 10s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devotionalist/openkv-client-go/kv"
	kvv1 "github.com/devotionalist/openkv-client-go/gen/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type result struct {
	Name  string
	OK    uint64
	Fail  uint64
	Ops   float64
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	NLat  int
	Notes string
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "gateway")
	mode := flag.String("mode", "all", "all|get|put|… (see -list)")
	list := flag.Bool("list", false, "print modes and exit")
	workers := flag.Int("workers", 16, "goroutines per API")
	duration := flag.Duration("duration", 10*time.Second, "steady-state per API")
	warmup := flag.Duration("warmup", 1*time.Second, "warmup per API")
	keys := flag.Int("keys", 200, "key space")
	prefix := flag.String("prefix", fmt.Sprintf("bench-%d", time.Now().UnixNano()%1e9), "key prefix")
	valueSize := flag.Int("vsize", 64, "value bytes")
	opTimeout := flag.Duration("op-timeout", 8*time.Second, "per-op timeout")
	flag.Parse()

	modes := allModes()
	if *list {
		fmt.Println(strings.Join(modes, "\n"))
		return
	}
	runModes := modes
	if *mode != "all" {
		runModes = strings.Split(*mode, ",")
	}

	ctx := context.Background()
	cli, err := kv.Dial(ctx, *addr, kv.WithTimeout(0))
	if err != nil {
		fatal(err)
	}
	defer cli.Close()

	conn, err := grpc.DialContext(ctx, *addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	rawKV := kvv1.NewKVServiceClient(conn)
	rawDS := kvv1.NewDSServiceClient(conn)

	val := make([]byte, *valueSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	fmt.Printf("seed %d keys under %s/ …\n", *keys, *prefix)
	seeded := seed(ctx, cli, *prefix, *keys, val, *opTimeout)
	if seeded < *keys {
		fmt.Printf("  seeded %d/%d (partial; continuing)\n", seeded, *keys)
		*keys = seeded
		if *keys < 1 {
			fatal(fmt.Errorf("seed produced 0 keys"))
		}
	}

	var results []result
	for _, m := range runModes {
		m = strings.TrimSpace(m)
		if m == "" || m == "all" {
			continue
		}
		fmt.Printf("\n>>> %s workers=%d duration=%s\n", m, *workers, *duration)
		r := runOne(ctx, cli, rawKV, rawDS, m, *prefix, *keys, val, *workers, *duration, *warmup, *opTimeout)
		results = append(results, r)
		fmt.Printf("    %.1f ops/s  p50=%s p95=%s p99=%s  ok=%d fail=%d %s\n",
			r.Ops, r.P50.Round(time.Microsecond), r.P95.Round(time.Microsecond),
			r.P99.Round(time.Microsecond), r.OK, r.Fail, r.Notes)
	}

	fmt.Println("\n================ SUMMARY ================")
	fmt.Printf("%-14s %10s %8s %8s %8s %8s %6s\n", "API", "ops/s", "p50", "p95", "p99", "ok", "fail")
	for _, r := range results {
		fmt.Printf("%-14s %10.1f %8s %8s %8s %8d %6d\n",
			r.Name, r.Ops,
			fmtDur(r.P50), fmtDur(r.P95), fmtDur(r.P99),
			r.OK, r.Fail)
	}
}

func allModes() []string {
	return []string{
		"get", "get_stale", "put", "put_ttl", "put_cas",
		"delete", "delete_cas",
		"batch_put", "batch_get",
		"scan", "list_prefix",
		"ttl", "expire", "persist",
		"txn", "txn_rollback",
		"watch",
		"hset", "hget",
		"sadd", "sismember",
		"lpush", "llen",
		"zadd", "zscore",
	}
}

func seed(ctx context.Context, cli *kv.Client, prefix string, n int, val []byte, to time.Duration) int {
	// Sequential seed: Put = TSO+Prewrite+Commit. Do not retry the same key after a
	// client/server timeout — a live prewrite lock yields ERR locked.
	ok := 0
	for i := 0; i < n; i++ {
		opCtx, cancel := context.WithTimeout(ctx, to)
		_, err := cli.Put(opCtx, keyOf(prefix, i), val)
		cancel()
		if err != nil {
			fmt.Printf("  seed stop at %d: %v\n", i, err)
			return ok
		}
		ok++
		if (i+1)%25 == 0 {
			fmt.Printf("  seed %d/%d\n", i+1, n)
		}
	}
	return ok
}

type opFn func(ctx context.Context, i int) error

func runOne(
	ctx context.Context,
	cli *kv.Client,
	rawKV kvv1.KVServiceClient,
	rawDS kvv1.DSServiceClient,
	mode, prefix string,
	keys int,
	val []byte,
	workers int,
	duration, warmup, opTimeout time.Duration,
) result {
	fn, notes := makeOp(cli, rawKV, rawDS, mode, prefix, keys, val)
	var ok, fail atomic.Uint64
	latCh := make(chan time.Duration, 1<<15)
	var samples []time.Duration
	var latMu sync.Mutex
	doneCollect := make(chan struct{})
	go func() {
		defer close(doneCollect)
		for d := range latCh {
			latMu.Lock()
			samples = append(samples, d)
			latMu.Unlock()
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var recording atomic.Bool

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := id
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				opCtx, opCancel := context.WithTimeout(ctx, opTimeout)
				t0 := time.Now()
				err := fn(opCtx, i)
				opCancel()
				d := time.Since(t0)
				i++
				if !recording.Load() {
					continue
				}
				if err != nil {
					fail.Add(1)
					continue
				}
				ok.Add(1)
				select {
				case latCh <- d:
				default:
				}
			}
		}(w)
	}

	time.Sleep(warmup)
	recording.Store(true)
	t0 := time.Now()
	time.Sleep(duration)
	elapsed := time.Since(t0)
	recording.Store(false)
	cancel()
	wg.Wait()
	close(latCh)
	<-doneCollect

	latMu.Lock()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50, p95, p99 := percentile(samples, 0.50), percentile(samples, 0.95), percentile(samples, 0.99)
	n := len(samples)
	latMu.Unlock()

	nOK := ok.Load()
	return result{
		Name: mode, OK: nOK, Fail: fail.Load(),
		Ops: float64(nOK) / elapsed.Seconds(),
		P50: p50, P95: p95, P99: p99, NLat: n, Notes: notes,
	}
}

func makeOp(
	cli *kv.Client,
	rawKV kvv1.KVServiceClient,
	rawDS kvv1.DSServiceClient,
	mode, prefix string,
	keys int,
	val []byte,
) (opFn, string) {
	delKey := func(i int) []byte { return keyOf(prefix+"-del", i%keys) }

	switch mode {
	case "get":
		return func(ctx context.Context, i int) error {
			_, _, _, err := cli.Get(ctx, keyOf(prefix, i%keys))
			return err
		}, ""
	case "get_stale":
		return func(ctx context.Context, i int) error {
			_, _, _, err := cli.GetOpt(ctx, keyOf(prefix, i%keys), false)
			return err
		}, "linearizable=false"
	case "put":
		return func(ctx context.Context, i int) error {
			_, err := cli.Put(ctx, keyOf(prefix, i%keys), val)
			return err
		}, ""
	case "put_ttl":
		return func(ctx context.Context, i int) error {
			_, err := cli.PutTTL(ctx, keyOf(prefix+"-pttl", i%keys), val, 60_000)
			return err
		}, "ttl=60s"
	case "put_cas":
		// create-only then overwrite with expected rev — use Put then PutCAS cycle per worker key
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-casw", i%keys)
			_, _, rev, err := cli.Get(ctx, k)
			if err != nil {
				return err
			}
			if rev == 0 {
				_, err = cli.PutCAS(ctx, k, val, 0)
				return err
			}
			_, err = cli.PutCAS(ctx, k, val, rev)
			return err
		}, "get+cas"
	case "delete":
		return func(ctx context.Context, i int) error {
			k := delKey(i)
			_, _ = cli.Put(ctx, k, val) // ensure exists (counted outside? include in op)
			_, _, err := cli.Delete(ctx, k)
			return err
		}, "put+delete"
	case "delete_cas":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-dcas", i%keys)
			rev, err := cli.Put(ctx, k, val)
			if err != nil {
				return err
			}
			_, _, err = cli.DeleteCAS(ctx, k, rev)
			return err
		}, "put+deleteCAS"
	case "batch_put":
		return func(ctx context.Context, i int) error {
			m := map[string][]byte{
				string(keyOf(prefix+"-bp", i%keys)):         val,
				string(keyOf(prefix+"-bp", (i+1)%keys)):     val,
				string(keyOf(prefix+"-bp", (i+2)%keys)):     val,
				string(keyOf(prefix+"-bp", (i+3)%keys)):     val,
			}
			return cli.BatchPut(ctx, m)
		}, "batch=4"
	case "batch_get":
		return func(ctx context.Context, i int) error {
			req := &kvv1.BatchGetRequest{Linearizable: true}
			for j := 0; j < 4; j++ {
				req.Keys = append(req.Keys, keyOf(prefix, (i+j)%keys))
			}
			_, err := rawKV.BatchGet(ctx, req)
			return err
		}, "batch=4"
	case "scan":
		return func(ctx context.Context, i int) error {
			_, err := cli.Scan(ctx, &kvv1.ScanRequest{
				StartKey: keyOf(prefix, 0),
				EndKey:   keyOf(prefix, keys),
				Limit:    32,
			})
			return err
		}, "limit=32"
	case "list_prefix":
		return func(ctx context.Context, i int) error {
			_, err := cli.ListPrefix(ctx, []byte(prefix+"/"), 32)
			return err
		}, "limit=32"
	case "ttl":
		return func(ctx context.Context, i int) error {
			_, err := cli.TTL(ctx, keyOf(prefix, i%keys))
			return err
		}, ""
	case "expire":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-exp", i%keys)
			_, _ = cli.Put(ctx, k, val)
			_, _, err := cli.Expire(ctx, k, 30_000)
			return err
		}, "put+expire"
	case "persist":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-pers", i%keys)
			_, _ = cli.PutTTL(ctx, k, val, 60_000)
			_, _, err := cli.Persist(ctx, k)
			return err
		}, "putTTL+persist"
	case "txn":
		return func(ctx context.Context, i int) error {
			t, err := cli.Begin(ctx)
			if err != nil {
				return err
			}
			k := keyOf(prefix+"-txn", i%keys)
			if _, _, err := t.Get(ctx, k); err != nil {
				_ = t.Rollback(ctx)
				return err
			}
			t.Put(k, val)
			_, err = t.Commit(ctx)
			return err
		}, "begin+get+put+commit"
	case "txn_rollback":
		return func(ctx context.Context, i int) error {
			t, err := cli.Begin(ctx)
			if err != nil {
				return err
			}
			k := keyOf(prefix+"-txnr", i%keys)
			t.Put(k, val)
			return t.Rollback(ctx)
		}, "begin+put+rollback"
	case "watch":
		return func(ctx context.Context, i int) error {
			wctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			ch, errCh, err := cli.Watch(wctx, keyOf(prefix, i%keys), nil, 0)
			if err != nil {
				return err
			}
			select {
			case <-ch:
			case <-errCh:
			case <-wctx.Done():
			}
			return nil
		}, "open+recv≤500ms"
	case "hset":
		return func(ctx context.Context, i int) error {
			_, err := rawDS.HSet(ctx, &kvv1.HSetRequest{
				Key: keyOf(prefix+"-h", i%keys), Field: []byte("f"), Value: val,
			})
			return err
		}, ""
	case "hget":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-h", i%keys)
			_, _ = rawDS.HSet(ctx, &kvv1.HSetRequest{Key: k, Field: []byte("f"), Value: val})
			_, err := rawDS.HGet(ctx, &kvv1.HGetRequest{Key: k, Field: []byte("f")})
			return err
		}, "hset+hget"
	case "sadd":
		return func(ctx context.Context, i int) error {
			_, err := rawDS.SAdd(ctx, &kvv1.SAddRequest{
				Key: keyOf(prefix+"-s", i%keys), Members: [][]byte{[]byte(fmt.Sprintf("m%d", i%16))},
			})
			return err
		}, ""
	case "sismember":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-s", i%keys)
			_, _ = rawDS.SAdd(ctx, &kvv1.SAddRequest{Key: k, Members: [][]byte{[]byte("m0")}})
			_, err := rawDS.SIsMember(ctx, &kvv1.SIsMemberRequest{Key: k, Member: []byte("m0")})
			return err
		}, "sadd+sismember"
	case "lpush":
		return func(ctx context.Context, i int) error {
			_, err := rawDS.LPush(ctx, &kvv1.LPushRequest{
				Key: keyOf(prefix+"-l", i%keys), Values: [][]byte{val},
			})
			return err
		}, ""
	case "llen":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-l", i%keys)
			_, _ = rawDS.LPush(ctx, &kvv1.LPushRequest{Key: k, Values: [][]byte{val}})
			_, err := rawDS.LLen(ctx, &kvv1.LLenRequest{Key: k})
			return err
		}, "lpush+llen"
	case "zadd":
		return func(ctx context.Context, i int) error {
			_, err := rawDS.ZAdd(ctx, &kvv1.ZAddRequest{
				Key: keyOf(prefix+"-z", i%keys),
				Items: []*kvv1.ZMemberScore{{Member: []byte(fmt.Sprintf("m%d", i%16)), Score: float64(i % 100)}},
			})
			return err
		}, ""
	case "zscore":
		return func(ctx context.Context, i int) error {
			k := keyOf(prefix+"-z", i%keys)
			_, _ = rawDS.ZAdd(ctx, &kvv1.ZAddRequest{
				Key: k, Items: []*kvv1.ZMemberScore{{Member: []byte("m0"), Score: 1}},
			})
			_, err := rawDS.ZScore(ctx, &kvv1.ZScoreRequest{Key: k, Member: []byte("m0")})
			return err
		}, "zadd+zscore"
	default:
		fatal(fmt.Errorf("unknown mode %q (use -list)", mode))
		return nil, ""
	}
}

func keyOf(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s/%08d", prefix, i))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "bench: %v\n", err)
	os.Exit(1)
}
