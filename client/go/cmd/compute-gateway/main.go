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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/amydemo/opskv/client/go/internal/route"
	"github.com/amydemo/opskv/client/go/internal/storagetcp"
	"github.com/amydemo/opskv/client/go/internal/traffic"
	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
	"google.golang.org/grpc"
)

// Compute Gateway：Range 路由（Meta GET_ROUTE）+ Leader 缓存 / redirect。

type server struct {
	kvv1.UnimplementedKVServiceServer
	kvv1.UnimplementedDSServiceServer
	stores     []*storagetcp.Client
	byAddr     map[string]*storagetcp.Client
	metaAddrs  []string
	mu         sync.Mutex
	leaderHint map[uint32]string
	routes   route.Cache
	activeTs *activeTsTracker
}

func (s *server) regionOf(key []byte) uint32 {
	return s.routes.Locate(key)
}

func (s *server) client(addr string) *storagetcp.Client {
	if c, ok := s.byAddr[addr]; ok {
		return c
	}
	return nil
}

func (s *server) tryOrder(region uint32) []*storagetcp.Client {
	s.mu.Lock()
	hint := s.leaderHint[region]
	s.mu.Unlock()
	out := make([]*storagetcp.Client, 0, len(s.stores))
	seen := map[string]bool{}
	add := func(addr string) {
		if addr == "" || seen[addr] {
			return
		}
		if c := s.client(addr); c != nil {
			out = append(out, c)
			seen[addr] = true
		}
	}
	add(hint)
	for _, addr := range s.routes.ReplicaAddrs(region) {
		add(addr)
	}
	// Meta 路由与 Raft 成员可能短暂不一致（place/选举中），扫完全部 Store
	for _, c := range s.stores {
		add(c.Addr())
	}
	return out
}

func (s *server) rememberLeader(region uint32, addr string) {
	if addr == "" {
		return
	}
	s.mu.Lock()
	s.leaderHint[region] = addr
	s.mu.Unlock()
}

func (s *server) clearLeaderHint(region uint32) {
	s.mu.Lock()
	delete(s.leaderHint, region)
	s.mu.Unlock()
}

// Get / Put / Delete：默认线性一致走 MVCC；linearizable=false 读任意副本本地快照。

func (s *server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if !req.Linearizable {
		return s.getStale(ctx, req.Key)
	}
	for attempt := 0; attempt < 4; attempt++ {
		ts, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		tg, err := s.TxnGet(ctx, &kvv1.TxnGetRequest{Key: req.Key, StartTs: ts})
		if err != nil {
			if strings.Contains(err.Error(), "splitting") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return nil, err
		}
		if tg.Locked {
			s.refreshRoutes()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return &kvv1.GetResponse{Value: tg.Value, Found: tg.Found, Revision: ts}, nil
	}
	return nil, fmt.Errorf("get: splitting or locked")
}

// getStale：任意副本 TxnGet(MaxUint64, lin=0)；不跟 not_leader。
func (s *server) getStale(ctx context.Context, key []byte) (*kvv1.GetResponse, error) {
	_ = ctx
	region := s.regionOf(key)
	var last error
	for _, st := range s.tryOrder(region) {
		val, found, err := st.TxnGetOpt(region, key, ^uint64(0), false)
		if err != nil {
			last = err
			continue
		}
		return &kvv1.GetResponse{Value: val, Found: found, Revision: 0}, nil
	}
	if last == nil {
		last = fmt.Errorf("no storage")
	}
	return nil, last
}

func (s *server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	var lastErr string
	for attempt := 0; attempt < 4; attempt++ {
		start, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		// 短 Put 也登记 start_ts，防止 GC 水位越过进行中的 Prewrite
		s.activeTs.add(start)
		pw, err := s.TxnPrewrite(ctx, &kvv1.TxnPrewriteRequest{
			StartTs:    start,
			PrimaryKey: req.Key,
			Mutations:  []*kvv1.TxnMutation{{Key: req.Key, Value: req.Value}},
		})
		if err != nil {
			s.activeTs.remove(start)
			return nil, err
		}
		if !pw.Ok {
			s.activeTs.remove(start)
			lastErr = pw.Error
			if strings.Contains(pw.Error, "splitting") || strings.Contains(pw.Error, "epoch_not_match") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			// Meta 重启后 TSO 回退时，旧 commit 会触发 write_conflict；换新 ts 重试。
			if strings.Contains(pw.Error, "write_conflict") {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return &kvv1.PutResponse{Ok: false}, fmt.Errorf("put prewrite: %s", pw.Error)
		}
		cm, err := s.TxnCommit(ctx, &kvv1.TxnCommitRequest{
			StartTs:    start,
			PrimaryKey: req.Key,
			Keys:       [][]byte{req.Key},
		})
		// TxnCommit 成功会 remove；失败则此处清掉（或留给 TTL）
		if err != nil || cm == nil || !cm.Ok {
			s.activeTs.remove(start)
		}
		if err != nil {
			return nil, err
		}
		if !cm.Ok {
			lastErr = cm.Error
			if strings.Contains(cm.Error, "splitting") || strings.Contains(cm.Error, "epoch_not_match") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return &kvv1.PutResponse{Ok: false}, fmt.Errorf("put commit: %s", cm.Error)
		}
		return &kvv1.PutResponse{Ok: true, Revision: cm.CommitTs}, nil
	}
	if lastErr == "" {
		lastErr = "retries_exhausted"
	}
	return &kvv1.PutResponse{Ok: false}, fmt.Errorf("put: %s", lastErr)
}

func (s *server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	for attempt := 0; attempt < 4; attempt++ {
		start, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		s.activeTs.add(start)
		pw, err := s.TxnPrewrite(ctx, &kvv1.TxnPrewriteRequest{
			StartTs:    start,
			PrimaryKey: req.Key,
			Mutations:  []*kvv1.TxnMutation{{Key: req.Key, IsDelete: true}},
		})
		if err != nil {
			s.activeTs.remove(start)
			return nil, err
		}
		if !pw.Ok {
			s.activeTs.remove(start)
			if strings.Contains(pw.Error, "splitting") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("del prewrite: %s", pw.Error)
		}
		cm, err := s.TxnCommit(ctx, &kvv1.TxnCommitRequest{
			StartTs:    start,
			PrimaryKey: req.Key,
			Keys:       [][]byte{req.Key},
		})
		if err != nil || cm == nil || !cm.Ok {
			s.activeTs.remove(start)
		}
		if err != nil {
			return nil, err
		}
		if !cm.Ok {
			if strings.Contains(cm.Error, "splitting") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("del commit: %s", cm.Error)
		}
		return &kvv1.DeleteResponse{Deleted: true, Revision: cm.CommitTs}, nil
	}
	return nil, fmt.Errorf("delete: splitting")
}

func (s *server) BatchGet(ctx context.Context, req *kvv1.BatchGetRequest) (*kvv1.BatchGetResponse, error) {
	out := &kvv1.BatchGetResponse{}
	// proto3 bool 默认 false；Batch 固定强一致。副本读用 GetOpt(false)。
	for _, k := range req.Keys {
		r, err := s.Get(ctx, &kvv1.GetRequest{Key: k, Linearizable: true})
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, r)
	}
	return out, nil
}

func (s *server) BatchPut(ctx context.Context, req *kvv1.BatchPutRequest) (*kvv1.BatchPutResponse, error) {
	out := &kvv1.BatchPutResponse{}
	for _, it := range req.Items {
		r, err := s.Put(ctx, &kvv1.PutRequest{Key: it.Key, Value: it.Value})
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, r)
	}
	return out, nil
}

// storeSkipErr：无此 Region / 拨号失败 — 换节点继续，且不覆盖更具体的业务错误。
func storeSkipErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no_region") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "transport: Error while dialing") ||
		strings.Contains(msg, "Unavailable")
}

func (s *server) withLeader(region uint32, fn func(*storagetcp.Client) error) error {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			s.clearLeaderHint(region)
			s.refreshRoutes()
			time.Sleep(time.Duration(40*attempt) * time.Millisecond)
		}
		tried := map[string]bool{}
		sawNotLeader := false
		for _, st := range s.tryOrder(region) {
			addr := st.Addr()
			if tried[addr] {
				continue
			}
			tried[addr] = true
			err := fn(st)
			if err == nil {
				s.rememberLeader(region, addr)
				return nil
			}
			var nl *storagetcp.NotLeaderError
			if errors.As(err, &nl) {
				sawNotLeader = true
				last = err
				if nl.Redirect != "" {
					s.rememberLeader(region, nl.Redirect)
					if c := s.client(nl.Redirect); c != nil && !tried[c.Addr()] {
						tried[c.Addr()] = true
						if err2 := fn(c); err2 == nil {
							return nil
						} else if errors.As(err2, &nl) {
							sawNotLeader = true
							last = err2
						} else if storeSkipErr(err2) {
							if last == nil {
								last = err2
							}
						} else {
							return err2
						}
					}
				}
				continue
			}
			if storeSkipErr(err) {
				if last == nil {
					last = err
				}
				continue
			}
			// write_conflict / epoch_not_match / locked 等：已打到持有 Region 的节点
			return err
		}
		if !sawNotLeader {
			break
		}
	}
	if last == nil {
		last = fmt.Errorf("no storage")
	}
	return last
}

func waitStorage(addrs []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, a := range addrs {
		for {
			c := storagetcp.New(a)
			if err := c.Ping(); err == nil {
				log.Printf("storage ready: %s", a)
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("storage %s not ready", a)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return nil
}

func defaultStatsListen(grpcListen string) string {
	host, port, err := net.SplitHostPort(grpcListen)
	if err != nil {
		return "127.0.0.1:19001"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	var p int
	fmt.Sscanf(port, "%d", &p)
	if p <= 0 {
		p = 19000
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", p+1))
}

func main() {
	listen := flag.String("listen", "0.0.0.0:19000", "gRPC listen address")
	statsListen := flag.String("stats-listen", "", "HTTP stats listen (default: gRPC port+1)")
	storages := flag.String("storages", "127.0.0.1:9100", "comma-separated storage TCP addresses")
	metas := flag.String("metas", "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502", "meta TCP for GET_ROUTE")
	flag.Parse()
	if *statsListen == "" {
		*statsListen = defaultStatsListen(*listen)
	}

	addrs := strings.Split(*storages, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	metaAddrs := strings.Split(*metas, ",")
	for i := range metaAddrs {
		metaAddrs[i] = strings.TrimSpace(metaAddrs[i])
	}
	if err := waitStorage(addrs, 45*time.Second); err != nil {
		log.Fatal(err)
	}

	srvImpl := &server{
		byAddr:     map[string]*storagetcp.Client{},
		metaAddrs:  metaAddrs,
		leaderHint: map[uint32]string{},
		activeTs:   newActiveTsTracker(60 * time.Second),
	}
	// 默认全空间 Region 0，直到 Meta 路由拉取成功
	srvImpl.routes.Update(&route.Table{Version: 0, Regions: []route.Region{{ID: 0}}})
	for _, a := range addrs {
		c := storagetcp.New(a)
		srvImpl.stores = append(srvImpl.stores, c)
		srvImpl.byAddr[a] = c
	}

	go func() {
		var since uint64
		for {
			// A2：WATCH_ROUTE 长轮询，替代固定 2s 盲轮询
			t, err := route.WatchFetch(metaAddrs, since, 4000)
			if err != nil {
				if t2, e2 := route.Fetch(metaAddrs); e2 == nil {
					t, err = t2, nil
				}
			}
			if err == nil && t != nil {
				if t.Version != since {
					srvImpl.routes.Update(t)
					srvImpl.syncEpochsAsync(t)
					log.Printf("route refreshed ver=%d regions=%d", t.Version, len(t.Regions))
				}
				since = t.Version
			}
			if err := route.ReportMinStartTs(metaAddrs, srvImpl.activeTs.min()); err != nil {
				log.Printf("report min start_ts: %v", err)
			}
		}
	}()

	log.Printf("protocol: gRPC listen=%s", *listen)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		traffic.ClientIn.Add(1)
		return handler(ctx, req)
	}))
	kvv1.RegisterKVServiceServer(gs, srvImpl)
	kvv1.RegisterDSServiceServer(gs, srvImpl)

	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		client := traffic.ClientIn.Load()
		toStore := traffic.ToStore.Load()
		toMeta := traffic.ToMeta.Load()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"requests":        client + toStore + toMeta,
			"client":          client,
			"gw":              client,
			"to_store":        toStore,
			"to_meta":         toMeta,
			"to_store_peers":  traffic.SnapshotStorePeers(),
			"to_meta_peers":   traffic.SnapshotMetaPeers(),
		})
	})
	go func() {
		log.Printf("compute-gateway stats HTTP %s", *statsListen)
		if err := http.ListenAndServe(*statsListen, mux); err != nil {
			log.Printf("stats http: %v", err)
		}
	}()

	go func() {
		log.Printf("compute-gateway gRPC %s storages=%v range-routing=on", *listen, addrs)
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	gs.GracefulStop()
}
