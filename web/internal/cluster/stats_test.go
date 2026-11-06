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

package cluster

import (
	"net"
	"net/http"
	"testing"
	"time"
)

func TestParseStatsLine(t *testing.T) {
	s, err := parseStatsLine("OK STATS requests=42 gw=10 raft=20 sync=5 admin=2 meta_out=3 client=10 peer_raft=storage-1:12,storage-2:8 peer_sync=storage-1:2 meta_out_peers=127.0.0.1:8500:4")
	if err != nil {
		t.Fatal(err)
	}
	if s.Requests != 42 || s.Gw != 10 || s.Raft != 20 || s.Sync != 5 || s.MetaOut != 3 {
		t.Fatalf("%+v", s)
	}
	if s.PeerRaft["storage-1"] != 12 || s.PeerSync["storage-1"] != 2 || s.MetaOutPeers["127.0.0.1:8500"] != 4 {
		t.Fatalf("peers %+v sync %+v meta %+v", s.PeerRaft, s.PeerSync, s.MetaOutPeers)
	}
}

func TestRateTracker(t *testing.T) {
	rt := NewRateTracker()
	if q := rt.Observe("a", 100); q != 0 {
		t.Fatalf("first sample qps=%v", q)
	}
	time.Sleep(500 * time.Millisecond)
	q := rt.Observe("a", 150)
	if q < 80 || q > 120 {
		t.Fatalf("qps=%v want ~100", q)
	}
}

func TestFetchGatewayStats(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"requests":99,"client":40,"gw":40,"to_store":50,"to_meta":9}`))
	})
	go http.Serve(ln, mux)

	s, err := FetchGatewayStats(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if s.Requests != 99 || s.ToStore != 50 || s.ToMeta != 9 {
		t.Fatalf("%+v", s)
	}
}
