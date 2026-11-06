// Copyright 2026 devotionalist
package lifecycle

import (
	"net"
	"strings"
	"testing"
)

func TestComponentsFromAddrsShortNames(t *testing.T) {
	comps := ComponentsFromAddrs(
		[]string{
			"meta-0.meta-headless.opskv.svc.cluster.local:8500",
			"meta-1.meta-headless.opskv.svc.cluster.local:8500",
		},
		[]string{
			"storage-0.storage-headless.opskv.svc.cluster.local:9100",
			"storage-1.storage-headless.opskv.svc.cluster.local:9100",
		},
		"gateway.opskv.svc.cluster.local:19000",
	)
	if len(comps) != 5 {
		t.Fatalf("len=%d want 5", len(comps))
	}
	want := []struct{ name, role string }{
		{"meta-0", "meta"},
		{"meta-1", "meta"},
		{"storage-0", "storage"},
		{"storage-1", "storage"},
		{"gateway", "gateway"},
	}
	for i, w := range want {
		if comps[i].Name != w.name || comps[i].Role != w.role {
			t.Fatalf("[%d] got name=%s role=%s want %s/%s", i, comps[i].Name, comps[i].Role, w.name, w.role)
		}
		if comps[i].Addr == "" || strings.HasPrefix(comps[i].Addr, "127.0.0.1") {
			t.Fatalf("[%d] unexpected addr %q", i, comps[i].Addr)
		}
	}
}

func TestListStatusRemoteTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	up := ListStatusRemote([]Component{{Name: "meta-0", Addr: addr, Role: "meta"}})
	if len(up) != 1 || !up[0].Up || !up[0].Port || up[0].Addr != addr {
		t.Fatalf("reachable: %+v", up)
	}

	down := ListStatusRemote([]Component{{Name: "meta-1", Addr: "127.0.0.1:1", Role: "meta"}})
	if len(down) != 1 || down[0].Up || down[0].Port {
		t.Fatalf("unreachable should be down: %+v", down)
	}
}
