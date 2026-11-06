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
	"fmt"
	"time"

	"github.com/amydemo/opskv/client/go/internal/route"
	"github.com/amydemo/opskv/client/go/internal/storagetcp"
)

func main() {
	metas := []string{"127.0.0.1:8500", "127.0.0.1:8501", "127.0.0.1:8502"}
	t0 := time.Now()
	tab, err := route.Fetch(metas)
	fmt.Printf("fetch route err=%v dur=%s ver=%v regions=%d\n", err, time.Since(t0), tab != nil && true, len(tab.Regions))
	if tab != nil {
		for _, r := range tab.Regions {
			fmt.Printf("  region %d epoch=%d end=%q leaders=", r.ID, r.Epoch, string(r.EndKey))
			for _, p := range r.Replicas {
				if p.IsLeader {
					fmt.Printf("%s@%s ", p.NodeID, p.Address)
				}
			}
			fmt.Println()
		}
	}
	t1 := time.Now()
	ts, err := route.AllocTs(metas, 1)
	fmt.Printf("allocTs=%d err=%v dur=%s\n", ts, err, time.Since(t1))

	key := []byte("simkey")
	region, _ := tab.Locate(key)
	reg, _ := tab.Region(region)
	epoch := reg.Epoch
	var leader string
	for _, p := range reg.Replicas {
		if p.IsLeader {
			leader = p.Address
		}
	}
	fmt.Printf("key region=%d epoch=%d leader=%s\n", region, epoch, leader)

	c := storagetcp.New(leader)
	t2 := time.Now()
	_, err = c.Prewrite(region, key, ts, key, 0, []byte("simval"), epoch)
	fmt.Printf("prewrite err=%v dur=%s\n", err, time.Since(t2))

	t3 := time.Now()
	cts, err := route.AllocTs(metas, 1)
	fmt.Printf("commitTs=%d err=%v dur=%s\n", cts, err, time.Since(t3))
	t4 := time.Now()
	_, err = c.CommitKey(region, key, ts, cts, epoch)
	fmt.Printf("commit err=%v dur=%s\n", err, time.Since(t4))

	// syncEpochs style — time one CallRaw set epoch on all stores
	stores := []string{"127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103", "127.0.0.1:9104", "127.0.0.1:9105"}
	t5 := time.Now()
	for _, a := range stores {
		st := storagetcp.New(a)
		tt := time.Now()
		resp, e := st.CallRaw(fmt.Sprintf("ADMIN_SET_EPOCH %d %d", region, epoch))
		fmt.Printf("  set_epoch %s dur=%s resp=%q err=%v\n", a, time.Since(tt), resp, e)
	}
	fmt.Printf("syncEpochs-all dur=%s\n", time.Since(t5))
}
