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
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/devotionalist/openkv/client/go/internal/route"
	"github.com/devotionalist/openkv/client/go/internal/storagetcp"
	kvv1 "github.com/devotionalist/openkv/client/go/gen/kv/v1"
)

const (
	scanDefaultLimit = 256
	scanHardLimit    = 4096
)

// prefixUpperBound 返回 prefix 的字典序上界（半开 end）；全 0xff 无法进位 → nil（+inf）。
func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func minBytes(a, b []byte) []byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if bytes.Compare(a, b) <= 0 {
		return a
	}
	return b
}

func maxBytes(a, b []byte) []byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if bytes.Compare(a, b) >= 0 {
		return a
	}
	return b
}

func resolveScanRange(req *kvv1.ScanRequest) (start, end []byte) {
	start = req.StartKey
	end = req.EndKey
	if len(req.Prefix) > 0 {
		start = req.Prefix
		end = prefixUpperBound(req.Prefix)
	}
	return start, end
}

func (s *server) scanOneRegion(regionID uint32, start, end, after []byte, startTs uint64,
	limit uint32, keysOnly, linearizable bool) (*storagetcp.TxnScanResult, error) {
	if linearizable {
		var part *storagetcp.TxnScanResult
		err := s.withLeader(regionID, func(c *storagetcp.Client) error {
			r, e := c.TxnScan(regionID, start, end, after, startTs, limit, keysOnly, true)
			if e != nil {
				return e
			}
			part = r
			return nil
		})
		return part, err
	}
	var last error
	for _, st := range s.tryOrder(regionID) {
		r, e := st.TxnScanOpt(regionID, start, end, after, startTs, limit, keysOnly, false)
		if e != nil {
			last = e
			continue
		}
		return r, nil
	}
	if last == nil {
		last = fmt.Errorf("scan: no storage")
	}
	return nil, last
}

// Scan：线性一致时 AllocTs + 各 Region Leader TXN_SCAN；跨 Region 扇出，无旁路索引。
func (s *server) Scan(ctx context.Context, req *kvv1.ScanRequest) (*kvv1.ScanResponse, error) {
	_ = ctx
	limit := req.Limit
	if limit == 0 {
		limit = scanDefaultLimit
	}
	if limit > scanHardLimit {
		limit = scanHardLimit
	}
	start, end := resolveScanRange(req)
	after := append([]byte(nil), req.StartAfter...)
	linearizable := req.Linearizable

	var startTs uint64
	if linearizable {
		ts, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		startTs = ts
	} else {
		startTs = math.MaxUint64
	}

	out := &kvv1.ScanResponse{}
	// loc：用于 Locate 的键；跨 Region 后推进到 region.EndKey
	loc := start
	if len(after) > 0 {
		loc = after
	}

	for uint32(len(out.Pairs)) < limit {
		remain := limit - uint32(len(out.Pairs))
		regionID := s.regionOf(loc)
		snap := s.routes.Snapshot()
		if snap == nil {
			s.refreshRoutes()
			snap = s.routes.Snapshot()
			if snap == nil {
				return nil, fmt.Errorf("scan: no route table")
			}
		}
		reg, ok := snap.Region(regionID)
		if !ok {
			s.refreshRoutes()
			snap = s.routes.Snapshot()
			if snap == nil {
				return nil, fmt.Errorf("scan: no route table")
			}
			reg, ok = snap.Region(regionID)
			if !ok {
				return nil, fmt.Errorf("scan: no region")
			}
		}

		segStart := maxBytes(start, reg.StartKey)
		segEnd := minBytes(end, reg.EndKey)
		// 空交集或 loc 已越过本 Region：跳下一片
		if len(segEnd) > 0 && len(segStart) > 0 && bytes.Compare(segStart, segEnd) >= 0 {
			if len(reg.EndKey) == 0 {
				break
			}
			loc = reg.EndKey
			if len(end) > 0 && bytes.Compare(loc, end) >= 0 {
				break
			}
			continue
		}
		if len(after) > 0 && len(reg.EndKey) > 0 && bytes.Compare(after, reg.EndKey) >= 0 {
			loc = reg.EndKey
			if len(end) > 0 && bytes.Compare(loc, end) >= 0 {
				break
			}
			continue
		}

		part, err := s.scanOneRegion(regionID, segStart, segEnd, after, startTs, remain, req.KeysOnly, linearizable)
		if err != nil {
			if strings.Contains(err.Error(), "splitting") || strings.Contains(err.Error(), "no_region") {
				s.refreshRoutes()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return nil, err
		}
		for _, p := range part.Pairs {
			out.Pairs = append(out.Pairs, &kvv1.ScanPair{
				Key: p.Key, Value: p.Value, Revision: p.Revision,
			})
		}
		if part.Truncated {
			out.Truncated = true
			out.NextKey = part.NextKey
			if len(out.NextKey) == 0 && len(part.Pairs) > 0 {
				out.NextKey = part.Pairs[len(part.Pairs)-1].Key
			}
			return out, nil
		}
		if len(reg.EndKey) == 0 {
			break
		}
		loc = reg.EndKey
		if len(end) > 0 && bytes.Compare(loc, end) >= 0 {
			break
		}
		// after 仍保留：后续 Region 的 TXN_SCAN 会跳过 <= after 的键
	}
	return out, nil
}
