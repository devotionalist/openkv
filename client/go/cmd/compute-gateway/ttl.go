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
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/devotionalist/openkv/client/go/internal/route"
	"github.com/devotionalist/openkv/client/go/internal/storagetcp"
	kvv1 "github.com/devotionalist/openkv/client/go/gen/kv/v1"
)

func (s *server) TTL(ctx context.Context, req *kvv1.TTLRequest) (*kvv1.TTLResponse, error) {
	if !req.Linearizable {
		return s.ttlStale(ctx, req.Key)
	}
	for attempt := 0; attempt < 4; attempt++ {
		ts, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		region := s.regionOf(req.Key)
		var ttl int64
		err = s.withLeader(region, func(c *storagetcp.Client) error {
			v, e := c.TxnTTL(region, req.Key, ts)
			if e != nil {
				return e
			}
			ttl = v
			return nil
		})
		if err == nil {
			return &kvv1.TTLResponse{TtlMs: ttl}, nil
		}
		var le *storagetcp.LockedError
		if errors.As(err, &le) {
			if re := s.resolveBlockingLock(ctx, req.Key, le); re != nil {
				if strings.Contains(re.Error(), "locked") {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				return nil, re
			}
			continue
		}
		if strings.Contains(err.Error(), "splitting") {
			s.refreshRoutes()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("ttl: locked or splitting")
}

func (s *server) ttlStale(ctx context.Context, key []byte) (*kvv1.TTLResponse, error) {
	_ = ctx
	region := s.regionOf(key)
	var last error
	for _, st := range s.tryOrder(region) {
		ttl, err := st.TxnTTLOpt(region, key, math.MaxUint64, false)
		if err != nil {
			last = err
			continue
		}
		return &kvv1.TTLResponse{TtlMs: ttl}, nil
	}
	if last == nil {
		last = fmt.Errorf("no storage")
	}
	return nil, last
}

// Expire：读出 value 后 Put 回写并 stamp 新 TTL；ttl_ms<=0 永久化。
func (s *server) Expire(ctx context.Context, req *kvv1.ExpireRequest) (*kvv1.ExpireResponse, error) {
	val, found, _, err := s.getForRewrite(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.ExpireResponse{Ok: false, Found: false}, nil
	}
	ttl := req.TtlMs
	if ttl < 0 {
		ttl = 0
	}
	pr, err := s.Put(ctx, &kvv1.PutRequest{Key: req.Key, Value: val, TtlMs: ttl})
	if err != nil {
		return nil, err
	}
	if !pr.Ok {
		return &kvv1.ExpireResponse{Ok: false, Found: true}, nil
	}
	return &kvv1.ExpireResponse{Ok: true, Found: true, Revision: pr.Revision}, nil
}

func (s *server) Persist(ctx context.Context, req *kvv1.PersistRequest) (*kvv1.PersistResponse, error) {
	ex, err := s.Expire(ctx, &kvv1.ExpireRequest{Key: req.Key, TtlMs: 0})
	if err != nil {
		return nil, err
	}
	return &kvv1.PersistResponse{Ok: ex.Ok, Found: ex.Found, Revision: ex.Revision}, nil
}

func (s *server) getForRewrite(ctx context.Context, key []byte) (value []byte, found bool, rev uint64, err error) {
	gr, err := s.Get(ctx, &kvv1.GetRequest{Key: key, Linearizable: true})
	if err != nil {
		return nil, false, 0, err
	}
	return gr.Value, gr.Found, gr.Revision, nil
}
