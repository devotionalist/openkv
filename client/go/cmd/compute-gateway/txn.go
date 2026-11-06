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
	"log"
	"strings"

	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
	"github.com/amydemo/opskv/client/go/internal/route"
	"github.com/amydemo/opskv/client/go/internal/storagetcp"
)

func (s *server) epochOf(region uint32) uint64 {
	return s.routes.Epoch(region)
}

func (s *server) refreshRoutes() {
	if t, err := route.Fetch(s.metaAddrs); err == nil {
		s.routes.Update(t)
		s.syncEpochsAsync(t)
	}
}

// syncEpochsAsync：向副本刷 Meta 路由 epoch。仅当 Meta epoch 更高时推送，避免把
// Storage 已 bump 的 epoch 降回去（SPLIT 后 Meta right 曾误写 1 时会搞坏 txn）。
func (s *server) syncEpochsAsync(t *route.Table) {
	if t == nil {
		return
	}
	type job struct {
		addr string
		line string
	}
	var jobs []job
	for _, reg := range t.Regions {
		if reg.Epoch == 0 {
			continue
		}
		line := fmt.Sprintf("ADMIN_SET_EPOCH %d %d", reg.ID, reg.Epoch)
		seen := map[string]bool{}
		for _, rep := range reg.Replicas {
			if rep.Address == "" || seen[rep.Address] {
				continue
			}
			seen[rep.Address] = true
			jobs = append(jobs, job{addr: rep.Address, line: line})
		}
	}
	if len(jobs) == 0 {
		return
	}
	go func() {
		for _, j := range jobs {
			if c := s.client(j.addr); c != nil {
				_, _ = c.CallRaw(j.line)
			}
		}
	}()
}

func parseEpochExpected(err error) uint64 {
	if err == nil {
		return 0
	}
	const p = "expected="
	msg := err.Error()
	i := strings.Index(msg, p)
	if i < 0 {
		return 0
	}
	rest := msg[i+len(p):]
	n := 0
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	if n == 0 {
		return 0
	}
	var out uint64
	for j := 0; j < n; j++ {
		out = out*10 + uint64(rest[j]-'0')
	}
	return out
}

func (s *server) withLeaderEpoch(region uint32, fn func(*storagetcp.Client, uint64) error) error {
	var last error
	epoch := s.epochOf(region)
	for attempt := 0; attempt < 3; attempt++ {
		use := epoch
		err := s.withLeader(region, func(c *storagetcp.Client) error {
			return fn(c, use)
		})
		if err == nil {
			return nil
		}
		last = err
		if exp := parseEpochExpected(err); exp > 0 {
			// Storage 权威 epoch 高于 Meta 缓存时，抬高本地再试（避免死磕 Meta 旧值）。
			s.routes.BumpEpoch(region, exp)
			epoch = exp
			continue
		}
		if strings.Contains(err.Error(), "splitting") ||
			strings.Contains(err.Error(), "epoch_not_match") {
			s.refreshRoutes()
			epoch = s.epochOf(region)
			continue
		}
		return err
	}
	return last
}

func (s *server) resolveBlockingLock(ctx context.Context, key []byte, le *storagetcp.LockedError) error {
	if le == nil || len(le.Primary) == 0 || le.LockTs == 0 {
		return fmt.Errorf("locked")
	}
	primaryRegion := s.regionOf(le.Primary)
	var status string
	var commitTs uint64
	err := s.withLeader(primaryRegion, func(c *storagetcp.Client) error {
		var e error
		status, commitTs, e = c.CheckTxnStatus(primaryRegion, le.Primary, le.LockTs)
		return e
	})
	if err != nil {
		return err
	}
	switch status {
	case "committed":
		return s.withLeaderEpoch(s.regionOf(key), func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.ResolveLock(s.regionOf(key), key, le.LockTs, commitTs, epoch)
			return e
		})
	case "rolledback", "expired":
		// expired：先清 primary 再 resolve secondary
		if status == "expired" {
			_ = s.withLeaderEpoch(primaryRegion, func(c *storagetcp.Client, epoch uint64) error {
				_, e := c.ResolveLock(primaryRegion, le.Primary, le.LockTs, 0, epoch)
				return e
			})
		}
		return s.withLeaderEpoch(s.regionOf(key), func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.ResolveLock(s.regionOf(key), key, le.LockTs, 0, epoch)
			return e
		})
	default:
		return fmt.Errorf("locked")
	}
}

func (s *server) TxnBegin(ctx context.Context, req *kvv1.TxnBeginRequest) (*kvv1.TxnBeginResponse, error) {
	n := req.Count
	if n == 0 {
		n = 1
	}
	start, err := route.AllocTs(s.metaAddrs, n)
	if err != nil {
		return nil, err
	}
	s.activeTs.add(start)
	return &kvv1.TxnBeginResponse{StartTs: start}, nil
}

func (s *server) TxnGet(ctx context.Context, req *kvv1.TxnGetRequest) (*kvv1.TxnGetResponse, error) {
	for attempt := 0; attempt < 3; attempt++ {
		region := s.regionOf(req.Key)
		var out *kvv1.TxnGetResponse
		err := s.withLeader(region, func(c *storagetcp.Client) error {
			val, found, e := c.TxnGet(region, req.Key, req.StartTs)
			if e != nil {
				return e
			}
			out = &kvv1.TxnGetResponse{Value: val, Found: found}
			return nil
		})
		if err == nil {
			return out, nil
		}
		var le *storagetcp.LockedError
		if errors.As(err, &le) {
			if re := s.resolveBlockingLock(ctx, req.Key, le); re != nil {
				if strings.Contains(re.Error(), "locked") {
					return &kvv1.TxnGetResponse{Locked: true}, nil
				}
				return nil, re
			}
			continue
		}
		return nil, err
	}
	return &kvv1.TxnGetResponse{Locked: true}, nil
}

func (s *server) TxnPrewrite(ctx context.Context, req *kvv1.TxnPrewriteRequest) (*kvv1.TxnPrewriteResponse, error) {
	if len(req.Mutations) == 0 {
		return &kvv1.TxnPrewriteResponse{Ok: false, Error: "empty"}, nil
	}
	primary := req.PrimaryKey
	if len(primary) == 0 {
		primary = req.Mutations[0].Key
	}
	for _, m := range req.Mutations {
		region := s.regionOf(m.Key)
		op := uint8(0)
		val := m.Value
		if m.IsDelete {
			op = 1
			val = nil
		}
		key := m.Key
		err := s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.Prewrite(region, key, req.StartTs, primary, op, val, epoch)
			return e
		})
		if err != nil {
			return &kvv1.TxnPrewriteResponse{Ok: false, Error: err.Error()}, nil
		}
	}
	return &kvv1.TxnPrewriteResponse{Ok: true}, nil
}

func (s *server) TxnCommit(ctx context.Context, req *kvv1.TxnCommitRequest) (*kvv1.TxnCommitResponse, error) {
	if len(req.Keys) == 0 {
		return &kvv1.TxnCommitResponse{Ok: false, Error: "empty"}, nil
	}
	commitTs, err := route.AllocTs(s.metaAddrs, 1)
	if err != nil {
		return nil, err
	}
	primary := req.PrimaryKey
	if len(primary) == 0 {
		primary = req.Keys[0]
	}
	// 1) 同步 commit primary（决定事务命运）
	pr := s.regionOf(primary)
	err = s.withLeaderEpoch(pr, func(c *storagetcp.Client, epoch uint64) error {
		_, e := c.CommitKey(pr, primary, req.StartTs, commitTs, epoch)
		return e
	})
	if err != nil {
		return &kvv1.TxnCommitResponse{Ok: false, CommitTs: commitTs, Error: err.Error()}, nil
	}
	s.activeTs.remove(req.StartTs)
	// 2) secondary：尽力同步；失败留给 CheckTxnStatus/Resolve 崩溃恢复
	secondaries := make([][]byte, 0, len(req.Keys))
	for _, k := range req.Keys {
		if string(k) == string(primary) {
			continue
		}
		secondaries = append(secondaries, k)
	}
	go s.asyncResolveSecondaries(req.StartTs, commitTs, secondaries)
	for _, key := range secondaries {
		region := s.regionOf(key)
		k := key
		_ = s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.CommitKey(region, k, req.StartTs, commitTs, epoch)
			return e
		})
	}
	return &kvv1.TxnCommitResponse{Ok: true, CommitTs: commitTs}, nil
}

func (s *server) asyncResolveSecondaries(startTs, commitTs uint64, keys [][]byte) {
	for _, key := range keys {
		region := s.regionOf(key)
		k := key
		err := s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.ResolveLock(region, k, startTs, commitTs, epoch)
			return e
		})
		if err != nil {
			log.Printf("async resolve secondary key=%q err=%v", string(k), err)
		}
	}
}

func (s *server) TxnRollback(ctx context.Context, req *kvv1.TxnRollbackRequest) (*kvv1.TxnRollbackResponse, error) {
	for _, key := range req.Keys {
		region := s.regionOf(key)
		k := key
		_ = s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.RollbackKey(region, k, req.StartTs, epoch)
			return e
		})
	}
	s.activeTs.remove(req.StartTs)
	return &kvv1.TxnRollbackResponse{Ok: true}, nil
}

func (s *server) TxnPessimisticLock(ctx context.Context, req *kvv1.TxnPessimisticLockRequest) (*kvv1.TxnPessimisticLockResponse, error) {
	if len(req.Keys) == 0 {
		return &kvv1.TxnPessimisticLockResponse{Ok: false, Error: "empty"}, nil
	}
	primary := req.PrimaryKey
	if len(primary) == 0 {
		primary = req.Keys[0]
	}
	forUpdate := req.ForUpdateTs
	if forUpdate == 0 {
		ts, err := route.AllocTs(s.metaAddrs, 1)
		if err != nil {
			return nil, err
		}
		forUpdate = ts
	}
	ttl := req.TtlMs
	if ttl == 0 {
		ttl = 5000
	}
	for _, key := range req.Keys {
		region := s.regionOf(key)
		k := key
		err := s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.PessimisticLock(region, k, req.StartTs, forUpdate, primary, ttl, epoch)
			return e
		})
		if err != nil {
			return &kvv1.TxnPessimisticLockResponse{Ok: false, Error: err.Error()}, nil
		}
	}
	return &kvv1.TxnPessimisticLockResponse{Ok: true}, nil
}

func (s *server) TxnPessimisticRollback(ctx context.Context, req *kvv1.TxnPessimisticRollbackRequest) (*kvv1.TxnPessimisticRollbackResponse, error) {
	for _, key := range req.Keys {
		region := s.regionOf(key)
		k := key
		_ = s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
			_, e := c.PessimisticRollback(region, k, req.StartTs, epoch)
			return e
		})
	}
	return &kvv1.TxnPessimisticRollbackResponse{Ok: true}, nil
}

func (s *server) TxnResolveLock(ctx context.Context, req *kvv1.TxnResolveLockRequest) (*kvv1.TxnResolveLockResponse, error) {
	region := s.regionOf(req.Key)
	err := s.withLeaderEpoch(region, func(c *storagetcp.Client, epoch uint64) error {
		_, e := c.ResolveLock(region, req.Key, req.StartTs, req.CommitTs, epoch)
		return e
	})
	if err != nil {
		return &kvv1.TxnResolveLockResponse{Ok: false, Error: err.Error()}, nil
	}
	return &kvv1.TxnResolveLockResponse{Ok: true}, nil
}

func (s *server) TxnCheckStatus(ctx context.Context, req *kvv1.TxnCheckStatusRequest) (*kvv1.TxnCheckStatusResponse, error) {
	region := s.regionOf(req.PrimaryKey)
	var status string
	var commitTs uint64
	err := s.withLeader(region, func(c *storagetcp.Client) error {
		var e error
		status, commitTs, e = c.CheckTxnStatus(region, req.PrimaryKey, req.LockTs)
		return e
	})
	if err != nil {
		return nil, err
	}
	return &kvv1.TxnCheckStatusResponse{Status: status, CommitTs: commitTs}, nil
}
