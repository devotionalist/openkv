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
	"math"
	"strings"
	"time"

	"github.com/amydemo/opskv/client/go/internal/storagetcp"
	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
)

func (s *server) matchWatch(req *kvv1.WatchRequest, key []byte) bool {
	if len(req.Key) > 0 {
		return bytes.Equal(req.Key, key)
	}
	if len(req.KeyPrefix) > 0 {
		return bytes.HasPrefix(key, req.KeyPrefix)
	}
	return true
}

func (s *server) regionsForWatch(req *kvv1.WatchRequest) []uint32 {
	if len(req.Key) > 0 {
		return []uint32{s.regionOf(req.Key)}
	}
	t := s.routes.Snapshot()
	if t == nil || len(t.Regions) == 0 {
		return []uint32{0}
	}
	ids := make([]uint32, 0, len(t.Regions))
	for _, r := range t.Regions {
		ids = append(ids, r.ID)
	}
	return ids
}

// Watch：轮询各 Region Leader 的 WATCH_POLL；revision = Raft apply index。
func (s *server) Watch(req *kvv1.WatchRequest, stream kvv1.KVService_WatchServer) error {
	since := req.StartRevision
	if since == 0 {
		for _, rid := range s.regionsForWatch(req) {
			_ = s.withLeader(rid, func(c *storagetcp.Client) error {
				head, err := c.WatchHead(rid)
				if err != nil {
					return err
				}
				if head > since {
					since = head
				}
				return nil
			})
		}
	}
	// 精确 key：先推当前快照（若存在）
	if len(req.Key) > 0 {
		tg, err := s.TxnGet(stream.Context(), &kvv1.TxnGetRequest{
			Key:     req.Key,
			StartTs: math.MaxUint64,
		})
		if err == nil && tg != nil && tg.Found && !tg.Locked {
			rev := since
			if rev == 0 {
				rev = 1
			}
			if err := stream.Send(&kvv1.WatchEvent{
				Type:     kvv1.WatchEvent_PUT,
				Key:      req.Key,
				Value:    tg.Value,
				Revision: rev,
			}); err != nil {
				return err
			}
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			for _, rid := range s.regionsForWatch(req) {
				var batch []storagetcp.WatchEvent
				var next uint64
				err := s.withLeader(rid, func(c *storagetcp.Client) error {
					var e error
					batch, next, e = c.WatchPoll(rid, since, 64)
					return e
				})
				if err != nil {
					if strings.Contains(err.Error(), "splitting") || strings.Contains(err.Error(), "not_leader") {
						s.refreshRoutes()
					}
					continue
				}
				for _, ev := range batch {
					if !s.matchWatch(req, ev.Key) {
						continue
					}
					if req.StartRevision > 0 && ev.Revision <= req.StartRevision {
						continue
					}
					typ := kvv1.WatchEvent_PUT
					if ev.Deleted {
						typ = kvv1.WatchEvent_DELETE
					}
					if err := stream.Send(&kvv1.WatchEvent{
						Type:     typ,
						Key:      ev.Key,
						Value:    ev.Value,
						Revision: ev.Revision,
					}); err != nil {
						return err
					}
				}
				if next > since {
					since = next
				}
			}
		}
	}
}
