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
	"math"
	"sync"
	"time"
)

// activeTsTracker：本 Gateway 已知的活跃 start_ts（TxnBegin / 短 Put·Get）。
// 过期条目自动剔除，避免泄漏挡住 GC。
type activeTsTracker struct {
	mu  sync.Mutex
	m   map[uint64]time.Time
	ttl time.Duration
}

func newActiveTsTracker(ttl time.Duration) *activeTsTracker {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &activeTsTracker{m: make(map[uint64]time.Time), ttl: ttl}
}

func (t *activeTsTracker) add(ts uint64) {
	if ts == 0 {
		return
	}
	t.mu.Lock()
	t.m[ts] = time.Now()
	t.mu.Unlock()
}

func (t *activeTsTracker) remove(ts uint64) {
	t.mu.Lock()
	delete(t.m, ts)
	t.mu.Unlock()
}

// min 返回活跃最小 start_ts；无活跃则为 math.MaxUint64。
func (t *activeTsTracker) min() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	min := uint64(math.MaxUint64)
	for ts, at := range t.m {
		if now.Sub(at) > t.ttl {
			delete(t.m, ts)
			continue
		}
		if ts < min {
			min = ts
		}
	}
	return min
}
