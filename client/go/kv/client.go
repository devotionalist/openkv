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

package kv

import (
	"context"
	"fmt"
	"time"

	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client 是面向业务的 Go SDK，对接 Compute Proxy 的 gRPC KVService。
type Client struct {
	conn   *grpc.ClientConn
	raw    kvv1.KVServiceClient
	timeout time.Duration
}

type Option func(*Client)

func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

func Dial(ctx context.Context, computeAddr string, opts ...Option) (*Client, error) {
	conn, err := grpc.DialContext(ctx, computeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial compute %s: %w", computeAddr, err)
	}
	c := &Client{
		conn:    conn,
		raw:     kvv1.NewKVServiceClient(conn),
		timeout: 3 * time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, rev uint64, err error) {
	return c.GetOpt(ctx, key, true)
}

// GetOpt linearizable=false 走读副本（可能落后）。
func (c *Client) GetOpt(ctx context.Context, key []byte, linearizable bool) (value []byte, found bool, rev uint64, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.raw.Get(ctx, &kvv1.GetRequest{Key: key, Linearizable: linearizable})
	if err != nil {
		return nil, false, 0, err
	}
	return resp.Value, resp.Found, resp.Revision, nil
}

// Watch 订阅 key 或 prefix 变更；调用方应取消 ctx 结束流。
func (c *Client) Watch(ctx context.Context, key, prefix []byte, startRev uint64) (<-chan *kvv1.WatchEvent, <-chan error, error) {
	stream, err := c.raw.Watch(ctx, &kvv1.WatchRequest{
		Key:           key,
		KeyPrefix:     prefix,
		StartRevision: startRev,
	})
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan *kvv1.WatchEvent, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(ch)
		defer close(errCh)
		for {
			ev, e := stream.Recv()
			if e != nil {
				errCh <- e
				return
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()
	return ch, errCh, nil
}

func (c *Client) Put(ctx context.Context, key, value []byte) (rev uint64, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.raw.Put(ctx, &kvv1.PutRequest{Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fmt.Errorf("put rejected")
	}
	return resp.Revision, nil
}

func (c *Client) Delete(ctx context.Context, key []byte) (deleted bool, rev uint64, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.raw.Delete(ctx, &kvv1.DeleteRequest{Key: key})
	if err != nil {
		return false, 0, err
	}
	return resp.Deleted, resp.Revision, nil
}

func (c *Client) BatchPut(ctx context.Context, kvs map[string][]byte) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	req := &kvv1.BatchPutRequest{}
	for k, v := range kvs {
		req.Items = append(req.Items, &kvv1.KeyValue{Key: []byte(k), Value: v})
	}
	resp, err := c.raw.BatchPut(ctx, req)
	if err != nil {
		return err
	}
	for i, r := range resp.Results {
		if !r.Ok {
			return fmt.Errorf("batch put item %d failed", i)
		}
	}
	return nil
}

// Txn 是客户端侧 Percolator 乐观事务（协调在 Gateway）。
type Txn struct {
	c        *Client
	startTs  uint64
	primary  []byte
	mutations []*kvv1.TxnMutation
	keys     [][]byte
}

func (c *Client) Begin(ctx context.Context) (*Txn, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.raw.TxnBegin(ctx, &kvv1.TxnBeginRequest{Count: 1})
	if err != nil {
		return nil, err
	}
	return &Txn{c: c, startTs: resp.StartTs}, nil
}

func (t *Txn) StartTs() uint64 { return t.startTs }

func (t *Txn) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	ctx, cancel := t.c.ctx(ctx)
	defer cancel()
	resp, err := t.c.raw.TxnGet(ctx, &kvv1.TxnGetRequest{Key: key, StartTs: t.startTs})
	if err != nil {
		return nil, false, err
	}
	if resp.Locked {
		return nil, false, fmt.Errorf("key locked")
	}
	return resp.Value, resp.Found, nil
}

func (t *Txn) Put(key, value []byte) {
	if len(t.primary) == 0 {
		t.primary = append([]byte(nil), key...)
	}
	t.mutations = append(t.mutations, &kvv1.TxnMutation{Key: key, Value: value})
	t.keys = append(t.keys, key)
}

func (t *Txn) Delete(key []byte) {
	if len(t.primary) == 0 {
		t.primary = append([]byte(nil), key...)
	}
	t.mutations = append(t.mutations, &kvv1.TxnMutation{Key: key, IsDelete: true})
	t.keys = append(t.keys, key)
}

// LockKeys 悲观锁（for update）；随后 Put/Delete + Commit。
func (t *Txn) LockKeys(ctx context.Context, keys ...[]byte) error {
	ctx, cancel := t.c.ctx(ctx)
	defer cancel()
	if len(t.primary) == 0 && len(keys) > 0 {
		t.primary = append([]byte(nil), keys[0]...)
	}
	for _, k := range keys {
		t.keys = append(t.keys, k)
	}
	resp, err := t.c.raw.TxnPessimisticLock(ctx, &kvv1.TxnPessimisticLockRequest{
		StartTs:     t.startTs,
		PrimaryKey:  t.primary,
		Keys:        keys,
		TtlMs:       5000,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("pessimistic lock: %s", resp.Error)
	}
	return nil
}

func (t *Txn) Commit(ctx context.Context) (commitTs uint64, err error) {
	ctx, cancel := t.c.ctx(ctx)
	defer cancel()
	pw, err := t.c.raw.TxnPrewrite(ctx, &kvv1.TxnPrewriteRequest{
		StartTs:     t.startTs,
		PrimaryKey:  t.primary,
		Mutations:   t.mutations,
	})
	if err != nil {
		return 0, err
	}
	if !pw.Ok {
		return 0, fmt.Errorf("prewrite: %s", pw.Error)
	}
	cm, err := t.c.raw.TxnCommit(ctx, &kvv1.TxnCommitRequest{
		StartTs:    t.startTs,
		PrimaryKey: t.primary,
		Keys:       t.keys,
	})
	if err != nil {
		return 0, err
	}
	if !cm.Ok {
		return cm.CommitTs, fmt.Errorf("commit: %s", cm.Error)
	}
	return cm.CommitTs, nil
}

func (t *Txn) Rollback(ctx context.Context) error {
	ctx, cancel := t.c.ctx(ctx)
	defer cancel()
	_, err := t.c.raw.TxnRollback(ctx, &kvv1.TxnRollbackRequest{
		StartTs: t.startTs,
		Keys:    t.keys,
	})
	return err
}
