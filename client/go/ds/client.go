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

// Package ds：数据结构 SDK（Hash/Set/List/ZSet），对接 Gateway DSService。
package ds

import (
	"context"
	"fmt"
	"time"

	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn    *grpc.ClientConn
	raw     kvv1.DSServiceClient
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
	c := &Client{conn: conn, raw: kvv1.NewDSServiceClient(conn), timeout: 5 * time.Second}
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

func (c *Client) HSet(ctx context.Context, key, field, value []byte) (added bool, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.HSet(ctx, &kvv1.HSetRequest{Key: key, Field: field, Value: value})
	if err != nil {
		return false, err
	}
	return r.Added, nil
}

func (c *Client) HGet(ctx context.Context, key, field []byte) (value []byte, found bool, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.HGet(ctx, &kvv1.HGetRequest{Key: key, Field: field})
	if err != nil {
		return nil, false, err
	}
	return r.Value, r.Found, nil
}

func (c *Client) HDel(ctx context.Context, key []byte, fields ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.HDel(ctx, &kvv1.HDelRequest{Key: key, Fields: fields})
	if err != nil {
		return 0, err
	}
	return r.Deleted, nil
}

func (c *Client) HLen(ctx context.Context, key []byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.HLen(ctx, &kvv1.HLenRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return r.Length, nil
}

func (c *Client) HGetAll(ctx context.Context, key []byte) (map[string][]byte, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.HGetAll(ctx, &kvv1.HGetAllRequest{Key: key})
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(r.Fields))
	for i, f := range r.Fields {
		out[string(f)] = r.Values[i]
	}
	return out, nil
}

func (c *Client) SAdd(ctx context.Context, key []byte, members ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.SAdd(ctx, &kvv1.SAddRequest{Key: key, Members: members})
	if err != nil {
		return 0, err
	}
	return r.Added, nil
}

func (c *Client) SRem(ctx context.Context, key []byte, members ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.SRem(ctx, &kvv1.SRemRequest{Key: key, Members: members})
	if err != nil {
		return 0, err
	}
	return r.Removed, nil
}

func (c *Client) SCard(ctx context.Context, key []byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.SCard(ctx, &kvv1.SCardRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return r.Card, nil
}

func (c *Client) SMembers(ctx context.Context, key []byte) ([][]byte, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.SMembers(ctx, &kvv1.SMembersRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return r.Members, nil
}

func (c *Client) SIsMember(ctx context.Context, key, member []byte) (bool, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.SIsMember(ctx, &kvv1.SIsMemberRequest{Key: key, Member: member})
	if err != nil {
		return false, err
	}
	return r.IsMember, nil
}

func (c *Client) LPush(ctx context.Context, key []byte, values ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.LPush(ctx, &kvv1.LPushRequest{Key: key, Values: values})
	if err != nil {
		return 0, err
	}
	return r.Length, nil
}

func (c *Client) RPush(ctx context.Context, key []byte, values ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.RPush(ctx, &kvv1.RPushRequest{Key: key, Values: values})
	if err != nil {
		return 0, err
	}
	return r.Length, nil
}

func (c *Client) LPop(ctx context.Context, key []byte) ([]byte, bool, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.LPop(ctx, &kvv1.LPopRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	return r.Value, r.Found, nil
}

func (c *Client) RPop(ctx context.Context, key []byte) ([]byte, bool, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.RPop(ctx, &kvv1.RPopRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	return r.Value, r.Found, nil
}

func (c *Client) LLen(ctx context.Context, key []byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.LLen(ctx, &kvv1.LLenRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return r.Length, nil
}

func (c *Client) LRange(ctx context.Context, key []byte, start, stop int64) ([][]byte, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.LRange(ctx, &kvv1.LRangeRequest{Key: key, Start: start, Stop: stop})
	if err != nil {
		return nil, err
	}
	return r.Values, nil
}

func (c *Client) ZAdd(ctx context.Context, key []byte, member []byte, score float64) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.ZAdd(ctx, &kvv1.ZAddRequest{
		Key:   key,
		Items: []*kvv1.ZMemberScore{{Member: member, Score: score}},
	})
	if err != nil {
		return 0, err
	}
	return r.Added, nil
}

func (c *Client) ZRem(ctx context.Context, key []byte, members ...[]byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.ZRem(ctx, &kvv1.ZRemRequest{Key: key, Members: members})
	if err != nil {
		return 0, err
	}
	return r.Removed, nil
}

func (c *Client) ZScore(ctx context.Context, key, member []byte) (float64, bool, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.ZScore(ctx, &kvv1.ZScoreRequest{Key: key, Member: member})
	if err != nil {
		return 0, false, err
	}
	return r.Score, r.Found, nil
}

func (c *Client) ZCard(ctx context.Context, key []byte) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.ZCard(ctx, &kvv1.ZCardRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return r.Card, nil
}

func (c *Client) ZRange(ctx context.Context, key []byte, start, stop int64, withScores bool) (members [][]byte, scores []float64, err error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.raw.ZRange(ctx, &kvv1.ZRangeRequest{Key: key, Start: start, Stop: stop, WithScores: withScores})
	if err != nil {
		return nil, nil, err
	}
	return r.Members, r.Scores, nil
}
