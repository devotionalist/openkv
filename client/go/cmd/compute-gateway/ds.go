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

	"github.com/amydemo/opskv/client/go/internal/dsencode"
	kvv1 "github.com/amydemo/opskv/client/go/gen/kv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func dsErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, dsencode.ErrWrongType) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, dsencode.ErrCorrupt) {
		return status.Error(codes.DataLoss, err.Error())
	}
	return err
}

func (s *server) dsLoad(ctx context.Context, mk []byte) ([]byte, bool, error) {
	r, err := s.Get(ctx, &kvv1.GetRequest{Key: mk, Linearizable: true})
	if err != nil {
		return nil, false, err
	}
	if !r.Found {
		return nil, false, nil
	}
	return r.Value, true, nil
}

func (s *server) dsStore(ctx context.Context, mk, blob []byte) error {
	if len(blob) == 0 {
		_, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: mk})
		return err
	}
	_, err := s.Put(ctx, &kvv1.PutRequest{Key: mk, Value: blob})
	return err
}

func requireKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key required")
	}
	return nil
}

// ---- Hash ----

func (s *server) HSet(ctx context.Context, req *kvv1.HSetRequest) (*kvv1.HSetResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	if len(req.Field) == 0 {
		return nil, status.Error(codes.InvalidArgument, "field required")
	}
	mk := dsencode.MetaKey(req.Key)
	blob, _, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if h == nil {
		h = dsencode.Hash{}
	}
	_, existed := h[string(req.Field)]
	h[string(req.Field)] = append([]byte(nil), req.Value...)
	if err := s.dsStore(ctx, mk, dsencode.EncodeHash(h)); err != nil {
		return nil, err
	}
	return &kvv1.HSetResponse{Ok: true, Added: !existed}, nil
}

func (s *server) HGet(ctx context.Context, req *kvv1.HGetRequest) (*kvv1.HGetResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.HGetResponse{}, nil
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	v, ok := h[string(req.Field)]
	if !ok {
		return &kvv1.HGetResponse{}, nil
	}
	return &kvv1.HGetResponse{Value: v, Found: true}, nil
}

func (s *server) HDel(ctx context.Context, req *kvv1.HDelRequest) (*kvv1.HDelResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.HDelResponse{}, nil
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	var n int64
	for _, f := range req.Fields {
		if _, ok := h[string(f)]; ok {
			delete(h, string(f))
			n++
		}
	}
	var store []byte
	if len(h) > 0 {
		store = dsencode.EncodeHash(h)
	}
	if err := s.dsStore(ctx, mk, store); err != nil {
		return nil, err
	}
	return &kvv1.HDelResponse{Deleted: n}, nil
}

func (s *server) HExists(ctx context.Context, req *kvv1.HExistsRequest) (*kvv1.HExistsResponse, error) {
	r, err := s.HGet(ctx, &kvv1.HGetRequest{Key: req.Key, Field: req.Field})
	if err != nil {
		return nil, err
	}
	return &kvv1.HExistsResponse{Exists: r.Found}, nil
}

func (s *server) HLen(ctx context.Context, req *kvv1.HLenRequest) (*kvv1.HLenResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.HLenResponse{}, nil
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	return &kvv1.HLenResponse{Length: int64(len(h))}, nil
}

func (s *server) HGetAll(ctx context.Context, req *kvv1.HGetAllRequest) (*kvv1.HGetAllResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	out := &kvv1.HGetAllResponse{}
	if !found {
		return out, nil
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	for f, v := range h {
		out.Fields = append(out.Fields, []byte(f))
		out.Values = append(out.Values, v)
	}
	return out, nil
}

func (s *server) HKeys(ctx context.Context, req *kvv1.HKeysRequest) (*kvv1.HKeysResponse, error) {
	all, err := s.HGetAll(ctx, &kvv1.HGetAllRequest{Key: req.Key})
	if err != nil {
		return nil, err
	}
	return &kvv1.HKeysResponse{Fields: all.Fields}, nil
}

func (s *server) HMSet(ctx context.Context, req *kvv1.HMSetRequest) (*kvv1.HMSetResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	if len(req.Fields) != len(req.Values) {
		return nil, status.Error(codes.InvalidArgument, "fields/values length mismatch")
	}
	mk := dsencode.MetaKey(req.Key)
	blob, _, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	h, err := dsencode.DecodeHash(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if h == nil {
		h = dsencode.Hash{}
	}
	for i, f := range req.Fields {
		h[string(f)] = append([]byte(nil), req.Values[i]...)
	}
	if err := s.dsStore(ctx, mk, dsencode.EncodeHash(h)); err != nil {
		return nil, err
	}
	return &kvv1.HMSetResponse{Ok: true}, nil
}

func (s *server) HMGet(ctx context.Context, req *kvv1.HMGetRequest) (*kvv1.HMGetResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	out := &kvv1.HMGetResponse{}
	var h dsencode.Hash
	if found {
		h, err = dsencode.DecodeHash(blob)
		if err != nil {
			return nil, dsErr(err)
		}
	}
	for _, f := range req.Fields {
		v, ok := h[string(f)]
		out.Founds = append(out.Founds, ok)
		if ok {
			out.Values = append(out.Values, v)
		} else {
			out.Values = append(out.Values, nil)
		}
	}
	return out, nil
}

// ---- Set ----

func (s *server) SAdd(ctx context.Context, req *kvv1.SAddRequest) (*kvv1.SAddResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, _, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	set, err := dsencode.DecodeSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if set == nil {
		set = dsencode.Set{}
	}
	var added int64
	for _, m := range req.Members {
		if _, ok := set[string(m)]; !ok {
			set[string(m)] = struct{}{}
			added++
		}
	}
	if err := s.dsStore(ctx, mk, dsencode.EncodeSet(set)); err != nil {
		return nil, err
	}
	return &kvv1.SAddResponse{Added: added}, nil
}

func (s *server) SRem(ctx context.Context, req *kvv1.SRemRequest) (*kvv1.SRemResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.SRemResponse{}, nil
	}
	set, err := dsencode.DecodeSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	var n int64
	for _, m := range req.Members {
		if _, ok := set[string(m)]; ok {
			delete(set, string(m))
			n++
		}
	}
	var store []byte
	if len(set) > 0 {
		store = dsencode.EncodeSet(set)
	}
	if err := s.dsStore(ctx, mk, store); err != nil {
		return nil, err
	}
	return &kvv1.SRemResponse{Removed: n}, nil
}

func (s *server) SIsMember(ctx context.Context, req *kvv1.SIsMemberRequest) (*kvv1.SIsMemberResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.SIsMemberResponse{}, nil
	}
	set, err := dsencode.DecodeSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	_, ok := set[string(req.Member)]
	return &kvv1.SIsMemberResponse{IsMember: ok}, nil
}

func (s *server) SCard(ctx context.Context, req *kvv1.SCardRequest) (*kvv1.SCardResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.SCardResponse{}, nil
	}
	set, err := dsencode.DecodeSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	return &kvv1.SCardResponse{Card: int64(len(set))}, nil
}

func (s *server) SMembers(ctx context.Context, req *kvv1.SMembersRequest) (*kvv1.SMembersResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	out := &kvv1.SMembersResponse{}
	if !found {
		return out, nil
	}
	set, err := dsencode.DecodeSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	for m := range set {
		out.Members = append(out.Members, []byte(m))
	}
	return out, nil
}

// ---- List ----

func (s *server) LPush(ctx context.Context, req *kvv1.LPushRequest) (*kvv1.LPushResponse, error) {
	return s.listPush(ctx, req.Key, req.Values, true)
}

func (s *server) RPush(ctx context.Context, req *kvv1.RPushRequest) (*kvv1.RPushResponse, error) {
	r, err := s.listPush(ctx, req.Key, req.Values, false)
	if err != nil {
		return nil, err
	}
	return &kvv1.RPushResponse{Length: r.Length}, nil
}

func (s *server) listPush(ctx context.Context, key []byte, values [][]byte, left bool) (*kvv1.LPushResponse, error) {
	if err := requireKey(key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(key)
	blob, _, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	l, err := dsencode.DecodeList(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if left {
		for _, v := range values {
			l = append(dsencode.List{append([]byte(nil), v...)}, l...)
		}
	} else {
		for _, v := range values {
			l = append(l, append([]byte(nil), v...))
		}
	}
	if err := s.dsStore(ctx, mk, dsencode.EncodeList(l)); err != nil {
		return nil, err
	}
	return &kvv1.LPushResponse{Length: int64(len(l))}, nil
}

func (s *server) LPop(ctx context.Context, req *kvv1.LPopRequest) (*kvv1.LPopResponse, error) {
	return s.listPop(ctx, req.Key, true)
}

func (s *server) RPop(ctx context.Context, req *kvv1.RPopRequest) (*kvv1.RPopResponse, error) {
	r, err := s.listPop(ctx, req.Key, false)
	if err != nil {
		return nil, err
	}
	return &kvv1.RPopResponse{Value: r.Value, Found: r.Found}, nil
}

func (s *server) listPop(ctx context.Context, key []byte, left bool) (*kvv1.LPopResponse, error) {
	if err := requireKey(key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.LPopResponse{}, nil
	}
	l, err := dsencode.DecodeList(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if len(l) == 0 {
		return &kvv1.LPopResponse{}, nil
	}
	var v []byte
	if left {
		v = l[0]
		l = l[1:]
	} else {
		v = l[len(l)-1]
		l = l[:len(l)-1]
	}
	var store []byte
	if len(l) > 0 {
		store = dsencode.EncodeList(l)
	}
	if err := s.dsStore(ctx, mk, store); err != nil {
		return nil, err
	}
	return &kvv1.LPopResponse{Value: v, Found: true}, nil
}

func (s *server) LLen(ctx context.Context, req *kvv1.LLenRequest) (*kvv1.LLenResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.LLenResponse{}, nil
	}
	l, err := dsencode.DecodeList(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	return &kvv1.LLenResponse{Length: int64(len(l))}, nil
}

func (s *server) LRange(ctx context.Context, req *kvv1.LRangeRequest) (*kvv1.LRangeResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	out := &kvv1.LRangeResponse{}
	if !found {
		return out, nil
	}
	l, err := dsencode.DecodeList(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	lo, hi, ok := dsencode.NormalizeRange(int64(len(l)), req.Start, req.Stop)
	if !ok {
		return out, nil
	}
	out.Values = append(out.Values, l[lo:hi]...)
	return out, nil
}

func (s *server) LIndex(ctx context.Context, req *kvv1.LIndexRequest) (*kvv1.LIndexResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.LIndexResponse{}, nil
	}
	l, err := dsencode.DecodeList(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	idx := req.Index
	n := int64(len(l))
	if idx < 0 {
		idx = n + idx
	}
	if idx < 0 || idx >= n {
		return &kvv1.LIndexResponse{}, nil
	}
	return &kvv1.LIndexResponse{Value: l[idx], Found: true}, nil
}

// ---- ZSet ----

func (s *server) ZAdd(ctx context.Context, req *kvv1.ZAddRequest) (*kvv1.ZAddResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, _, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	z, err := dsencode.DecodeZSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	if z == nil {
		z = dsencode.ZSet{}
	}
	var added int64
	for _, it := range req.Items {
		if _, ok := z[string(it.Member)]; !ok {
			added++
		}
		z[string(it.Member)] = it.Score
	}
	if err := s.dsStore(ctx, mk, dsencode.EncodeZSet(z)); err != nil {
		return nil, err
	}
	return &kvv1.ZAddResponse{Added: added}, nil
}

func (s *server) ZRem(ctx context.Context, req *kvv1.ZRemRequest) (*kvv1.ZRemResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.ZRemResponse{}, nil
	}
	z, err := dsencode.DecodeZSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	var n int64
	for _, m := range req.Members {
		if _, ok := z[string(m)]; ok {
			delete(z, string(m))
			n++
		}
	}
	var store []byte
	if len(z) > 0 {
		store = dsencode.EncodeZSet(z)
	}
	if err := s.dsStore(ctx, mk, store); err != nil {
		return nil, err
	}
	return &kvv1.ZRemResponse{Removed: n}, nil
}

func (s *server) ZScore(ctx context.Context, req *kvv1.ZScoreRequest) (*kvv1.ZScoreResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.ZScoreResponse{}, nil
	}
	z, err := dsencode.DecodeZSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	sc, ok := z[string(req.Member)]
	if !ok {
		return &kvv1.ZScoreResponse{}, nil
	}
	return &kvv1.ZScoreResponse{Score: sc, Found: true}, nil
}

func (s *server) ZCard(ctx context.Context, req *kvv1.ZCardRequest) (*kvv1.ZCardResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	if !found {
		return &kvv1.ZCardResponse{}, nil
	}
	z, err := dsencode.DecodeZSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	return &kvv1.ZCardResponse{Card: int64(len(z))}, nil
}

func (s *server) ZRange(ctx context.Context, req *kvv1.ZRangeRequest) (*kvv1.ZRangeResponse, error) {
	if err := requireKey(req.Key); err != nil {
		return nil, err
	}
	mk := dsencode.MetaKey(req.Key)
	blob, found, err := s.dsLoad(ctx, mk)
	if err != nil {
		return nil, err
	}
	out := &kvv1.ZRangeResponse{}
	if !found {
		return out, nil
	}
	z, err := dsencode.DecodeZSet(blob)
	if err != nil {
		return nil, dsErr(err)
	}
	ents := dsencode.ZSorted(z)
	lo, hi, ok := dsencode.NormalizeRange(int64(len(ents)), req.Start, req.Stop)
	if !ok {
		return out, nil
	}
	for _, e := range ents[lo:hi] {
		out.Members = append(out.Members, []byte(e.Member))
		if req.WithScores {
			out.Scores = append(out.Scores, e.Score)
		}
	}
	return out, nil
}
