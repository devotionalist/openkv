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

// Package dsencode：DS 物理 key 与 v1 blob 编解码。
package dsencode

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// 类型字节
const (
	TypeHash byte = 'h'
	TypeSet  byte = 's'
	TypeList byte = 'l'
	TypeZSet byte = 'z'
)

var (
	ErrWrongType = errors.New("wrong_type")
	ErrCorrupt   = errors.New("corrupt_ds_blob")
)

// marker 插入 ukey 与 type 之间，保证与普通 KV 隔离且 ukey 仍为前缀。
var marker = []byte{0x00, 0x01, 'd', 's', 0x00}

// MetaKey = ukey + marker（类型只在 blob 内，保证同一 ukey 只能一种 DS 类型）。
func MetaKey(ukey []byte) []byte {
	out := make([]byte, 0, len(ukey)+len(marker))
	out = append(out, ukey...)
	out = append(out, marker...)
	return out
}

// ---- 二进制 blob：magic + typ + payload ----
// magic "SKVD" + typ(1) + ...

var magic = []byte{'S', 'K', 'V', 'D'}

func wrap(typ byte, payload []byte) []byte {
	out := make([]byte, 0, 5+len(payload))
	out = append(out, magic...)
	out = append(out, typ)
	out = append(out, payload...)
	return out
}

func unwrap(blob []byte) (typ byte, payload []byte, err error) {
	if len(blob) < 5 || !bytes.Equal(blob[:4], magic) {
		return 0, nil, ErrCorrupt
	}
	return blob[4], blob[5:], nil
}

func checkType(blob []byte, want byte) (payload []byte, err error) {
	if len(blob) == 0 {
		return nil, nil
	}
	typ, p, err := unwrap(blob)
	if err != nil {
		return nil, err
	}
	if typ != want {
		return nil, fmt.Errorf("%w: have=%c want=%c", ErrWrongType, typ, want)
	}
	return p, nil
}

func putBytes(buf *bytes.Buffer, b []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	buf.Write(n[:])
	buf.Write(b)
}

func getBytes(r *bytes.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := r.Read(n[:]); err != nil {
		return nil, ErrCorrupt
	}
	ln := binary.BigEndian.Uint32(n[:])
	if ln > 64<<20 {
		return nil, ErrCorrupt
	}
	b := make([]byte, ln)
	if _, err := r.Read(b); err != nil {
		return nil, ErrCorrupt
	}
	return b, nil
}

// ---------- Hash ----------

type Hash map[string][]byte

func EncodeHash(h Hash) []byte {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(keys)))
	buf.Write(n[:])
	for _, k := range keys {
		putBytes(&buf, []byte(k))
		putBytes(&buf, h[k])
	}
	return wrap(TypeHash, buf.Bytes())
}

func DecodeHash(blob []byte) (Hash, error) {
	p, err := checkType(blob, TypeHash)
	if err != nil || p == nil {
		return Hash{}, err
	}
	r := bytes.NewReader(p)
	var n [4]byte
	if _, err := r.Read(n[:]); err != nil {
		return nil, ErrCorrupt
	}
	cnt := binary.BigEndian.Uint32(n[:])
	h := make(Hash, cnt)
	for i := uint32(0); i < cnt; i++ {
		fk, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		fv, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		h[string(fk)] = fv
	}
	return h, nil
}

// ---------- Set ----------

type Set map[string]struct{}

func EncodeSet(s Set) []byte {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(keys)))
	buf.Write(n[:])
	for _, k := range keys {
		putBytes(&buf, []byte(k))
	}
	return wrap(TypeSet, buf.Bytes())
}

func DecodeSet(blob []byte) (Set, error) {
	p, err := checkType(blob, TypeSet)
	if err != nil || p == nil {
		return Set{}, err
	}
	r := bytes.NewReader(p)
	var n [4]byte
	if _, err := r.Read(n[:]); err != nil {
		return nil, ErrCorrupt
	}
	cnt := binary.BigEndian.Uint32(n[:])
	s := make(Set, cnt)
	for i := uint32(0); i < cnt; i++ {
		m, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		s[string(m)] = struct{}{}
	}
	return s, nil
}

// ---------- List ----------

type List [][]byte

func EncodeList(l List) []byte {
	var buf bytes.Buffer
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(l)))
	buf.Write(n[:])
	for _, v := range l {
		putBytes(&buf, v)
	}
	return wrap(TypeList, buf.Bytes())
}

func DecodeList(blob []byte) (List, error) {
	p, err := checkType(blob, TypeList)
	if err != nil || p == nil {
		return List{}, err
	}
	r := bytes.NewReader(p)
	var n [4]byte
	if _, err := r.Read(n[:]); err != nil {
		return nil, ErrCorrupt
	}
	cnt := binary.BigEndian.Uint32(n[:])
	l := make(List, 0, cnt)
	for i := uint32(0); i < cnt; i++ {
		v, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		l = append(l, v)
	}
	return l, nil
}

// NormalizeRange 将 Redis 风格 [start,stop] 转为切片下标（含两端）。
func NormalizeRange(n, start, stop int64) (lo, hi int, ok bool) {
	if n <= 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		return 0, 0, false
	}
	if start >= n {
		return 0, 0, false
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return 0, 0, false
	}
	return int(start), int(stop) + 1, true
}

// ---------- ZSet ----------

type ZSet map[string]float64

// ZEntry 按 score、member 排序后的项。
type ZEntry struct {
	Member string
	Score  float64
}

func EncodeZSet(z ZSet) []byte {
	ents := ZSorted(z)
	var buf bytes.Buffer
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(ents)))
	buf.Write(n[:])
	for _, e := range ents {
		putBytes(&buf, []byte(e.Member))
		var sb [8]byte
		binary.BigEndian.PutUint64(sb[:], math.Float64bits(e.Score))
		buf.Write(sb[:])
	}
	return wrap(TypeZSet, buf.Bytes())
}

func DecodeZSet(blob []byte) (ZSet, error) {
	p, err := checkType(blob, TypeZSet)
	if err != nil || p == nil {
		return ZSet{}, err
	}
	r := bytes.NewReader(p)
	var n [4]byte
	if _, err := r.Read(n[:]); err != nil {
		return nil, ErrCorrupt
	}
	cnt := binary.BigEndian.Uint32(n[:])
	z := make(ZSet, cnt)
	for i := uint32(0); i < cnt; i++ {
		m, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		var sb [8]byte
		if _, err := r.Read(sb[:]); err != nil {
			return nil, ErrCorrupt
		}
		z[string(m)] = math.Float64frombits(binary.BigEndian.Uint64(sb[:]))
	}
	return z, nil
}

func ZSorted(z ZSet) []ZEntry {
	ents := make([]ZEntry, 0, len(z))
	for m, s := range z {
		ents = append(ents, ZEntry{m, s})
	}
	sort.Slice(ents, func(i, j int) bool {
		if ents[i].Score != ents[j].Score {
			return ents[i].Score < ents[j].Score
		}
		return ents[i].Member < ents[j].Member
	})
	return ents
}
