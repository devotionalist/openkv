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

package dsencode

import (
	"bytes"
	"errors"
	"testing"
)

func TestMetaKeyPrefix(t *testing.T) {
	ukey := []byte("user:1")
	mk := MetaKey(ukey)
	if !bytes.HasPrefix(mk, ukey) {
		t.Fatalf("MetaKey should be prefixed by ukey")
	}
	if bytes.Equal(mk, ukey) {
		t.Fatalf("MetaKey must differ from ukey")
	}
}

func TestHashRoundTrip(t *testing.T) {
	h := Hash{"a": []byte("1"), "b": []byte("2")}
	blob := EncodeHash(h)
	got, err := DecodeHash(blob)
	if err != nil {
		t.Fatal(err)
	}
	if string(got["a"]) != "1" || string(got["b"]) != "2" {
		t.Fatalf("got %#v", got)
	}
}

func TestWrongType(t *testing.T) {
	blob := EncodeHash(Hash{"f": []byte("v")})
	_, err := DecodeSet(blob)
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("want wrong_type, got %v", err)
	}
}

func TestSetListZSet(t *testing.T) {
	s := Set{"x": {}, "y": {}}
	sb := EncodeSet(s)
	sg, err := DecodeSet(sb)
	if err != nil || len(sg) != 2 {
		t.Fatalf("set %v %v", sg, err)
	}

	l := List{[]byte("a"), []byte("b"), []byte("c")}
	lb := EncodeList(l)
	lg, err := DecodeList(lb)
	if err != nil || len(lg) != 3 || string(lg[1]) != "b" {
		t.Fatalf("list %v %v", lg, err)
	}
	lo, hi, ok := NormalizeRange(3, 0, -1)
	if !ok || lo != 0 || hi != 3 {
		t.Fatalf("range %d %d %v", lo, hi, ok)
	}

	z := ZSet{"m1": 1.5, "m0": 0.5}
	zb := EncodeZSet(z)
	zg, err := DecodeZSet(zb)
	if err != nil || zg["m1"] != 1.5 {
		t.Fatalf("zset %v %v", zg, err)
	}
	ents := ZSorted(zg)
	if ents[0].Member != "m0" || ents[1].Member != "m1" {
		t.Fatalf("sorted %#v", ents)
	}
}
