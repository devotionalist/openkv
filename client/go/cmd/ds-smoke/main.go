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

// ds-smoke：对运行中 Gateway 验收 DS Hash/Set/List/ZSet。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/amydemo/opskv/client/go/ds"
	"github.com/amydemo/opskv/client/go/kv"
)

func fail(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+msg+"\n", args...)
	os.Exit(1)
}

func ok(msg string) { fmt.Println("OK:", msg) }

func main() {
	addr := env("GATEWAY", "127.0.0.1:19000")
	suite := strings.ToLower(env("DS_SUITE", "all"))
	ctx := context.Background()

	dsc, err := ds.Dial(ctx, addr, ds.WithTimeout(8*time.Second))
	if err != nil {
		fail("dial ds: %v", err)
	}
	defer dsc.Close()

	kvc, err := kv.Dial(ctx, addr, kv.WithTimeout(8*time.Second))
	if err != nil {
		fail("dial kv: %v", err)
	}
	defer kvc.Close()

	switch suite {
	case "hash":
		runHash(ctx, dsc, kvc)
	case "set":
		runSet(ctx, dsc)
	case "list":
		runList(ctx, dsc)
	case "zset":
		runZSet(ctx, dsc)
	case "all":
		runHash(ctx, dsc, kvc)
		runSet(ctx, dsc)
		runList(ctx, dsc)
		runZSet(ctx, dsc)
	default:
		fail("unknown DS_SUITE=%s", suite)
	}
	fmt.Println("ALL PASS: ds-smoke", suite)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func runHash(ctx context.Context, c *ds.Client, kvc *kv.Client) {
	key := []byte("ds-hash-smoke")
	_, _ = c.HDel(ctx, key, []byte("a"), []byte("b"))
	added, err := c.HSet(ctx, key, []byte("a"), []byte("1"))
	if err != nil || !added {
		fail("HSet a: %v added=%v", err, added)
	}
	_, err = c.HSet(ctx, key, []byte("b"), []byte("2"))
	if err != nil {
		fail("HSet b: %v", err)
	}
	v, found, err := c.HGet(ctx, key, []byte("a"))
	if err != nil || !found || string(v) != "1" {
		fail("HGet: %v %v %q", err, found, v)
	}
	n, err := c.HLen(ctx, key)
	if err != nil || n != 2 {
		fail("HLen: %v %d", err, n)
	}
	all, err := c.HGetAll(ctx, key)
	if err != nil || len(all) != 2 || string(all["b"]) != "2" {
		fail("HGetAll: %v %#v", err, all)
	}
	del, err := c.HDel(ctx, key, []byte("a"))
	if err != nil || del != 1 {
		fail("HDel: %v %d", err, del)
	}
	// 普通 Put 同名 ukey 不覆盖 Hash
	if _, err := kvc.Put(ctx, key, []byte("plain")); err != nil {
		fail("Put plain: %v", err)
	}
	n, err = c.HLen(ctx, key)
	if err != nil || n != 1 {
		fail("HLen after plain Put: %v %d", err, n)
	}
	// 错类型：对 Hash key 用 Set 编码空间不同 type 字节，不同 MetaKey——用同一 type 冲突需先 Put 成 Set
	// 验证：先建 Set 再 HGet → wrong_type
	skey := []byte("ds-type-conflict")
	if _, err := c.SAdd(ctx, skey, []byte("m")); err != nil {
		fail("SAdd setup: %v", err)
	}
	_, _, err = c.HGet(ctx, skey, []byte("f"))
	if err == nil || !stringsContains(err.Error(), "wrong_type") {
		fail("expect wrong_type, got %v", err)
	}
	ok("hash")
}

func runSet(ctx context.Context, c *ds.Client) {
	key := []byte("ds-set-smoke")
	_, _ = c.SRem(ctx, key, []byte("x"), []byte("y"))
	n, err := c.SAdd(ctx, key, []byte("x"), []byte("y"), []byte("x"))
	if err != nil || n != 2 {
		fail("SAdd: %v %d", err, n)
	}
	card, err := c.SCard(ctx, key)
	if err != nil || card != 2 {
		fail("SCard: %v %d", err, card)
	}
	okm, err := c.SIsMember(ctx, key, []byte("x"))
	if err != nil || !okm {
		fail("SIsMember: %v %v", err, okm)
	}
	ms, err := c.SMembers(ctx, key)
	if err != nil || len(ms) != 2 {
		fail("SMembers: %v %d", err, len(ms))
	}
	rm, err := c.SRem(ctx, key, []byte("x"))
	if err != nil || rm != 1 {
		fail("SRem: %v %d", err, rm)
	}
	ok("set")
}

func runList(ctx context.Context, c *ds.Client) {
	key := []byte("ds-list-smoke")
	for {
		_, found, _ := c.LPop(ctx, key)
		if !found {
			break
		}
	}
	n, err := c.RPush(ctx, key, []byte("a"), []byte("b"))
	if err != nil || n != 2 {
		fail("RPush: %v %d", err, n)
	}
	n, err = c.LPush(ctx, key, []byte("z"))
	if err != nil || n != 3 {
		fail("LPush: %v %d", err, n)
	}
	rng, err := c.LRange(ctx, key, 0, -1)
	if err != nil || len(rng) != 3 || string(rng[0]) != "z" || string(rng[2]) != "b" {
		fail("LRange: %v %v", err, rng)
	}
	v, found, err := c.LPop(ctx, key)
	if err != nil || !found || string(v) != "z" {
		fail("LPop: %v %v %q", err, found, v)
	}
	v, found, err = c.RPop(ctx, key)
	if err != nil || !found || string(v) != "b" {
		fail("RPop: %v %v %q", err, found, v)
	}
	ln, err := c.LLen(ctx, key)
	if err != nil || ln != 1 {
		fail("LLen: %v %d", err, ln)
	}
	ok("list")
}

func runZSet(ctx context.Context, c *ds.Client) {
	key := []byte("ds-zset-smoke")
	_, _ = c.ZRem(ctx, key, []byte("m1"), []byte("m2"), []byte("m0"))
	n, err := c.ZAdd(ctx, key, []byte("m1"), 1.0)
	if err != nil || n != 1 {
		fail("ZAdd m1: %v %d", err, n)
	}
	_, err = c.ZAdd(ctx, key, []byte("m0"), 0.5)
	if err != nil {
		fail("ZAdd m0: %v", err)
	}
	_, err = c.ZAdd(ctx, key, []byte("m2"), 2.0)
	if err != nil {
		fail("ZAdd m2: %v", err)
	}
	sc, found, err := c.ZScore(ctx, key, []byte("m1"))
	if err != nil || !found || sc != 1.0 {
		fail("ZScore: %v %v %v", err, found, sc)
	}
	card, err := c.ZCard(ctx, key)
	if err != nil || card != 3 {
		fail("ZCard: %v %d", err, card)
	}
	ms, scores, err := c.ZRange(ctx, key, 0, -1, true)
	if err != nil || len(ms) != 3 || string(ms[0]) != "m0" || scores[0] != 0.5 {
		fail("ZRange: %v %v %v", err, ms, scores)
	}
	rm, err := c.ZRem(ctx, key, []byte("m1"))
	if err != nil || rm != 1 {
		fail("ZRem: %v %d", err, rm)
	}
	ok("zset")
}

func stringsContains(s, sub string) bool {
	return strings.Contains(s, sub)
}
