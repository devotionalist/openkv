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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/amydemo/opskv/client/go/ds"
)

// handleDS：经 Gateway DSService 调试 Hash/Set/List/ZSet。
// POST JSON: { "op":"hset|hget|...", "key":"...", ... }
func (s *Server) handleDS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, fmt.Errorf("POST only"))
		return
	}
	var body struct {
		Op      string   `json:"op"`
		Key     string   `json:"key"`
		Field   string   `json:"field"`
		Value   string   `json:"value"`
		Member  string   `json:"member"`
		Members []string `json:"members"`
		Fields  []string `json:"fields"`
		Values  []string `json:"values"`
		Score   float64 `json:"score"`
		Start   int64   `json:"start"`
		Stop    *int64  `json:"stop"`
		Index   int64   `json:"index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	op := strings.ToLower(strings.TrimSpace(body.Op))
	if op == "" || body.Key == "" {
		writeErr(w, 400, fmt.Errorf("op and key required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	cli, err := ds.Dial(ctx, s.Gateway, ds.WithTimeout(25*time.Second))
	if err != nil {
		writeErr(w, 502, err)
		return
	}
	defer cli.Close()

	key := []byte(body.Key)
	out := map[string]any{"op": op, "key": body.Key, "ok": true}

	switch op {
	case "hset":
		added, err := cli.HSet(ctx, key, []byte(body.Field), []byte(body.Value))
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["added"] = added
	case "hget":
		v, found, err := cli.HGet(ctx, key, []byte(body.Field))
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["found"] = found
		out["field"] = body.Field
		out["value"] = string(v)
	case "hdel":
		fields := body.Fields
		if body.Field != "" {
			fields = append(fields, body.Field)
		}
		fb := make([][]byte, 0, len(fields))
		for _, f := range fields {
			fb = append(fb, []byte(f))
		}
		n, err := cli.HDel(ctx, key, fb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["deleted"] = n
	case "hexists":
		v, found, err := cli.HGet(ctx, key, []byte(body.Field))
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["exists"] = found
		_ = v
	case "hlen":
		n, err := cli.HLen(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["length"] = n
	case "hgetall":
		m, err := cli.HGetAll(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		sm := map[string]string{}
		for k, v := range m {
			sm[k] = string(v)
		}
		out["fields"] = sm
	case "hkeys":
		m, err := cli.HGetAll(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		out["fields"] = keys

	case "sadd":
		ms := body.Members
		if body.Member != "" {
			ms = append(ms, body.Member)
		}
		mb := make([][]byte, 0, len(ms))
		for _, m := range ms {
			mb = append(mb, []byte(m))
		}
		n, err := cli.SAdd(ctx, key, mb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["added"] = n
	case "srem":
		ms := body.Members
		if body.Member != "" {
			ms = append(ms, body.Member)
		}
		mb := make([][]byte, 0, len(ms))
		for _, m := range ms {
			mb = append(mb, []byte(m))
		}
		n, err := cli.SRem(ctx, key, mb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["removed"] = n
	case "sismember":
		ok, err := cli.SIsMember(ctx, key, []byte(body.Member))
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["is_member"] = ok
	case "scard":
		n, err := cli.SCard(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["card"] = n
	case "smembers":
		ms, err := cli.SMembers(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		arr := make([]string, len(ms))
		for i, m := range ms {
			arr[i] = string(m)
		}
		out["members"] = arr

	case "lpush":
		vs := body.Values
		if body.Value != "" {
			vs = append(vs, body.Value)
		}
		vb := make([][]byte, 0, len(vs))
		for _, v := range vs {
			vb = append(vb, []byte(v))
		}
		n, err := cli.LPush(ctx, key, vb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["length"] = n
	case "rpush":
		vs := body.Values
		if body.Value != "" {
			vs = append(vs, body.Value)
		}
		vb := make([][]byte, 0, len(vs))
		for _, v := range vs {
			vb = append(vb, []byte(v))
		}
		n, err := cli.RPush(ctx, key, vb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["length"] = n
	case "lpop":
		v, found, err := cli.LPop(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["found"] = found
		out["value"] = string(v)
	case "rpop":
		v, found, err := cli.RPop(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["found"] = found
		out["value"] = string(v)
	case "llen":
		n, err := cli.LLen(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["length"] = n
	case "lrange":
		stop := int64(-1)
		if body.Stop != nil {
			stop = *body.Stop
		}
		vs, err := cli.LRange(ctx, key, body.Start, stop)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		arr := make([]string, len(vs))
		for i, v := range vs {
			arr[i] = string(v)
		}
		out["values"] = arr
		out["start"] = body.Start
		out["stop"] = stop

	case "zadd":
		n, err := cli.ZAdd(ctx, key, []byte(body.Member), body.Score)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["added"] = n
		out["member"] = body.Member
		out["score"] = body.Score
	case "zrem":
		ms := body.Members
		if body.Member != "" {
			ms = append(ms, body.Member)
		}
		mb := make([][]byte, 0, len(ms))
		for _, m := range ms {
			mb = append(mb, []byte(m))
		}
		n, err := cli.ZRem(ctx, key, mb...)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["removed"] = n
	case "zscore":
		sc, found, err := cli.ZScore(ctx, key, []byte(body.Member))
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["found"] = found
		out["score"] = sc
		out["member"] = body.Member
	case "zcard":
		n, err := cli.ZCard(ctx, key)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		out["card"] = n
	case "zrange":
		stop := int64(-1)
		if body.Stop != nil {
			stop = *body.Stop
		}
		ms, scores, err := cli.ZRange(ctx, key, body.Start, stop, true)
		if err != nil {
			writeErr(w, 502, err)
			return
		}
		arr := make([]map[string]any, len(ms))
		for i, m := range ms {
			item := map[string]any{"member": string(m)}
			if i < len(scores) {
				item["score"] = scores[i]
			}
			arr[i] = item
		}
		out["items"] = arr
		out["start"] = body.Start
		out["stop"] = stop

	default:
		writeErr(w, 400, fmt.Errorf("unknown op %q", op))
		return
	}
	writeJSON(w, 200, out)
}
