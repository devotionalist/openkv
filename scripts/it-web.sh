#!/usr/bin/env bash
# Copyright 2026 devotionalist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# I50–I54 + Web DS：管控台 HTTP 全链路（依赖已起的 6-Store 集群）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
WEB="${WEB:-http://127.0.0.1:19100}"
SKV="${SKV:-./build/tools/skv}"
mkdir -p "$RUN"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start"

ensure_web() {
  # 强制用本仓库 RUN/ROOT 拉起，避免占用 :19100 的旧进程 -run-dir 指错导致 I54 假成功
  echo "==> (re)start web on :19100 run-dir=$RUN"
  if [[ ! -f "$ROOT/web/static/index.html" ]]; then
    (cd "$ROOT/web/frontend" && npm install --no-fund --no-audit >/dev/null && npm run build >/dev/null)
  fi
  (cd "$ROOT/web" && go build -o "$RUN/skv-web" .)
  if [[ -f "$RUN/web.pid" ]]; then
    kill "$(cat "$RUN/web.pid")" 2>/dev/null || true
    rm -f "$RUN/web.pid"
  fi
  for p in $(lsof -t -iTCP:19100 -sTCP:LISTEN 2>/dev/null || true); do
    kill -9 "$p" 2>/dev/null || true
  done
  sleep 0.3
  SKV_RUN="$RUN" SKV_ROOT="$ROOT" "$RUN/skv-web" \
    -listen "0.0.0.0:19100" \
    -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
    -storages 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105 \
    -gateway 127.0.0.1:19000 \
    -run-dir "$RUN" \
    -root "$ROOT" \
    >"$RUN/web.log" 2>&1 &
  echo $! >"$RUN/web.pid"
  for _ in $(seq 1 40); do
    curl -sf -m 1 "$WEB/api/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  fail "web not ready; see $RUN/web.log"
}

ensure_web

echo "==> I50 overview/route/nodes/topology/ping"
curl -sf "$WEB/api/overview" -o /tmp/it_web_ov.json || fail "overview"
python3 - <<'PY' || fail "overview fields"
import json
d=json.load(open("/tmp/it_web_ov.json"))
for k in ("meta_up","storage_up","gateway_up","region_count","topology"):
    assert k in d, k
assert int(d.get("meta_up") or 0) >= 1, d
assert int(d.get("storage_up") or 0) >= 1, d
assert d.get("gateway_up"), d
assert int(d.get("region_count") or 0) >= 1, d
print(f"meta={d['meta_up']}/{d.get('meta_total')} store={d['storage_up']}/{d.get('storage_total')} regions={d['region_count']}")
PY
curl -sf "$WEB/api/route" -o /tmp/it_web_route.json || fail "route"
curl -sf "$WEB/api/nodes" -o /tmp/it_web_nodes.json || fail "nodes"
curl -sf "$WEB/api/topology" >/dev/null || fail "topology"
curl -sf "$WEB/api/ping" >/dev/null || fail "ping"
ok "I50 API smoke"

echo "==> I53 KV put/get/locate"
TS=$(date +%s)
KEY="itweb-$TS"
curl -sf -X POST "$WEB/api/kv/put" -H 'Content-Type: application/json' \
  -d "{\"key\":\"$KEY\",\"value\":\"wv1\"}" -o /tmp/it_web_put.json || fail "kv put"
python3 -c 'import json;d=json.load(open("/tmp/it_web_put.json")); assert d.get("ok"), d'
curl -sf "$WEB/api/kv/get?key=$KEY" -o /tmp/it_web_get.json || fail "kv get"
python3 -c 'import json;d=json.load(open("/tmp/it_web_get.json")); assert d.get("found") and d.get("value")=="wv1", d'
curl -sf "$WEB/api/locate?key=$KEY" -o /tmp/it_web_loc.json || fail "locate"
python3 -c 'import json;d=json.load(open("/tmp/it_web_loc.json")); assert d, d; print(d)'
curl -sf "$WEB/api/kv/get?key=$KEY&stale=1" >/dev/null || fail "stale get"
ok "I53 KV+locate"

echo "==> I52 split / find-merge-right / merge"
LEFTK="itw${TS}a"
MID="itw${TS}m"
RIGHTK="itw${TS}z"
curl -sf -X POST "$WEB/api/kv/put" -H 'Content-Type: application/json' \
  -d "{\"key\":\"$LEFTK\",\"value\":\"L\"}" >/dev/null
curl -sf -X POST "$WEB/api/kv/put" -H 'Content-Type: application/json' \
  -d "{\"key\":\"$RIGHTK\",\"value\":\"R\"}" >/dev/null
RID=$(python3 -c 'import json; r=json.load(open("/tmp/it_web_route.json")); print(sorted(r["regions"], key=lambda x:x["id"])[0]["id"])')
curl -sf -X POST "$WEB/api/split" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$RID,\"split_key\":\"$MID\"}" -o /tmp/it_web_spl.json || fail "split"
echo "split resp:"; head -c 240 /tmp/it_web_spl.json; echo
sleep 1
curl -sf "$WEB/api/kv/get?key=$LEFTK" -o /tmp/it_web_gl.json
curl -sf "$WEB/api/kv/get?key=$RIGHTK" -o /tmp/it_web_gr.json
python3 -c 'import json;d=json.load(open("/tmp/it_web_gl.json")); assert d.get("found") and d.get("value")=="L", d'
python3 -c 'import json;d=json.load(open("/tmp/it_web_gr.json")); assert d.get("found") and d.get("value")=="R", d'
curl -sf "$WEB/api/find-merge-right?left_id=$RID" -o /tmp/it_web_fm.json || true
RIGHT_ID=$(python3 -c 'import json;d=json.load(open("/tmp/it_web_fm.json")); print(d.get("right_id") or "")' 2>/dev/null || true)
if [[ -n "$RIGHT_ID" && "$RIGHT_ID" != "None" ]]; then
  curl -sf -X POST "$WEB/api/merge" -H 'Content-Type: application/json' \
    -d "{\"left_id\":$RID,\"right_id\":$RIGHT_ID}" -o /tmp/it_web_mrg.json || fail "merge"
  sleep 1
  curl -sf "$WEB/api/kv/get?key=$LEFTK" | python3 -c 'import json,sys;d=json.load(sys.stdin); assert d.get("found") and d.get("value")=="L", d'
  curl -sf "$WEB/api/kv/get?key=$RIGHTK" | python3 -c 'import json,sys;d=json.load(sys.stdin); assert d.get("found") and d.get("value")=="R", d'
  ok "I52 split+merge data intact"
else
  ok "I52 split (merge skipped: no merge-right)"
fi

echo "==> Web DS + wrong_type"
DSK="itwds-$TS"
curl -sf -X POST "$WEB/api/ds" -H 'Content-Type: application/json' \
  -d "{\"op\":\"hset\",\"key\":\"$DSK\",\"field\":\"f\",\"value\":\"1\"}" -o /tmp/it_web_hset.json || fail "hset"
python3 -c 'import json;d=json.load(open("/tmp/it_web_hset.json")); assert d.get("ok"), d'
curl -sf -X POST "$WEB/api/ds" -H 'Content-Type: application/json' \
  -d "{\"op\":\"hget\",\"key\":\"$DSK\",\"field\":\"f\"}" -o /tmp/it_web_hget.json || fail "hget"
python3 -c 'import json;d=json.load(open("/tmp/it_web_hget.json")); assert d.get("found") and d.get("value")=="1", d'
set +e
WT=$(curl -s -X POST "$WEB/api/ds" -H 'Content-Type: application/json' \
  -d "{\"op\":\"sadd\",\"key\":\"$DSK\",\"member\":\"x\"}")
set -e
echo "$WT" | python3 -c 'import sys;t=sys.stdin.read().lower(); assert "wrong_type" in t or "wrong type" in t or ("type" in t and "error" in t), t'
ok "Web DS + wrong_type"

echo "==> I54 node stop/start (storage-5)"
# 以端口/skv status 为准（TCP probe 比 overview 计数更稳）
port_up() { python3 -c "import socket,sys;s=socket.socket();s.settimeout(0.3);sys.exit(0 if s.connect_ex(('127.0.0.1',int(sys.argv[1])))==0 else 1)" "$1"; }
curl -sf -X POST "$WEB/api/node/stop" -H 'Content-Type: application/json' \
  -d '{"name":"storage-5"}' -o /tmp/it_web_stop.json || fail "node stop (need -run-dir); see $RUN/web.log"
python3 -c 'import json;d=json.load(open("/tmp/it_web_stop.json")); assert d.get("ok"), d'
down=0
for _ in $(seq 1 30); do
  if ! port_up 9105; then down=1; break; fi
  sleep 0.3
done
[[ "$down" == "1" ]] || fail "storage-5 port 9105 still up after stop"
curl -sf -X POST "$WEB/api/node/start" -H 'Content-Type: application/json' \
  -d '{"name":"storage-5"}' -o /tmp/it_web_start.json || fail "node start"
python3 -c 'import json;d=json.load(open("/tmp/it_web_start.json")); assert d.get("ok"), d'
up=0
for _ in $(seq 1 40); do
  if port_up 9105; then up=1; break; fi
  sleep 0.4
done
[[ "$up" == "1" ]] || fail "storage-5 port 9105 not back after start"
ok "I54 node stop/start"

echo
echo "ALL PASS: it-web"
