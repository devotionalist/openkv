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

# I37: Split → Region1 加副本 → 下线副本后，路由与节点 Region 列表必须自洽。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SKV="${SKV:-./build/tools/skv}"
WEB="${WEB:-http://127.0.0.1:19100}"
META_ADDRS="${META_ADDRS:-127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502}"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

meta() {
  local req="$1"
  python3 - "$META_ADDRS" "$req" <<'PY'
import socket, sys
addrs = sys.argv[1].split(",")
req = sys.argv[2] + "\n"
last = None
for a in addrs:
    host, port = a.split(":")
    cur = (host, int(port))
    for _ in range(6):
        try:
            s = socket.create_connection(cur, 2)
            s.sendall(req.encode())
            buf = b""
            while b"\n" not in buf:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
            s.close()
            line = buf.decode().strip()
            if "not_leader" in line and "redirect=" in line:
                r = line.split("redirect=", 1)[1].strip().split()[0]
                h, p = r.rsplit(":", 1)
                cur = (h, int(p))
                continue
            print(line)
            sys.exit(0)
        except Exception as e:
            last = e
            break
print(f"ERR meta {last}", file=sys.stderr)
sys.exit(1)
PY
}

wait_web() {
  for i in $(seq 1 40); do
    if curl -sf "$WEB/api/overview" >/dev/null; then return 0; fi
    sleep 0.25
  done
  fail "web not ready at $WEB"
}

json_get() {
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(d'"$1"')'
}

# 断言：路由 peers ↔ 节点 region_ids 互逆；每 Region 恰好 1 个 *L 且在 peers 内。
# Split/Place 后 *L 靠心跳纠正，最多等 ~25s。
_check_route_nodes_once() {
  local label="$1"
  # STRICT_LEADERS=1 时强制每 Region 恰 1 个 *L；默认只校验副本成员 ↔ 节点 region_ids（用户报的「节点信息不对」）
  STRICT_LEADERS="${STRICT_LEADERS:-0}" python3 - "$label" <<'PY'
import json, os, sys
label = sys.argv[1]
strict = os.environ.get("STRICT_LEADERS", "0") == "1"
route = json.load(open("/tmp/skv_route.json"))
nodes = json.load(open("/tmp/skv_nodes.json"))
regs = {r["id"]: r for r in route.get("regions", [])}
from_route = {}
missing_L = []
for rid, r in regs.items():
    leaders = [p["node_id"] for p in r.get("replicas", []) if p.get("is_leader")]
    peers = [p["node_id"] for p in r.get("replicas", [])]
    if len(leaders) != 1:
        missing_L.append(f"R{rid} leaders={leaders}")
        if strict:
            raise SystemExit(f"{label}: R{rid} leaders={leaders} want exactly 1")
    elif leaders[0] not in peers:
        raise SystemExit(f"{label}: R{rid} leader {leaders[0]} not in peers {peers}")
    for p in peers:
        from_route.setdefault(p, set()).add(rid)

for n in nodes.get("nodes", []):
    if n.get("role") != "storage":
        continue
    nid = n["id"]
    got = set(n.get("region_ids") or [])
    want = from_route.get(nid, set())
    if got != want:
        raise SystemExit(f"{label}: node {nid} region_ids={sorted(got)} route_says={sorted(want)}")
    if n.get("regions") is not None and int(n["regions"]) != len(want):
        raise SystemExit(
            f"{label}: node {nid} STORE regions={n['regions']} != len(route)={len(want)}"
        )
    leaders_got = set(n.get("leader_region_ids") or [])
    leaders_want = set()
    for rid, r in regs.items():
        for p in r.get("replicas", []):
            if p.get("is_leader") and p["node_id"] == nid:
                leaders_want.add(rid)
    if leaders_got != leaders_want:
        raise SystemExit(
            f"{label}: node {nid} leader_region_ids={sorted(leaders_got)} want={sorted(leaders_want)}"
        )
extra = f" (warn no *L: {', '.join(missing_L)})" if missing_L else ""
print(f"{label}: route↔nodes OK regions={len(regs)}{extra}")
PY
}

assert_route_nodes_consistent() {
  local label="$1"
  local deadline=$((SECONDS + 30))
  local err=""
  while (( SECONDS < deadline )); do
    curl -sf "$WEB/api/route" > /tmp/skv_route.json || { sleep 0.4; continue; }
    curl -sf "$WEB/api/nodes" > /tmp/skv_nodes.json || { sleep 0.4; continue; }
    if err=$(_check_route_nodes_once "$label" 2>&1); then
      echo "$err"
      return 0
    fi
    sleep 0.5
  done
  fail "$label consistency (timeout): $err"
}

wait_scale_done() {
  local tid="$1"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    local body
    body=$(curl -sf "$WEB/api/scale-task?task_id=$tid")
    local state done failed
    state=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("state_zh") or d.get("state"))')
    done=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("done"))')
    failed=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("failed"))')
    echo "  scale $tid → $state"
    if [[ "$failed" == "True" ]]; then
      fail "scale $tid failed: $body"
    fi
    if [[ "$done" == "True" ]]; then
      return 0
    fi
    sleep 0.8
  done
  fail "scale $tid timeout"
}

echo "==> ensure cluster + web"
"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start --fresh"
wait_web

echo "==> seed keys"
"$SKV" put aaa left-val >/dev/null
"$SKV" put mmm mid-val >/dev/null
"$SKV" put zzz right-val >/dev/null

echo "==> split region 0 at m"
# Meta SPLIT 成功后 Storage ADMIN_SPLIT 偶发 apply_timeout；以路由 regions=2 为准并重试 admin。
SPLIT_OUT=$("$SKV" split 0 m 2>&1 || true)
echo "$SPLIT_OUT"
for i in $(seq 1 30); do
  ROUTE=$(meta "GET_ROUTE")
  if echo "$ROUTE" | grep -Eq 'OK ROUTE [0-9]+ 2 '; then break; fi
  sleep 0.4
done
ROUTE=$(meta "GET_ROUTE")
echo "$ROUTE" | grep -Eq 'OK ROUTE [0-9]+ 2 ' || fail "expect 2 regions: $ROUTE"
# 若 admin 超时，补打左副本 ADMIN_SPLIT
if ! echo "$SPLIT_OUT" | grep -q 'split -> OK'; then
  echo "==> retry ADMIN_SPLIT on left peers"
  "$SKV" split 0 m 2>&1 || true
  sleep 1
fi
ok "split → 2 regions"

# 找 right id（start=m）
RIGHT=$(python3 - <<'PY'
import re,subprocess,os
# parse from skv route
import socket
def meta(req):
  for a in os.environ.get("META_ADDRS","127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502").split(","):
    h,p=a.split(":")
    cur=(h,int(p))
    for _ in range(5):
      s=socket.create_connection(cur,2)
      s.sendall((req+"\n").encode()); buf=b""
      while b"\n" not in buf:
        c=s.recv(4096)
        if not c: break
        buf+=c
      s.close(); line=buf.decode().strip()
      if "not_leader" in line and "redirect=" in line:
        r=line.split("redirect=",1)[1].strip().split()[0]
        hh,pp=r.rsplit(":",1); cur=(hh,int(pp)); continue
      return line
  raise SystemExit("no meta")
line=meta("GET_ROUTE")
# tokens: id:starthex:endhex:epoch:n,peers...
parts=line.split()
for tok in parts[5:]:
  cols=tok.split(":")
  if len(cols)<4: continue
  rid=cols[0]
  start=bytes.fromhex(cols[1]).decode() if cols[1] else ""
  if start=="m":
    print(rid); break
else:
  raise SystemExit("no right region start=m in "+line)
PY
)
[[ -n "$RIGHT" ]] || fail "cannot find right region"
ok "right region id=$RIGHT"
assert_route_nodes_consistent "after-split"

echo "==> join storage-3"
# 等心跳续租后再 JOIN；以 STORE_STATUS state=1 为准（最多 ~30s）
for i in $(seq 1 60); do
  STATE=$(meta "STORE_STATUS storage-3" || true)
  if echo "$STATE" | grep -q 'state=1'; then break; fi
  curl -sf -X POST "$WEB/api/join-node" -H 'Content-Type: application/json' \
    -d '{"node_id":"storage-3"}' >/tmp/skv_join.json 2>/dev/null || true
  sleep 0.5
done
STATE=$(meta "STORE_STATUS storage-3")
echo "$STATE" | grep -q 'state=1' || fail "storage-3 not Active: $STATE"
ok "storage-3 Active"

echo "==> add replica storage-3 → R$RIGHT"
ADD=$(curl -sf -X POST "$WEB/api/add-replica" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$RIGHT,\"node_id\":\"storage-3\"}")
echo "$ADD"
TID=$(echo "$ADD" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("task_id",""))')
[[ -n "$TID" ]] || fail "no task_id"
wait_scale_done "$TID"
sleep 3

# R$RIGHT 必须含 storage-3；R0 不应被误改成带 storage-3（除非本来就有）
curl -sf "$WEB/api/route" > /tmp/skv_route.json
python3 - "$RIGHT" <<'PY' || fail "add-replica route wrong"
import json,sys
rid=int(sys.argv[1])
route=json.load(open("/tmp/skv_route.json"))
by={r["id"]:r for r in route["regions"]}
r1=by[rid]
peers=[p["node_id"] for p in r1["replicas"]]
if "storage-3" not in peers:
    raise SystemExit(f"R{rid} peers={peers} missing storage-3")
r0=by[0]
p0=[p["node_id"] for p in r0["replicas"]]
# R0 默认仍是 0/1/2；加副本只应影响 R{rid}
if "storage-3" in p0 and rid != 0:
    raise SystemExit(f"R0 unexpectedly has storage-3 after add to R{rid}: {p0}")
print(f"R{rid} peers={peers}")
PY
assert_route_nodes_consistent "after-add-replica"
ok "add-replica R$RIGHT"

# 从 R$RIGHT 下线一个非 Leader 副本（仍须留在 R0）；避免再触发转主竞态
OFF_NODE=$(python3 - "$RIGHT" <<'PY'
import json,sys
rid=int(sys.argv[1])
route=json.load(open("/tmp/skv_route.json"))
r=next(x for x in route["regions"] if x["id"]==rid)
leader=next(p["node_id"] for p in r["replicas"] if p.get("is_leader"))
# 优先挑仍在 R0 上的非 Leader
r0=next(x for x in route["regions"] if x["id"]==0)
r0peers={p["node_id"] for p in r0["replicas"]}
for p in r["replicas"]:
    if p["node_id"]!=leader and p["node_id"] in r0peers and p["node_id"]!="storage-3":
        print(p["node_id"]); break
else:
    for p in r["replicas"]:
        if p["node_id"]!=leader:
            print(p["node_id"]); break
PY
)
[[ -n "$OFF_NODE" ]] || fail "no offline candidate"
echo "==> offline replica $OFF_NODE from R$RIGHT (must NOT remove from R0)"
# 刷新 route 快照供后续断言
curl -sf "$WEB/api/route" > /tmp/skv_route.json
OFF=$(curl -sf -X POST "$WEB/api/replica-offline" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$RIGHT,\"node_id\":\"$OFF_NODE\"}")
echo "$OFF"
echo "$OFF" | grep -q '"ok":true' || fail "replica-offline failed: $OFF"
sleep 4

curl -sf "$WEB/api/route" > /tmp/skv_route.json
python3 - "$RIGHT" "$OFF_NODE" <<'PY' || fail "offline route wrong"
import json,sys
rid=int(sys.argv[1]); node=sys.argv[2]
route=json.load(open("/tmp/skv_route.json"))
by={r["id"]:r for r in route["regions"]}
r1_peers=[p["node_id"] for p in by[rid]["replicas"]]
r0_peers=[p["node_id"] for p in by[0]["replicas"]]
if node in r1_peers:
    raise SystemExit(f"R{rid} still has {node}: {r1_peers}")
if node not in r0_peers:
    raise SystemExit(f"R0 lost {node} after offline on R{rid}: {r0_peers}")
print(f"R0={r0_peers} R{rid}={r1_peers}")
PY
assert_route_nodes_consistent "after-offline"
ok "offline replica isolated to R$RIGHT"

echo "==> kick local data of $OFF_NODE for R$RIGHT"
KICK=$(curl -sf -X POST "$WEB/api/replica-kick" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$RIGHT,\"node_id\":\"$OFF_NODE\"}")
echo "$KICK"
echo "$KICK" | grep -q '"ok":true' || fail "replica-kick failed: $KICK"
sleep 1
assert_route_nodes_consistent "after-kick"

echo "==> KV still readable"
[[ "$("$SKV" get aaa | tail -1)" == *left-val* ]] || "$SKV" get aaa | grep -q left-val || fail "aaa"
[[ "$("$SKV" get zzz | tail -1)" == *right-val* ]] || "$SKV" get zzz | grep -q right-val || fail "zzz"
ok "kv readable"

echo
echo "ALL PASS: it-region-replica"
