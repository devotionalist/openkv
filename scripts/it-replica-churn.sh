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

# I39: 快速加副本 / 立刻下线 / 并发 SCALE_OUT 排队，不得 scale_busy 假成功或半吊子路由。
# 经 Web API（Meta 已默认 gRPC）；不直连 Meta TCP:8500。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SKV="${SKV:-./build/tools/skv}"
WEB="${WEB:-http://127.0.0.1:19100}"
NODE_A="${NODE_A:-storage-3}"
NODE_B="${NODE_B:-storage-4}"
REGION="${REGION:-0}"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

wait_web() {
  for _ in $(seq 1 40); do
    if curl -sf "$WEB/api/overview" >/dev/null; then return 0; fi
    sleep 0.25
  done
  fail "web not ready at $WEB"
}

node_state() {
  local nid="$1"
  curl -sf "$WEB/api/nodes" | python3 -c '
import json,sys
nid=sys.argv[1]
for n in json.load(sys.stdin).get("nodes",[]):
  if n.get("id")==nid:
    print(n.get("state") or n.get("state_zh") or "")
    sys.exit(0)
print("")
' "$nid"
}

ensure_active() {
  local nid="$1"
  for _ in $(seq 1 60); do
    local st
    st=$(node_state "$nid")
    if [[ "$st" == "Active" || "$st" == "1" ]]; then return 0; fi
    curl -sf -X POST "$WEB/api/join-node" -H 'Content-Type: application/json' \
      -d "{\"node_id\":\"$nid\"}" >/dev/null 2>&1 || true
    sleep 0.5
  done
  fail "$nid not Active (state=$(node_state "$nid"))"
}

region_has_peer() {
  local rid="$1" nid="$2"
  curl -sf "$WEB/api/route" | python3 -c "
import json,sys
rid=int(sys.argv[1]); nid=sys.argv[2]
regs=json.load(sys.stdin).get('regions',[])
r=next((x for x in regs if x['id']==rid), None)
if not r: sys.exit(1)
sys.exit(0 if any(p['node_id']==nid for p in r.get('replicas',[])) else 1)
" "$rid" "$nid"
}

wait_peer_gone() {
  local rid="$1" nid="$2"
  local deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if ! region_has_peer "$rid" "$nid"; then return 0; fi
    sleep 0.4
  done
  fail "peer $nid still on R$rid"
}

wait_peer_present() {
  local rid="$1" nid="$2"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if region_has_peer "$rid" "$nid"; then return 0; fi
    sleep 0.5
  done
  fail "peer $nid not on R$rid"
}

wait_scale_terminal() {
  local tid="$1"
  local allow_cancel="${2:-0}"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    local body state done failed err
    body=$(curl -sf "$WEB/api/scale-task?task_id=$tid" || echo '{}')
    state=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("state_zh") or d.get("state") or "")')
    done=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("done"))')
    failed=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("failed"))')
    err=$(echo "$body" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("error") or "")')
    echo "  scale $tid → $state err=$err"
    if [[ "$done" == "True" ]]; then return 0; fi
    if [[ "$failed" == "True" ]]; then
      if [[ "$allow_cancel" == "1" && "$err" == *cancel* ]]; then return 0; fi
      fail "scale $tid failed: $body"
    fi
    sleep 0.6
  done
  fail "scale $tid timeout"
}

offline_and_kick() {
  local nid="$1"
  curl -sf -X POST "$WEB/api/replica-offline" -H 'Content-Type: application/json' \
    -d "{\"region_id\":$REGION,\"node_id\":\"$nid\"}" >/tmp/skv_off.json || true
  wait_peer_gone "$REGION" "$nid" || true
  curl -sf -X POST "$WEB/api/replica-kick" -H 'Content-Type: application/json' \
    -d "{\"region_id\":$REGION,\"node_id\":\"$nid\"}" >/dev/null || true
}

echo "==> cluster + web"
"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start"
wait_web
ensure_active "$NODE_A"
ensure_active "$NODE_B"
ok "nodes Active: $NODE_A $NODE_B"

if region_has_peer "$REGION" "$NODE_A"; then
  echo "==> cleanup: offline $NODE_A from R$REGION"
  offline_and_kick "$NODE_A"
fi
if region_has_peer "$REGION" "$NODE_B"; then
  echo "==> cleanup: offline $NODE_B from R$REGION"
  offline_and_kick "$NODE_B"
fi

echo "==> case1: rapid add → immediate offline ($NODE_A → R$REGION)"
ADD=$(curl -sf -X POST "$WEB/api/add-replica" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_A\"}")
echo "$ADD"
echo "$ADD" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("ok") else 1)' \
  || fail "add-replica not ok: $ADD"
TID=$(echo "$ADD" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("task_id",""))')
[[ -n "$TID" ]] || fail "no task_id"

OFF=$(curl -sf -X POST "$WEB/api/replica-offline" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_A\"}")
echo "$OFF"
echo "$OFF" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("ok") else 1)' \
  || fail "replica-offline not ok: $OFF"
echo "$OFF" | grep -qi 'cancel_scale' || echo "  (warn: logs missing cancel_scale)"

wait_scale_terminal "$TID" 1
wait_peer_gone "$REGION" "$NODE_A"
curl -sf -X POST "$WEB/api/replica-kick" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_A\"}" >/dev/null || true
ok "case1 rapid churn cleaned"

echo "==> case2: concurrent SCALE_OUT queues (not scale_busy Failed)"
ADD_A=$(curl -sf -X POST "$WEB/api/add-replica" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_A\"}")
ADD_B=$(curl -sf -X POST "$WEB/api/add-replica" -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_B\"}")
echo "A: $ADD_A"
echo "B: $ADD_B"
echo "$ADD_A" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("ok") else 1)' \
  || fail "add A failed: $ADD_A"
echo "$ADD_B" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("ok") else 1)' \
  || fail "add B failed (expect queued, not busy): $ADD_B"
echo "$ADD_B" | python3 -c '
import json,sys
d=json.load(sys.stdin)
err=(d.get("hint") or "")+(d.get("response") or "")
if "scale_busy" in err or d.get("state")==6:
    raise SystemExit("second add Failed/busy: "+str(d))
'
TID_A=$(echo "$ADD_A" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("task_id",""))')
TID_B=$(echo "$ADD_B" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("task_id",""))')
[[ -n "$TID_A" && -n "$TID_B" ]] || fail "missing task ids"
wait_scale_terminal "$TID_A" 0
wait_scale_terminal "$TID_B" 0
wait_peer_present "$REGION" "$NODE_A"
wait_peer_present "$REGION" "$NODE_B"
ok "case2 both promoted"

echo "==> case3: duplicate add → reject"
HTTP_CODE=$(curl -s -o /tmp/skv_dup.json -w '%{http_code}' -X POST "$WEB/api/add-replica" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":$REGION,\"node_id\":\"$NODE_A\"}")
DUP=$(cat /tmp/skv_dup.json)
if [[ "$HTTP_CODE" == "400" ]]; then
  ok "case3 rejected duplicate (http 400): $DUP"
elif echo "$DUP" | python3 -c 'import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get("ok") is False else 1)'; then
  ok "case3 rejected duplicate: $DUP"
else
  fail "expected reject, http=$HTTP_CODE body=$DUP"
fi

echo "==> cleanup: offline $NODE_A $NODE_B from R$REGION"
offline_and_kick "$NODE_A"
offline_and_kick "$NODE_B"

echo "==> KV smoke"
"$SKV" put churn-k "churn-v" >/dev/null
"$SKV" get churn-k | grep -q churn-v || fail "kv get"
ok "kv ok"

echo
echo "ALL PASS: it-replica-churn"
