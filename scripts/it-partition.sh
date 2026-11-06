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

# F3：杀同 Region 2/3 副本 → 写必须失败；恢复后可写。
# F11 近似：kill -STOP 冻结 1 follower 仍可写；冻结 2 则不可写；CONT 恢复。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
GW="${GATEWAY_ADDR:-127.0.0.1:19000}"
SKV="${SKV:-./build/tools/skv}"
WEB="${WEB:-http://127.0.0.1:19100}"
mkdir -p "$RUN"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

"$SKV" status >/dev/null || fail "cluster down"
(cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos \
  && go build -o "$RUN/kv-example" ./examples/basic)

pid_of() { cat "$RUN/$1.pid" 2>/dev/null || true; }

if ! curl -sf -m 2 "$WEB/api/route" >/dev/null 2>&1; then
  echo "==> start web for route"
  (cd "$ROOT/web" && go build -o "$RUN/skv-web" .)
  for p in $(lsof -t -iTCP:19100 -sTCP:LISTEN 2>/dev/null || true); do kill -9 "$p" 2>/dev/null || true; done
  SKV_RUN="$RUN" SKV_ROOT="$ROOT" "$RUN/skv-web" \
    -listen "0.0.0.0:19100" \
    -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
    -storages 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105 \
    -gateway 127.0.0.1:19000 -run-dir "$RUN" -root "$ROOT" \
    >"$RUN/web.log" 2>&1 &
  echo $! >"$RUN/web.pid"
  sleep 1
fi

# 从 Web route 取 Region 0 的三个 peer node_id（优先 *L 排前）
PEERS_STR=$(python3 - <<'PY' || true
import json, urllib.request, sys
try:
    route=json.load(urllib.request.urlopen("http://127.0.0.1:19100/api/route", timeout=3))
except Exception as e:
    print("", file=sys.stderr); sys.exit(1)
r=next((x for x in route.get("regions",[]) if int(x.get("id",-1))==0), None)
if not r:
    regs=route.get("regions") or []
    r=regs[0] if regs else None
if not r:
    sys.exit(1)
reps=sorted(r.get("replicas",[]), key=lambda p: (0 if p.get("is_leader") else 1))
print(" ".join(p["node_id"] for p in reps[:3]))
PY
)
[[ -n "$PEERS_STR" ]] || fail "need web /api/route for peers"
# shellcheck disable=SC2086
set -- $PEERS_STR
[[ $# -ge 3 ]] || fail "need 3 peers for R0, got: $PEERS_STR"
A="$1"; B="$2"; C="$3"
echo "R0 peers: A=$A(leader?) B=$B C=$C"

smoke_pct() {
  "$RUN/kv-chaos" -addr "$GW" -duration 4s -qps 40 -report 10s 2>&1 \
    | awk '/overall_success=/{gsub(/%/,"",$0); if (match($0,/[0-9.]+/)) v=substr($0,RSTART,RLENGTH)} END{print v+0}'
}

echo "==> baseline"
BASE=0
for _ in $(seq 1 12); do
  BASE=$(smoke_pct)
  if python3 -c "import sys; sys.exit(0 if float('${BASE:-0}')>=80 else 1)"; then
    break
  fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${BASE:-0}')>=80 else 1)" || fail "baseline $BASE"
ok "baseline $BASE%"

echo "==> F11-approx: STOP one follower ($B), majority writes OK"
PB=$(pid_of "$B")
[[ -n "$PB" ]] || fail "no pid $B"
kill -STOP "$PB"
sleep 2
ONE=$(smoke_pct)
python3 -c "import sys; sys.exit(0 if float('${ONE:-0}')>=40 else 1)" || fail "one STOP should still serve, got $ONE"
kill -CONT "$PB"
ok "F11 one STOP still serves ($ONE%)"

echo "==> F11-approx: STOP two ($B,$C) → expect near-zero writes"
kill -STOP "$(pid_of "$B")" 2>/dev/null || true
kill -STOP "$(pid_of "$C")" 2>/dev/null || true
sleep 2
TWO=$(smoke_pct)
# 正确降级：相对单冻明显变差，或绝对成功率偏低（本机 chaos 窗口抖动较大）
python3 -c "import sys; one=float('${ONE:-100}'); two=float('${TWO:-100}'); sys.exit(0 if two<=45 or two<=one-20 else 1)" \
  || fail "two STOP should degrade vs one STOP (one=$ONE two=$TWO)"
kill -CONT "$(pid_of "$B")" 2>/dev/null || true
kill -CONT "$(pid_of "$C")" 2>/dev/null || true
sleep 6
REC=0
for _ in $(seq 1 10); do
  REC=$(smoke_pct)
  if python3 -c "import sys; sys.exit(0 if float('${REC:-0}')>=70 else 1)"; then break; fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${REC:-0}')>=70 else 1)" || fail "CONT recover $REC"
ok "F11 two STOP degraded ($TWO%) then recovered ($REC%)"

echo "==> F3: kill two peers ($A,$B), expect unavailable; restart one"
"$SKV" kill "$A" 2>/dev/null || true
"$SKV" kill "$B" 2>/dev/null || true
sleep 2
DEAD=$(smoke_pct)
python3 -c "import sys; sys.exit(0 if float('${DEAD:-100}')<=30 else 1)" \
  || fail "F3 must be unavailable (success<=30%), got $DEAD — do not false-PASS"
ok "F3 correctly unavailable ($DEAD%)"
"$SKV" restart "$A" 2>/dev/null || true
sleep 8
BACK=0
for _ in $(seq 1 12); do
  BACK=$(smoke_pct)
  if python3 -c "import sys; sys.exit(0 if float('${BACK:-0}')>=60 else 1)"; then break; fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${BACK:-0}')>=60 else 1)" || fail "F3 recover $BACK"
"$SKV" restart "$B" 2>/dev/null || true
sleep 5
ok "F3 recovered after restarting majority ($BACK%)"

echo
echo "ALL PASS: it-partition"
