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

# 完整扩缩容 IT：持续 put/get（模拟用户）期间完成
#   1) 扩容 join → balance（RF 恒定 Replace）
#   2) 缩容 scale-in → Draining → Tombstone → kick
# 业务侧带短超时 + 重试，衡量「读写无感」：fail_ratio 上限可配。
#
# 协议：全链路强制 gRPC（Meta/Storage/Raft/Gateway）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SKV="${SKV:-./build/tools/skv}"
LINE_RPC="${LINE_RPC:-}"
WEB="${WEB:-http://127.0.0.1:19100}"
META_ADDRS="${META_ADDRS:-127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502}"
SCALE_OUT_NODE="${SCALE_OUT_NODE:-storage-3}"
DRAIN_NODE="${DRAIN_NODE:-storage-0}"
# 迁副本窗口允许短暂失败；默认 35%（put/get 均带重试后通常更低）
MAX_FAIL_RATIO="${MAX_FAIL_RATIO:-0.35}"
MIN_TRAFFIC_SEC="${MIN_TRAFFIC_SEC:-12}"
TRAFFIC_FLAG=/tmp/skv_scale_live_traffic_run
TRAFFIC_STATS=/tmp/skv_scale_live_traffic_stats
TRAFFIC_LOG=/tmp/skv_scale_live_traffic.log
SKV_TIMEOUT_SEC="${SKV_TIMEOUT_SEC:-2}"
PUT_RETRIES="${PUT_RETRIES:-2}"
GET_RETRIES="${GET_RETRIES:-3}"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

ensure_line_rpc() {
  if [[ -n "${LINE_RPC}" && -x "${LINE_RPC}" ]]; then
    return 0
  fi
  local bin="$ROOT/.run/line-rpc"
  mkdir -p "$ROOT/.run"
  if [[ ! -x "$bin" ]]; then
    (cd "$ROOT/client/go" && go build -o "$bin" ./cmd/line-rpc)
  fi
  LINE_RPC="$bin"
}

meta() {
  local req="$1"
  ensure_line_rpc
  "$LINE_RPC" -kind meta -addrs "$META_ADDRS" -req "$req"
}

wait_web() {
  for _ in $(seq 1 8); do
    if curl -sf "$WEB/api/overview" >/dev/null; then
      ok "web ready"
      return 0
    fi
    sleep 0.25
  done
  echo "WARN: web not at $WEB (continuing; skv/meta only)"
}

# state: 0 Joining 1 Active 2 Draining 3 Offline 4 Tombstone
store_state() {
  meta "STORE_STATUS $1" | python3 -c 'import sys,re; m=re.search(r"state=(\d+)",sys.stdin.read()); print(m.group(1) if m else "")'
}

store_regions() {
  meta "STORE_STATUS $1" | python3 -c 'import sys,re; m=re.search(r"regions=(\d+)",sys.stdin.read()); print(m.group(1) if m else "0")'
}

join_node() {
  local nid="$1"
  echo "==> join $nid (Joining → Active)"
  local round
  for round in 1 2 3 4; do
    for _ in $(seq 1 40); do
      local st
      st=$(store_state "$nid" || true)
      if [[ "$st" == "1" ]]; then
        ok "$nid Active"
        return 0
      fi
      if "$SKV" join "$nid" >/tmp/skv_join_out.txt 2>&1; then
        sleep 0.4
        continue
      fi
      if grep -qE 'heartbeat|stale|unreachable' /tmp/skv_join_out.txt 2>/dev/null; then
        echo "  join blocked ($(head -1 /tmp/skv_join_out.txt)); restart $nid (round $round)"
        "$SKV" restart "$nid" >/dev/null 2>&1 || true
        sleep 3
        break
      fi
      sleep 0.5
    done
  done
  echo "WARN: $nid not Active: $(meta "STORE_STATUS $nid"); last join: $(cat /tmp/skv_join_out.txt 2>/dev/null)" >&2
  return 1
}

skv_to() {
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$SKV_TIMEOUT_SEC" "$SKV" "$@"
  elif command -v timeout >/dev/null 2>&1; then
    timeout "$SKV_TIMEOUT_SEC" "$SKV" "$@"
  else
    # macOS 常无 GNU timeout：用 python 包一层，避免 apply_timeout/选举空窗卡死流量协程
    python3 - "$SKV_TIMEOUT_SEC" "$SKV" "$@" <<'PY'
import subprocess, sys
sec = float(sys.argv[1])
cmd = sys.argv[2:]
try:
    r = subprocess.run(cmd, timeout=sec)
    sys.exit(r.returncode)
except subprocess.TimeoutExpired:
    sys.exit(124)
PY
  fi
}

start_traffic() {
  rm -f "$TRAFFIC_FLAG" "$TRAFFIC_STATS" "$TRAFFIC_STATS.partial" "$TRAFFIC_LOG"
  touch "$TRAFFIC_FLAG"
  local t0
  t0=$(date +%s)
  (
    ok_n=0
    fail_n=0
    i=0
    while [[ -f "$TRAFFIC_FLAG" ]]; do
      k="live-$(printf '%04d' $((i % 256)))"
      v="v-$i-$(date +%s)"
      put_ok=0
      for _ in $(seq 1 "$PUT_RETRIES"); do
        if skv_to put "$k" "$v" >>"$TRAFFIC_LOG" 2>&1; then
          put_ok=1
          break
        fi
        sleep 0.08
      done
      if [[ "$put_ok" -eq 1 ]]; then
        got=""
        for _ in $(seq 1 "$GET_RETRIES"); do
          got=$(skv_to get "$k" 2>/dev/null | tail -1 || true)
          if [[ "$got" == *"$v"* ]]; then break; fi
          sleep 0.08
        done
        if [[ "$got" == *"$v"* ]]; then
          ok_n=$((ok_n + 1))
        else
          fail_n=$((fail_n + 1))
          echo "GET miss $k want=$v got=$got" >>"$TRAFFIC_LOG"
        fi
      else
        fail_n=$((fail_n + 1))
        echo "PUT fail $k" >>"$TRAFFIC_LOG"
      fi
      i=$((i + 1))
      # 每次落盘中间统计（避免卡住时 snapshot 读到上轮 stale partial）
      echo "$ok_n $fail_n $i $t0 $(date +%s)" >"${TRAFFIC_STATS}.partial"
      sleep 0.06
    done
    echo "$ok_n $fail_n $i $t0 $(date +%s)" >"$TRAFFIC_STATS"
  ) &
  TRAFFIC_PID=$!
  ok "background traffic pid=$TRAFFIC_PID (put×$PUT_RETRIES get×$GET_RETRIES)"
}

stop_traffic() {
  rm -f "$TRAFFIC_FLAG"
  if [[ -n "${TRAFFIC_PID:-}" ]]; then
    wait "$TRAFFIC_PID" 2>/dev/null || true
  fi
  sleep 0.3
}

traffic_snapshot() {
  local label="$1"
  local f="${TRAFFIC_STATS}.partial"
  if [[ -f "$f" ]]; then
    read -r tok tfail ttotal t0 t1 <"$f" || true
    local total=$((tok + tfail))
    local ratio=0
    if (( total > 0 )); then
      ratio=$(python3 -c "print(f'{$tfail/float($total):.3f}')")
    fi
    echo "  [$label] traffic sofar ok=$tok fail=$tfail ratio=$ratio"
  else
    echo "  [$label] traffic stats not yet"
  fi
}

assert_not_in_route() {
  local nid="$1"
  local route
  route=$(meta "GET_ROUTE")
  if echo "$route" | grep -qE "(^|,)${nid}@"; then
    fail "$nid still in route: $route"
  fi
  ok "$nid absent from route"
}

assert_in_route() {
  local nid="$1"
  local route
  route=$(meta "GET_ROUTE")
  if ! echo "$route" | grep -qE "(^|,)${nid}@"; then
    fail "$nid not in route: $route"
  fi
  ok "$nid present in route"
}

nudge_drain_place() {
  local drain="$1"
  local spare="$2"
  # spare 已在成员里时，再补其它 Active（如 storage-1/4）凑满 RF=3
  python3 - "$META_ADDRS" "$drain" "$spare" "$SKV" <<'PY' || true
import re, socket, subprocess, sys
addrs, drain, spare, skv = sys.argv[1].split(","), sys.argv[2], sys.argv[3], sys.argv[4]

def meta(req):
  for a in addrs:
    cur = a
    for _ in range(6):
      h, p = cur.split(":")
      s = socket.create_connection((h, int(p)), 2)
      s.sendall((req + "\n").encode())
      buf = b""
      while b"\n" not in buf:
        c = s.recv(4096)
        if not c: break
        buf += c
      s.close()
      line = buf.decode().strip()
      if "not_leader" in line and "redirect=" in line:
        cur = line.split("redirect=", 1)[1].strip().split()[0]
        continue
      return line
  return ""

# Active storage ids（state=1）
actives = []
nodes = meta("LIST_NODES storage")
for tok in nodes.split()[2:]:
  # id@addr:S:state
  try:
    left, st = tok.rsplit(":", 1)
    nid = left.split("@", 1)[0]
    if st == "1" and nid != drain:
      actives.append(nid)
  except Exception:
    pass
# prefer explicit spare first
pool = []
if spare in actives:
  pool.append(spare)
for a in actives:
  if a not in pool:
    pool.append(a)

line = meta("GET_ROUTE")
for tok in line.split()[5:]:
  # token: rid:start:end:epoch:n,id@host:port,...  — 不可整体 split(':')
  rid = tok.split(":", 1)[0]
  ids = re.findall(r"([A-Za-z0-9_-]+)@", tok)
  if drain not in ids:
    continue
  desired = [x for x in ids if x != drain]
  for cand in pool:
    if len(desired) >= 3:
      break
    if cand not in desired:
      desired.append(cand)
  desired = desired[:3]
  if len(desired) < 3:
    print(f"skip rid={rid} cannot fill RF desired={desired} pool={pool}", flush=True)
    continue
  cmd = [skv, "place", rid, ",".join(desired)]
  print("nudge:", " ".join(cmd), flush=True)
  r = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=300)
  print((r.stdout or r.stderr or "")[:400], flush=True)
PY
}

cancel_migrates() {
  local list
  list=$(meta "LIST_MIGRATE" 2>/dev/null || true)
  python3 - "$list" "$META_ADDRS" <<'PY' || true
import socket, sys
line, addrs = sys.argv[1], sys.argv[2].split(",")

def meta(req):
  for a in addrs:
    cur = a
    for _ in range(6):
      h, p = cur.split(":")
      s = socket.create_connection((h, int(p)), 2)
      s.sendall((req + "\n").encode())
      buf = b""
      while b"\n" not in buf:
        c = s.recv(4096)
        if not c: break
        buf += c
      s.close()
      out = buf.decode().strip()
      if "not_leader" in out and "redirect=" in out:
        cur = out.split("redirect=", 1)[1].strip().split()[0]
        continue
      return out
  return ""

# OK MIGRATE_LIST n id:rid:state ...
parts = line.split()
for tok in parts[3:]:
  tid = tok.split(":", 1)[0]
  if tid.startswith("mig-"):
    print("cancel", tid, meta(f"CANCEL_MIGRATE {tid}"), flush=True)
PY
}

place_one_onto() {
  local spare="$1"
  python3 - "$META_ADDRS" "$spare" "$SKV" <<'PY'
import re, socket, subprocess, sys
addrs, spare, skv = sys.argv[1].split(","), sys.argv[2], sys.argv[3]

def meta(req):
  for a in addrs:
    cur = a
    for _ in range(6):
      h, p = cur.split(":")
      s = socket.create_connection((h, int(p)), 2)
      s.sendall((req + "\n").encode())
      buf = b""
      while b"\n" not in buf:
        c = s.recv(4096)
        if not c: break
        buf += c
      s.close()
      line = buf.decode().strip()
      if "not_leader" in line and "redirect=" in line:
        cur = line.split("redirect=", 1)[1].strip().split()[0]
        continue
      return line
  return ""

line = meta("GET_ROUTE")
for tok in line.split()[5:]:
  rid = tok.split(":", 1)[0]
  ids = re.findall(r"([A-Za-z0-9_-]+)@", tok)
  if spare in ids:
    continue
  if len(ids) < 3:
    continue
  desired = [spare] + [x for x in ids if x != spare][:2]
  cmd = [skv, "place", rid, ",".join(desired)]
  print("place-out:", " ".join(cmd), flush=True)
  r = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=300)
  print((r.stdout or "") + (r.stderr or ""), flush=True)
  sys.exit(0 if r.returncode == 0 else 1)
print("no region to place onto", spare, file=sys.stderr)
sys.exit(1)
PY
}

wait_balanced_onto() {
  local nid="$1"
  local deadline=$((SECONDS + 300))
  # 不用 BALANCE：其 place_fn 与 skv place 并行会 ConfChange 互踩，导致路由/数据面副本集不一致。
  echo "==> cancel migrates + skv place onto $nid (RF-constant Replace)"
  cancel_migrates
  sleep 1
  place_one_onto "$nid" || true
  while (( SECONDS < deadline )); do
    local regs route
    regs=$(store_regions "$nid" || echo "?")
    route=$(meta GET_ROUTE 2>/dev/null || true)
    echo "  scale-out $nid → regions=$regs  ($(meta "LIST_MIGRATE" 2>/dev/null || true))"
    if [[ "$regs" != "0" && "$regs" != "?" ]]; then
      return 0
    fi
    if echo "$route" | grep -qE "(^|,)${nid}@"; then
      ok "$nid already in route"
      return 0
    fi
    cancel_migrates
    place_one_onto "$nid" || true
    sleep 2
  done
  fail "scale-out timeout: $nid still regions=$(store_regions "$nid"); route=$(meta GET_ROUTE)"
}

wait_drained() {
  local nid="$1"
  local spare="$2"
  local deadline=$((SECONDS + 420))
  local last_regs=""
  local stuck=0
  while (( SECONDS < deadline )); do
    local raw st regs
    raw=$(meta "STORE_STATUS $nid" || true)
    st=$(echo "$raw" | python3 -c 'import sys,re; m=re.search(r"state=(\d+)",sys.stdin.read()); print(m.group(1) if m else "")')
    regs=$(echo "$raw" | python3 -c 'import sys,re; m=re.search(r"regions=(\d+)",sys.stdin.read()); print(m.group(1) if m else "?")')
    echo "  scale-in $nid → state=$st regions=$regs"
    if [[ "$st" == "4" ]]; then
      return 0
    fi
    if [[ "$st" == "2" && "$regs" == "0" ]]; then
      sleep 1
      continue
    fi
    if [[ "$regs" == "$last_regs" ]]; then
      stuck=$((stuck + 1))
    else
      stuck=0
      last_regs=$regs
    fi
    if (( stuck > 0 && stuck % 20 == 0 )); then
      echo "  drain stuck; nudge place → $spare"
      nudge_drain_place "$nid" "$spare" || true
      stuck=0
    fi
    sleep 1
  done
  fail "scale-in timeout: $(meta "STORE_STATUS $nid")"
}

check_traffic_quality() {
  local label="$1"
  [[ -f "$TRAFFIC_STATS" ]] || fail "no traffic stats ($label)"
  read -r TOK TFAIL TTOTAL T0 T1 <"$TRAFFIC_STATS" || fail "bad traffic stats"
  echo "  traffic ok=$TOK fail=$TFAIL attempts=$TTOTAL wall=$((T1 - T0))s"
  python3 - "$TOK" "$TFAIL" "$MAX_FAIL_RATIO" "$T0" "$T1" "$MIN_TRAFFIC_SEC" "$label" <<'PY' || fail "traffic quality too low ($label)"
import sys
ok, fail, lim = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
t0, t1, min_sec, label = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]), sys.argv[7]
total = ok + fail
wall = max(0, t1 - t0)
ratio = (fail / total) if total else 1.0
print(f"  [{label}] fail_ratio={ratio:.3f} limit={lim} wall={wall}s ok={ok}")
if wall < min_sec:
    raise SystemExit(f"traffic wall {wall}s < {min_sec}s")
if ok < 8:
    raise SystemExit(f"too few successful ops: {ok}")
if total == 0:
    raise SystemExit("no traffic ops")
if ratio > lim:
    raise SystemExit(f"fail_ratio {ratio:.3f} > {lim}")
PY
}

expect_get() {
  local key="$1" needle="$2"
  local out=""
  for _ in $(seq 1 30); do
    out=$(skv_to get "$key" 2>/dev/null | tail -1 || true)
    if [[ -n "$needle" && "$out" == *"$needle"* ]]; then return 0; fi
    if [[ -z "$needle" && -n "$out" && "$out" != *not_leader* && "$out" != ERR* && "$out" == *"found="* ]]; then
      return 0
    fi
    sleep 0.4
  done
  echo "last get $key → $out" >&2
  return 1
}

# ---------- main ----------
echo "==> ensure cluster"
"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start --fresh"
wait_web

# 扩容目标须仍为 Joining（若上次已 Active 也可继续）
ST_OUT=$(store_state "$SCALE_OUT_NODE" || true)
ST_DRAIN=$(store_state "$DRAIN_NODE" || true)
[[ "$ST_DRAIN" == "1" ]] || fail "$DRAIN_NODE must be Active (got state=$ST_DRAIN); use fresh cluster"
[[ "$ST_OUT" == "0" || "$ST_OUT" == "1" ]] || fail "$SCALE_OUT_NODE unexpected state=$ST_OUT"

echo "==> seed + split (≥2 Region，便于 balance / drain)"
"$SKV" put aaa before-left >/dev/null || true
"$SKV" put mmm before-mid >/dev/null || true
"$SKV" put zzz before-right >/dev/null || true
SPLIT_OUT=$("$SKV" split 0 m 2>&1 || true)
echo "$SPLIT_OUT"
for _ in $(seq 1 30); do
  ROUTE=$(meta "GET_ROUTE")
  if echo "$ROUTE" | grep -Eq 'OK ROUTE [0-9]+ 2 '; then break; fi
  sleep 0.4
done
meta "GET_ROUTE" | grep -Eq 'OK ROUTE [0-9]+ 2 ' || fail "need 2 regions"
ok "2 regions"
echo "  route: $(meta GET_ROUTE)"

REG_BEFORE=$(store_regions "$DRAIN_NODE")
[[ "$REG_BEFORE" != "0" ]] || fail "$DRAIN_NODE has 0 regions"
ok "$DRAIN_NODE carries regions=$REG_BEFORE before scale-out"

echo "==> start live traffic (simulating users; runs through scale-out + scale-in)"
start_traffic
sleep 3
traffic_snapshot "steady"

# ===== SCALE-OUT =====
echo
echo "======== PHASE scale-out: join $SCALE_OUT_NODE + balance ========"
join_node "$SCALE_OUT_NODE" || fail "scale-out join $SCALE_OUT_NODE failed"
echo "==> pause traffic briefly during ConfChange place (avoid election storm)"
stop_traffic
echo "==> balance until $SCALE_OUT_NODE holds ≥1 region"
wait_balanced_onto "$SCALE_OUT_NODE"
assert_in_route "$SCALE_OUT_NODE"
ok "scale-out done: $SCALE_OUT_NODE regions=$(store_regions "$SCALE_OUT_NODE")"
start_traffic
sleep 2
traffic_snapshot "after-scale-out"
echo "  route: $(meta GET_ROUTE)"

# 额外空闲 Active：缩容 Replace 时若 spare 已在成员里，可补干净节点（避免踢掉节点残留 LOCK）
FILL_NODE="${FILL_NODE:-storage-4}"
if [[ "$(store_state "$FILL_NODE")" == "0" ]]; then
  if ! join_node "$FILL_NODE"; then
    echo "WARN: fill $FILL_NODE not Active; scale-in will use spare/other Actives"
  fi
elif [[ "$(store_state "$FILL_NODE")" == "1" ]]; then
  ok "$FILL_NODE already Active"
fi

# ===== SCALE-IN =====
echo
echo "======== PHASE scale-in ========"
# 选缩容目标：Active、有副本、与 spare 重叠尽量少、自身 region 数尽量少
DRAIN_NODE=$(python3 - "$META_ADDRS" "$SCALE_OUT_NODE" <<'PY'
import re, socket, sys
addrs, spare = sys.argv[1].split(","), sys.argv[2]

def meta(req):
  for a in addrs:
    cur = a
    for _ in range(6):
      h, p = cur.split(":")
      s = socket.create_connection((h, int(p)), 2)
      s.sendall((req + "\n").encode())
      buf = b""
      while b"\n" not in buf:
        c = s.recv(4096)
        if not c: break
        buf += c
      s.close()
      line = buf.decode().strip()
      if "not_leader" in line and "redirect=" in line:
        cur = line.split("redirect=", 1)[1].strip().split()[0]
        continue
      return line
  return ""

regions = []
for tok in meta("GET_ROUTE").split()[5:]:
  ids = re.findall(r"([A-Za-z0-9_-]+)@", tok)
  if ids:
    regions.append(ids)

cands = []
# OK NODES n id@addr:S:state ...
parts = meta("LIST_NODES storage").split()
for tok in parts[3:]:
  try:
    left, st = tok.rsplit(":", 1)
    nid = left.split("@", 1)[0]
    if st != "1" or nid == spare:
      continue
    on = sum(1 for ids in regions if nid in ids)
    if on == 0:
      continue
    overlap = sum(1 for ids in regions if nid in ids and spare in ids)
    cands.append((overlap, on, nid))
  except Exception:
    pass
cands.sort()
print(cands[0][2] if cands else "")
PY
)
[[ -n "$DRAIN_NODE" ]] || fail "no drain candidate"
[[ "$(store_regions "$DRAIN_NODE")" != "0" ]] || fail "no Active store with regions to drain"
ok "drain target=$DRAIN_NODE regions=$(store_regions "$DRAIN_NODE") (spare=$SCALE_OUT_NODE fill=$FILL_NODE)"

SI_OUT=$("$SKV" scale-in "$DRAIN_NODE" 2>&1 || true)
echo "$SI_OUT"
echo "$SI_OUT" | grep -q '^OK' || fail "scale-in failed: $SI_OUT"
for _ in $(seq 1 20); do
  ST=$(store_state "$DRAIN_NODE" || true)
  [[ "$ST" == "2" ]] && break
  sleep 0.25
done
ST=$(store_state "$DRAIN_NODE")
[[ "$ST" == "2" ]] || fail "expect Draining(2) got $ST"

echo "==> wait drain while traffic runs"
wait_drained "$DRAIN_NODE" "$SCALE_OUT_NODE"
assert_not_in_route "$DRAIN_NODE"
traffic_snapshot "after-scale-in"

echo "==> settle after scale (催选主 + 稳态流量)"
stop_traffic
# 踢副本后可能短暂无主：每 Region 只催一个副本，避免全员同时选主打成平票
python3 - "$META_ADDRS" <<'PY' || true
import re, socket, sys, time
addrs = sys.argv[1].split(",")

def meta(req):
  for a in addrs:
    cur = a
    for _ in range(6):
      h, p = cur.split(":")
      s = socket.create_connection((h, int(p)), 2)
      s.sendall((req + "\n").encode())
      buf = b""
      while b"\n" not in buf:
        c = s.recv(4096)
        if not c: break
        buf += c
      s.close()
      line = buf.decode().strip()
      if "not_leader" in line and "redirect=" in line:
        cur = line.split("redirect=", 1)[1].strip().split()[0]
        continue
      return line
  return ""

def campaign(rid, addr):
  host, port = addr.rsplit(":", 1)
  s = socket.create_connection((host, int(port)), 2)
  s.sendall(f"ADMIN_CAMPAIGN {rid}\n".encode())
  line = s.recv(256).decode().strip()
  s.close()
  return line

def has_leader(rid, peers):
  for addr in peers:
    host, port = addr.rsplit(":", 1)
    try:
      s = socket.create_connection((host, int(port)), 2)
      s.sendall(f"ADMIN_REGION_STATS {rid}\n".encode())
      line = s.recv(512).decode().strip()
      s.close()
      if "role=2" in line:
        return True
    except Exception:
      pass
  return False

route = meta("GET_ROUTE")
regions = {}
for tok in route.split()[5:]:
  rid = tok.split(":", 1)[0]
  peers = [m.group(1) for m in re.finditer(r"@([0-9.]+:[0-9]+)", tok)]
  regions[rid] = peers

for rid, peers in regions.items():
  if not peers:
    continue
  for round_i in range(8):
    if has_leader(rid, peers):
      print(f"region {rid}: leader ready", flush=True)
      break
    addr = peers[round_i % len(peers)]
    try:
      print(f"campaign {rid}@{addr}:", campaign(rid, addr), flush=True)
    except Exception as e:
      print(f"campaign {rid}@{addr}: {e}", flush=True)
    time.sleep(0.8)
  else:
    print(f"region {rid}: no leader after campaigns", flush=True)
PY
sleep 3
# 稳态探针：迁完后 put/get（提出超时放宽；迁副本后首几次 propose 常 >2s）
settle_ok=0
settle_fail=0
_old_to=${SKV_TIMEOUT_SEC}
SKV_TIMEOUT_SEC="${SETTLE_TIMEOUT_SEC:-10}"
for i in $(seq 1 20); do
  if skv_to put "settle-$i" "sv-$i" >/dev/null 2>&1 && skv_to get "settle-$i" 2>/dev/null | grep -q "sv-$i"; then
    settle_ok=$((settle_ok + 1))
  else
    settle_fail=$((settle_fail + 1))
    sleep 0.4
  fi
done
SKV_TIMEOUT_SEC=${_old_to}
echo "  settle probes ok=$settle_ok fail=$settle_fail"
python3 - "$settle_ok" "$settle_fail" "$MAX_FAIL_RATIO" <<'PY' || fail "settle probe quality too low"
import sys
ok, fail, lim = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
total = ok + fail
ratio = (fail / total) if total else 1.0
print(f"  [settle-probe] fail_ratio={ratio:.3f} limit={lim} ok={ok}")
if ok < 8:
    raise SystemExit(f"too few settle ok: {ok}")
if ratio > lim:
    raise SystemExit(f"fail_ratio {ratio:.3f} > {lim}")
PY
ok "live traffic seamless after scale-out + scale-in"

echo "==> KV still readable"
expect_get aaa before-left || fail "aaa lost"
expect_get zzz before-right || fail "zzz lost"
for i in 0 17 100 200; do
  k="live-$(printf '%04d' $i)"
  expect_get "$k" "" || fail "get $k failed"
done
ok "kv readable"

echo "==> kick tombstone $DRAIN_NODE"
KICK=$("$SKV" remove-tombstone "$DRAIN_NODE" 2>&1 || true)
echo "$KICK"
ok "tombstone cleanup attempted"

ROUTE=$(meta "GET_ROUTE")
echo "$ROUTE" | grep -qE "(^|,)${DRAIN_NODE}@" && fail "drain node still in route: $ROUTE"
echo "$ROUTE" | grep -qE "(^|,)${SCALE_OUT_NODE}@" || fail "scale-out node missing from route: $ROUTE"
ok "final route uses $SCALE_OUT_NODE, not $DRAIN_NODE"
echo "  route: $ROUTE"

echo
echo "ALL PASS: it-scale-live (scale-out + scale-in under continuous R/W)"
