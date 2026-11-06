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

# I38: 用户流量持续期间完成 Store 缩容（Draining → 迁副本 → Tombstone）。
# 模拟：后台 put/get 不停；对承载副本的 Store 做 scale-in；期间业务可重试失败，结束后数据可读且路由无该节点。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SKV="${SKV:-./build/tools/skv}"
WEB="${WEB:-http://127.0.0.1:19100}"
META_ADDRS="${META_ADDRS:-127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502}"
DRAIN_NODE="${DRAIN_NODE:-storage-0}"
SPARE_NODE="${SPARE_NODE:-storage-3}"
# 迁副本/转主窗口 put 会阻塞或失败；默认允许 50% 失败，且至少跑满若干秒
MAX_FAIL_RATIO="${MAX_FAIL_RATIO:-0.50}"
MIN_TRAFFIC_SEC="${MIN_TRAFFIC_SEC:-8}"
TRAFFIC_FLAG=/tmp/skv_scalein_traffic_run
TRAFFIC_STATS=/tmp/skv_scalein_traffic_stats
TRAFFIC_LOG=/tmp/skv_scalein_traffic.log
SKV_TIMEOUT_SEC="${SKV_TIMEOUT_SEC:-2}"

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
  # web 仅作集群就绪参考；缩容 IT 本身走 skv/meta，不必强依赖 :19100
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

join_spare() {
  local nid="$1"
  echo "==> join spare $nid (RF 恒定 Replace 需要空闲 Active)"
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
      # 心跳过期：重启 Store 重新 REGISTER/HEARTBEAT，再 join
      if grep -qE 'heartbeat|stale|unreachable' /tmp/skv_join_out.txt 2>/dev/null; then
        echo "  join blocked ($(head -1 /tmp/skv_join_out.txt)); restart $nid (round $round)"
        "$SKV" restart "$nid" >/dev/null 2>&1 || true
        sleep 3
        break
      fi
      sleep 0.5
    done
  done
  fail "$nid not Active: $(meta "STORE_STATUS $nid"); last join: $(cat /tmp/skv_join_out.txt 2>/dev/null)"
}

skv_to() {
  # 避免迁主窗口无限卡住吞吐统计
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$SKV_TIMEOUT_SEC" "$SKV" "$@"
  elif command -v timeout >/dev/null 2>&1; then
    timeout "$SKV_TIMEOUT_SEC" "$SKV" "$@"
  else
    "$SKV" "$@"
  fi
}

start_traffic() {
  rm -f "$TRAFFIC_FLAG" "$TRAFFIC_STATS" "$TRAFFIC_LOG"
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
      if skv_to put "$k" "$v" >>"$TRAFFIC_LOG" 2>&1; then
        got=""
        for _ in 1 2 3; do
          got=$(skv_to get "$k" 2>/dev/null | tail -1 || true)
          if [[ "$got" == *"$v"* ]]; then break; fi
          sleep 0.05
        done
        if [[ "$got" == *"$v"* ]]; then
          ok_n=$((ok_n + 1))
        else
          fail_n=$((fail_n + 1))
          echo "GET miss $k want=$v got=$got" >>"$TRAFFIC_LOG"
        fi
      else
        fail_n=$((fail_n + 1))
      fi
      i=$((i + 1))
      # 略降频，减轻与 Place/ConfChange 抢 Leader
      sleep 0.08
    done
    echo "$ok_n $fail_n $i $t0 $(date +%s)" >"$TRAFFIC_STATS"
  ) &
  TRAFFIC_PID=$!
  ok "background traffic pid=$TRAFFIC_PID"
}

stop_traffic() {
  rm -f "$TRAFFIC_FLAG"
  if [[ -n "${TRAFFIC_PID:-}" ]]; then
    wait "$TRAFFIC_PID" 2>/dev/null || true
  fi
  sleep 0.3
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

# 若调度卡住：对仍挂着 drain 节点的 Region 显式 skv place 到 remaining+spare
nudge_drain_place() {
  local drain="$1"
  local spare="$2"
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

line = meta("GET_ROUTE")
# OK ROUTE ver n n tok...（peer 含 host:port，不能整体 split(':')）
for tok in line.split()[5:]:
  rid = tok.split(":", 1)[0]
  ids = re.findall(r"([A-Za-z0-9_-]+)@", tok)
  if drain not in ids:
    continue
  desired = [x for x in ids if x != drain]
  if spare not in desired:
    desired.append(spare)
  desired = desired[:3]
  if len(desired) < 3:
    continue
  cmd = [skv, "place", rid, ",".join(desired)]
  print("nudge:", " ".join(cmd), flush=True)
  subprocess.run(cmd, check=False, timeout=90)
PY
}

wait_drained() {
  local nid="$1"
  local deadline=$((SECONDS + 240))
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
    # 同一 regions 计数卡住 >25s → 显式 place 推进
    if (( stuck > 0 && stuck % 25 == 0 )); then
      echo "  drain stuck; nudge place → $SPARE_NODE"
      nudge_drain_place "$nid" "$SPARE_NODE"
    fi
    sleep 1
  done
  fail "scale-in timeout: $(meta "STORE_STATUS $nid")"
}

echo "==> ensure cluster + web"
"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start --fresh"
wait_web

echo "==> seed + split (制造 ≥2 Region，缩容有东西可迁)"
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

join_spare "$SPARE_NODE"

# 确认 DRAIN_NODE 当前确实挂了副本
REG_BEFORE=$(store_regions "$DRAIN_NODE")
[[ "$REG_BEFORE" != "0" ]] || fail "$DRAIN_NODE has 0 regions; nothing to drain"
ok "$DRAIN_NODE carries regions=$REG_BEFORE"

echo "==> start live traffic (simulating users)"
start_traffic
# 先攒一段稳态流量再缩容
sleep "$MIN_TRAFFIC_SEC"

echo "==> scale-in $DRAIN_NODE (STORE_DELETE → Draining)"
SI_OUT=$("$SKV" scale-in "$DRAIN_NODE" 2>&1 || true)
echo "$SI_OUT"
echo "$SI_OUT" | grep -q '^OK' || fail "scale-in failed: $SI_OUT"
# 等状态落到 Draining（Raft apply 有短暂延迟）
for _ in $(seq 1 20); do
  ST=$(store_state "$DRAIN_NODE" || true)
  [[ "$ST" == "2" ]] && break
  sleep 0.25
done
ST=$(store_state "$DRAIN_NODE")
[[ "$ST" == "2" ]] || fail "expect Draining(2) got $ST"

echo "==> wait drain while traffic runs"
wait_drained "$DRAIN_NODE"
assert_not_in_route "$DRAIN_NODE"

echo "==> stop traffic and check failure ratio"
stop_traffic
if [[ ! -f "$TRAFFIC_STATS" ]]; then
  fail "no traffic stats"
fi
read -r TOK TFAIL TTOTAL T0 T1 <"$TRAFFIC_STATS" || fail "bad traffic stats"
echo "  traffic ok=$TOK fail=$TFAIL attempts=$TTOTAL wall=$((T1 - T0))s"
python3 - "$TOK" "$TFAIL" "$MAX_FAIL_RATIO" "$T0" "$T1" "$MIN_TRAFFIC_SEC" <<'PY' || fail "traffic quality too low"
import sys
ok, fail, lim = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
t0, t1, min_sec = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
total = ok + fail
wall = max(0, t1 - t0)
ratio_now = (fail / total) if total else 1.0
print(f"  fail_ratio={ratio_now:.3f} limit={lim} wall={wall}s ok={ok}")
if wall < min_sec:
    raise SystemExit(f"traffic wall {wall}s < {min_sec}s")
if ok < 5:
    raise SystemExit(f"too few successful ops: {ok}")
if total == 0:
    raise SystemExit("no traffic ops")
ratio = fail / total
if ratio > lim:
    raise SystemExit(f"fail_ratio {ratio:.3f} > {lim}")
PY
ok "live traffic survived scale-in"

echo "==> KV still readable (seed + live keys)"
expect_get() {
  local key="$1" needle="$2"
  local out=""
  for _ in $(seq 1 30); do
    out=$(skv_to get "$key" 2>/dev/null | tail -1 || true)
    if [[ -n "$needle" && "$out" == *"$needle"* ]]; then return 0; fi
    # live key：只要读通（含 found=true）；排除 not_leader / ERR / 超时空输出
    if [[ -z "$needle" && -n "$out" && "$out" != *not_leader* && "$out" != ERR* && "$out" == *"found="* ]]; then
      return 0
    fi
    sleep 0.4
  done
  echo "last get $key → $out" >&2
  return 1
}
expect_get aaa before-left || fail "aaa lost"
expect_get zzz before-right || fail "zzz lost"
for i in 0 17 100 200; do
  k="live-$(printf '%04d' $i)"
  expect_get "$k" "" || fail "get $k failed after scale-in"
done
ok "kv readable"

echo "==> kick tombstone $DRAIN_NODE"
KICK=$("$SKV" remove-tombstone "$DRAIN_NODE" 2>&1 || true)
echo "$KICK"
ok "tombstone cleanup attempted"

# 路由仍无 DRAIN_NODE；spare 应出现在至少一个 Region（Replace 目标）
ROUTE=$(meta "GET_ROUTE")
echo "$ROUTE" | grep -qE "(^|,)${DRAIN_NODE}@" && fail "drain node still in route after kick"
if ! echo "$ROUTE" | grep -qE "(^|,)${SPARE_NODE}@"; then
  fail "spare $SPARE_NODE not used as replace target: $ROUTE"
fi
ok "route uses $SPARE_NODE, not $DRAIN_NODE"

echo
echo "ALL PASS: it-scale-in-live"
