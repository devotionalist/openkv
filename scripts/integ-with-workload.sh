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

# =============================================================================
# 集成测试（全程持续用户读写）
# -----------------------------------------------------------------------------
# 1) 启集群  2) 后台 kv-chaos 模拟多用户读写  3) 跑 e2e/txn/watch
# 4) 注入轻量故障（杀 1 Store）并断言后台成功率  5) 停负载、汇总
#
#   make integ-workload
#   WORKERS=4 QPS=40 bash scripts/integ-with-workload.sh
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
GW="${GATEWAY_ADDR:-127.0.0.1:19000}"
GW_PORT="${GW##*:}"
LOG="$RUN/integ-workload.log"
WORKERS="${WORKERS:-2}"
QPS="${QPS:-20}"
WRITE_RATIO="${WRITE_RATIO:-0.5}"
mkdir -p "$RUN"

ensure_line_rpc() {
  local bin="$RUN/line-rpc"
  if [[ ! -x "$bin" ]]; then
    (cd "$ROOT/client/go" && go build -o "$bin" ./cmd/line-rpc) || return 1
  fi
  LINE_RPC="$bin"
}
meta_route_ok() {
  ensure_line_rpc || return 1
  local out
  out="$("$LINE_RPC" -kind meta -addrs "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502" -req GET_ROUTE 2>/dev/null || true)"
  [[ "$out" == OK\ ROUTE* || "$out" == *"redirect="* ]]
}

: >"$LOG"

ok_steps=0
fail_steps=0
log() { echo "[integ] $*" | tee -a "$LOG" >&2; }
pass() { log "PASS: $*"; ok_steps=$((ok_steps + 1)); }
fail() { log "FAIL: $*"; fail_steps=$((fail_steps + 1)); }

pid_of() { cat "$RUN/$1.pid" 2>/dev/null || true; }
alive() {
  local p
  p="$(pid_of "$1")"
  [[ -n "$p" ]] && kill -0 "$p" 2>/dev/null
}

wait_port() {
  local port="$1" sec="${2:-25}"
  python3 - "$port" "$sec" <<'PY'
import socket, sys, time
port, sec = int(sys.argv[1]), float(sys.argv[2])
deadline = time.time() + sec
while time.time() < deadline:
    try:
        s = socket.create_connection(("127.0.0.1", port), 0.2)
        s.close()
        sys.exit(0)
    except OSError:
        time.sleep(0.2)
sys.exit(1)
PY
}

cleanup_load() {
  if [[ -f "$RUN/chaos-bg.pid" ]]; then
    kill "$(cat "$RUN/chaos-bg.pid")" 2>/dev/null || true
    wait "$(cat "$RUN/chaos-bg.pid")" 2>/dev/null || true
    rm -f "$RUN/chaos-bg.pid"
  fi
}
trap cleanup_load EXIT

log "==> build"
test -x build/meta/meta_server || cmake --build build -j --target meta_server storage_node
(cd client/go && go build -o "$RUN/compute-gateway" ./cmd/compute-gateway \
  && go build -o "$RUN/kv-chaos" ./examples/chaos \
  && go build -o "$RUN/kv-example" ./examples/basic \
  && go build -o "$RUN/kv-e2e" ./examples/e2e \
  && go build -o "$RUN/kv-txn" ./examples/txn \
  && go build -o "$RUN/kv-watch" ./examples/watch)

log "==> stop old cluster, then start fresh"
# 必须先停进程再清 data（RocksDB/Raft 占用会导致 rm 失败）
python3 - <<'PY'
import os, pathlib, signal, time
RUN = pathlib.Path(".run")
for p in RUN.glob("*.pid"):
    try:
        pid = int(p.read_text().strip())
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    p.unlink(missing_ok=True)
time.sleep(1.0)
for p in RUN.glob("*.pid"):
    try:
        pid = int(p.read_text().strip())
        os.kill(pid, signal.SIGKILL)
    except Exception:
        pass
    p.unlink(missing_ok=True)
# 释放端口上残留监听
import subprocess
for port in (8500, 8501, 8502, 9100, 9101, 9102, 9103, 9104, 9105, 19000):
    try:
        out = subprocess.check_output(["lsof", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"], text=True)
        for pid in out.split():
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass
    except Exception:
        pass
time.sleep(0.5)
PY
if [[ "${INTEG_FRESH:-1}" == "1" ]]; then
  rm -rf "$ROOT/data" 2>/dev/null || true
  # 二次清理 LOCK / 残留
  find "$ROOT/data" -name LOCK -delete 2>/dev/null || true
  rm -rf "$ROOT/data" 2>/dev/null || true
  mkdir -p "$ROOT/data"
fi
python3 scripts/daemon_cluster.py
wait_port "$GW_PORT" 40
pass "cluster up on :$GW_PORT"

log "==> wait Meta Leader + Put/Get ready (up to 90s)"
ready=0
for _ in $(seq 1 90); do
  # Meta 多数派可读路由
  if meta_route_ok; then
    if "$RUN/kv-example" "$GW" >>"$LOG" 2>&1; then
      ready=1
      break
    fi
  fi
  sleep 1
done
if [[ "$ready" -eq 1 ]]; then
  pass "kv-example (readiness)"
else
  fail "cluster not ready (Meta/GW Put)"
  tail -30 "$RUN/meta-0.log" "$RUN/gateway.log" 2>/dev/null || true
fi

log "==> Stage A: functional clients (before sustained load)"
# 客户端内已有瞬时错误重试；脚本层再兜底几轮（启动竞态 / Leader 切换）
run_retry() {
  local name="$1"; shift
  local tries=5
  if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    tries="$1"
    shift
  fi
  local i
  for i in $(seq 1 "$tries"); do
    if "$@" >>"$LOG" 2>&1; then
      pass "$name"
      return 0
    fi
    sleep $((i < 3 ? 2 : 3))
  done
  fail "$name"
  return 1
}
run_retry kv-e2e "$RUN/kv-e2e" -addr "$GW" -op-timeout 8s -retries 5 || true
run_retry kv-txn "$RUN/kv-txn" "$GW" || true
run_retry kv-watch "$RUN/kv-watch" "$GW" || true

log "==> Stage B: start continuous user R/W (workers=$WORKERS qps=$QPS write_ratio=$WRITE_RATIO)"
"$RUN/kv-chaos" -addr "$GW" -duration 0 -workers "$WORKERS" -qps "$QPS" \
  -write-ratio "$WRITE_RATIO" -report 2s -prefix integ -op-timeout 3s \
  >"$RUN/chaos-bg.log" 2>&1 &
echo $! >"$RUN/chaos-bg.pid"
sleep 4
if ! kill -0 "$(cat "$RUN/chaos-bg.pid")" 2>/dev/null; then
  fail "background chaos died"
  tail -40 "$RUN/chaos-bg.log" || true
  exit 1
fi
pass "background workload running"

# 窗口成功率：取 chaos 日志里最近一次 success=xx.x%
window_success() {
  python3 - <<'PY'
import re, pathlib
p = pathlib.Path(".run/chaos-bg.log")
if not p.exists():
    print(0); raise SystemExit
vals = []
for line in p.read_text(errors="ignore").splitlines():
    m = re.search(r"success=([0-9.]+)%", line)
    if m:
        vals.append(float(m.group(1)))
print(vals[-1] if vals else 0)
PY
}

sleep 4
ws="$(window_success)"
log "steady window success≈${ws}%"
if python3 -c "import sys; sys.exit(0 if float('${ws}' or 0) >= 85 else 1)"; then
  pass "steady load >=85%"
else
  fail "steady load <85% (${ws}%)"
fi

log "==> Stage C: kill storage-1 under continuous load"
if alive storage-1; then
  kill "$(pid_of storage-1)" 2>/dev/null || true
  sleep 0.2
  kill -9 "$(pid_of storage-1)" 2>/dev/null || true
  rm -f "$RUN/storage-1.pid"
  sleep 8
  ws="$(window_success)"
  log "during store-1 down window≈${ws}%"
  # 杀节点瞬间窗口可能全失败；用最近若干窗口均值
  avg_ws="$(python3 - <<'PY'
import re
from pathlib import Path
vals=[]
for line in Path(".run/chaos-bg.log").read_text(errors="ignore").splitlines()[-30:]:
    m=re.search(r"success=([0-9.]+)%", line)
    if m: vals.append(float(m.group(1)))
print(f"{sum(vals)/len(vals):.1f}" if vals else "0")
PY
)"
  log "during store-1 down recent-avg≈${avg_ws}% (last window ${ws}%)"
  if python3 -c "import sys; sys.exit(0 if float('${avg_ws}' or 0) >= 40 else 1)"; then
    pass "majority serves under load (avg>=40%)"
  else
    fail "under load during store-1 down (avg ${avg_ws}%)"
  fi
  # 拉回
  find "$ROOT/data/storage-1" -name LOCK -delete 2>/dev/null || true
  ./build/storage/storage_node conf/storage-1.conf >"$RUN/storage-1.log" 2>&1 &
  echo $! >"$RUN/storage-1.pid"
  disown || true
  wait_port 9101 30
  sleep 5
  ws="$(window_success)"
  log "after store-1 restart window≈${ws}%"
  if python3 -c "import sys; sys.exit(0 if float('${ws}' or 0) >= 85 else 1)"; then
    pass "recovered under continuous load"
  else
    fail "recovery window ${ws}%"
  fi
else
  log "warn: storage-1 not running; skip fault inject"
fi

log "==> stop background workload"
cleanup_load
trap - EXIT

overall="$(python3 - <<'PY'
import re
from pathlib import Path
text = Path(".run/chaos-bg.log").read_text(errors="ignore")
vals = []
final = None
for line in text.splitlines():
    if "overall_success=" in line:
        m = re.search(r"overall_success=([0-9.]+)%", line)
        if m:
            final = float(m.group(1))
    m = re.search(r"success=([0-9.]+)%", line)
    if m:
        vals.append(float(m.group(1)))
if final is not None:
    print(f"{final:.2f}")
elif vals:
    print(f"{sum(vals)/len(vals):.2f}")
else:
    print("0")
PY
)"
log "background overall_success≈${overall}%"
# 含杀节点窗口，门槛略低于稳态
if python3 -c "import sys; sys.exit(0 if float('${overall}' or 0) >= 50 else 1)"; then
  pass "overall continuous R/W >=50%"
else
  fail "overall continuous R/W ${overall}%"
fi

log "=========================================="
log "RESULT: passed=$ok_steps failed=$fail_steps"
log "detail: $LOG | chaos: $RUN/chaos-bg.log"
[[ "$fail_steps" -eq 0 ]] || exit 1
echo "INTEG WITH WORKLOAD PASSED"
