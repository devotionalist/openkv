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

# 节点间网络抖动 IT（FAULT_DRILL 网络抖动分类）
#   NJ1 / F10-delay：Store 出站 AE 延迟
#   NJ2 / F10-drop：Store 出站 AE 丢包
#   NJ3 / F10-mix：延迟+丢包（与 fault-drill step7 同形）
#   NJ4 / F17：Meta 出站 Raft 延迟（管控面多数派仍服务）
#
# 用法：集群已起时
#   bash scripts/it-net-jitter.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
GW="${GATEWAY_ADDR:-127.0.0.1:19000,127.0.0.1:19002}"
SKV="${SKV:-./build/tools/skv}"
STOR="${STOR_BIN:-./build/storage/storage_node}"
META="${META_BIN:-./build/meta/meta_server}"
mkdir -p "$RUN"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }
log() { echo "==> $*"; }

"$SKV" status >/dev/null || fail "cluster down; run: ./build/tools/skv start"
[[ -x "$STOR" ]] || fail "missing $STOR"
[[ -x "$META" ]] || fail "missing $META"

(cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos)

# stdout: overall write read
smoke_rw() {
  local dur="${1:-5s}"
  local out
  out="$("$RUN/kv-chaos" -addr "$GW" -duration "$dur" -qps 40 -workers 4 -report 10s 2>&1)"
  python3 - "$out" <<'PY'
import re, sys
s = sys.argv[1]
def grab_last(k):
    ms = re.findall(rf"{k}=([0-9.]+)%", s)
    return ms[-1] if ms else "0"
print(grab_last("overall_success"), grab_last("write_success"), grab_last("read_success"))
PY
}

smoke_pct() {
  local o w r
  read -r o w r <<< "$(smoke_rw "${1:-5s}")"
  printf '%s\n' "${o:-0}"
}

alive() {
  local name="$1"
  [[ -f "$RUN/$name.pid" ]] && kill -0 "$(cat "$RUN/$name.pid")" 2>/dev/null
}

kill_named() {
  local name="$1"
  if alive "$name"; then
    kill "$(cat "$RUN/$name.pid")" 2>/dev/null || true
    sleep 1
    if alive "$name"; then kill -9 "$(cat "$RUN/$name.pid")" 2>/dev/null || true; fi
  fi
  rm -f "$RUN/$name.pid"
}

wait_port() {
  local port="$1" sec="${2:-30}"
  for _ in $(seq 1 "$sec"); do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

# 按 conf 名拉起单个 storage（带故障 env）
start_storage_fault() {
  local idx="$1"
  shift
  local conf="conf/storage.conf"
  local port=9100
  if [[ "$idx" -gt 0 ]]; then
    conf="conf/storage-$idx.conf"
    port=$((9100 + idx))
  fi
  [[ -f "$conf" ]] || fail "missing $conf"
  env "$@" "$STOR" "$conf" >"$RUN/storage-$idx.log" 2>&1 &
  echo $! >"$RUN/storage-$idx.pid"
  disown || true
  wait_port "$port" 30 || fail "storage-$idx port $port not up"
}

start_meta_fault() {
  local idx="$1"
  shift
  local conf="conf/bootstrap.conf"
  local port=8500
  if [[ "$idx" -eq 1 ]]; then conf="conf/meta-1.conf"; port=8501; fi
  if [[ "$idx" -eq 2 ]]; then conf="conf/meta-2.conf"; port=8502; fi
  [[ -f "$conf" ]] || fail "missing $conf"
  env "$@" "$META" "$conf" >"$RUN/meta-$idx.log" 2>&1 &
  echo $! >"$RUN/meta-$idx.pid"
  disown || true
  wait_port "$port" 30 || fail "meta-$idx port $port not up"
}

check_ge() {
  python3 -c "import sys; sys.exit(0 if float('${1:-0}')>=float('$2') else 1)" \
    || fail "$3 (got $1 need >=$2)"
}

check_rw_ge() {
  local w="$1" r="$2" w_thr="$3" r_thr="$4" msg="$5"
  python3 -c "import sys; sys.exit(0 if float('$w' or 0)>=float('$w_thr') and float('$r' or 0)>=float('$r_thr') else 1)" \
    || fail "$msg (write=${w}% need≥${w_thr}; read=${r}% need≥${r_thr})"
  ok "$msg (write=${w}% read=${r}%)"
}

# 解析 Region Leader → storage-N 下标（出站 AE 从 Leader 发出，注入才打到复制路径）
leader_idx() {
  local route name
  route="$("$SKV" route 2>/dev/null || true)"
  name="$(python3 - "$route" <<'PY'
import re, sys
s = sys.argv[1] if len(sys.argv) > 1 else ""
# ... storage-1@127.0.0.1:9101*L ...
m = re.search(r"(storage-(\d+))@[^\s,]*\*L", s)
print(m.group(2) if m else "0")
PY
)"
  echo "${name:-0}"
}

# 失败也尽量清掉带 env 重启的节点，避免残留注入拖垮后续跑测。
CLEAN_STORE_IDX=""
CLEAN_META_IDX=""
cleanup_inject() {
  if [[ -n "$CLEAN_STORE_IDX" ]]; then
    kill_named "storage-$CLEAN_STORE_IDX" || true
    start_storage_fault "$CLEAN_STORE_IDX" || true
    CLEAN_STORE_IDX=""
  fi
  if [[ -n "$CLEAN_META_IDX" ]]; then
    kill_named "meta-$CLEAN_META_IDX" || true
    start_meta_fault "$CLEAN_META_IDX" || true
    CLEAN_META_IDX=""
  fi
}
trap cleanup_inject EXIT

log "baseline"
BASE_W=0 BASE_R=0
for _ in $(seq 1 8); do
  read -r _ BASE_W BASE_R <<< "$(smoke_rw 4s)"
  if python3 -c "import sys; sys.exit(0 if float('${BASE_W:-0}')>=80 and float('${BASE_R:-0}')>=80 else 1)"; then break; fi
  sleep 1
done
check_rw_ge "$BASE_W" "$BASE_R" 80 80 "baseline write+read"

# tag drop delay write_thr read_thr
run_store_jitter() {
  local tag="$1" drop="$2" delay="$3" w_thr="$4" r_thr="$5"
  local LIDX LNAME o w r ro rw rr
  LIDX="$(leader_idx)"
  LNAME="storage-$LIDX"
  log "$tag: $LNAME DROP=${drop}% DELAY=${delay}ms (need write≥${w_thr}% read≥${r_thr}%)"
  kill_named "$LNAME"
  CLEAN_STORE_IDX="$LIDX"
  start_storage_fault "$LIDX" SKV_FAULT_DROP_AE_PCT="$drop" SKV_FAULT_DELAY_AE_MS="$delay"
  sleep 3
  w=0 r=0
  for _ in $(seq 1 3); do
    read -r o w r <<< "$(smoke_rw 8s)"
    if python3 -c "import sys; sys.exit(0 if float('${w:-0}')>=float('$w_thr') and float('${r:-0}')>=float('$r_thr') else 1)"; then
      break
    fi
    sleep 2
  done
  check_rw_ge "$w" "$r" "$w_thr" "$r_thr" "$tag under jitter"
  kill_named "$LNAME"
  start_storage_fault "$LIDX"
  CLEAN_STORE_IDX=""
  sleep 5
  rw=0 rr=0
  for _ in $(seq 1 6); do
    read -r ro rw rr <<< "$(smoke_rw 5s)"
    if python3 -c "import sys; sys.exit(0 if float('${rw:-0}')>=85 and float('${rr:-0}')>=85 else 1)"; then
      break
    fi
    sleep 2
  done
  LIDX="$(leader_idx)"
  LNAME="storage-$LIDX"
  check_rw_ge "$rw" "$rr" 85 85 "$tag after clear (leader now $LNAME)"
}

# ----- NJ1：AE 延迟（写更敏感；读门槛略高） -----
run_store_jitter "NJ1/F10-delay" 0 40 30 50

# ----- NJ2：AE 丢包 -----
run_store_jitter "NJ2/F10-drop" 30 0 20 40

# ----- NJ3：延迟+丢包 -----
run_store_jitter "NJ3/F10-mix" 30 20 20 40

# ----- NJ4：Meta 出站 Raft 延迟（数据面写+读；TSO 可能短暂变慢） -----
# 写门槛略低于手册「多数可用」：杀/启 Meta + 出站延迟会挤占 TSO 提交窗口。
log "NJ4 / F17: meta-1 Raft delay 20ms (data plane write+read)"
if alive meta-1; then
  kill_named meta-1
  CLEAN_META_IDX=1
  start_meta_fault 1 SKV_FAULT_DELAY_RAFT_MS=20
  sleep 5
  META_W=0 META_R=0
  for _ in $(seq 1 4); do
    read -r _ META_W META_R <<< "$(smoke_rw 6s)"
    if python3 -c "import sys; sys.exit(0 if float('${META_W:-0}')>=55 and float('${META_R:-0}')>=80 else 1)"; then
      break
    fi
    sleep 2
  done
  check_rw_ge "$META_W" "$META_R" 55 80 "NJ4 meta delay data plane"
  kill_named meta-1
  start_meta_fault 1
  CLEAN_META_IDX=""
  sleep 3
  REC4_W=0 REC4_R=0
  for _ in $(seq 1 4); do
    read -r _ REC4_W REC4_R <<< "$(smoke_rw 5s)"
    if python3 -c "import sys; sys.exit(0 if float('${REC4_W:-0}')>=85 and float('${REC4_R:-0}')>=85 else 1)"; then
      break
    fi
    sleep 2
  done
  check_rw_ge "$REC4_W" "$REC4_R" 85 85 "NJ4 after clear meta delay"
else
  log "warn: skip NJ4 (no meta-1)"
fi

trap - EXIT
echo "ALL it-net-jitter PASS"
