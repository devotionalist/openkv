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
# 故障演练 — 验证集群可用性（分区降级 + 恢复）
# -----------------------------------------------------------------------------
# 当前拓扑：每分片 3 副本 Raft（多数派 2）。
#   - 杀掉 1 个 Storage → 仍应接近全可用（2/3 多数派）
#   - 杀掉 Gateway     → 数据面全不可用，重启后恢复
#   - 杀掉 Meta        → 数据面应仍可用（存算/管控分离）
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
GW="${GATEWAY_ADDR:-127.0.0.1:19000}"
GW_PORT="${GW##*:}"
LOG="$RUN/fault-drill.log"
mkdir -p "$RUN"
: >"$LOG"

ok_steps=0
fail_steps=0

# 打到 stderr，避免被 $(...) 捕获
log() { echo "[drill] $*" | tee -a "$LOG" >&2; }
pass() { log "PASS: $*"; ok_steps=$((ok_steps + 1)); }
fail() { log "FAIL: $*"; fail_steps=$((fail_steps + 1)); }

ensure_line_rpc() {
  local bin="$RUN/line-rpc"
  if [[ ! -x "$bin" ]]; then
    (cd "$ROOT/client/go" && go build -o "$bin" ./cmd/line-rpc) || return 1
  fi
  LINE_RPC="$bin"
}

meta_rpc() {
  ensure_line_rpc || return 1
  "$LINE_RPC" -kind meta -addrs "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502" -req "$1" 2>/dev/null
}

meta_route_ok() {
  local out
  out="$(meta_rpc GET_ROUTE || true)"
  [[ "$out" == OK\ ROUTE* || "$out" == *"redirect="* ]]
}

pid_of() { cat "$RUN/$1.pid" 2>/dev/null || true; }
alive() {
  local p
  p="$(pid_of "$1")"
  [[ -n "$p" ]] && kill -0 "$p" 2>/dev/null
}

wait_port() {
  local port="$1" sec="${2:-20}"
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

ensure_cluster() {
  if alive gateway && wait_port "$GW_PORT" 2; then
    log "cluster already up"
    return
  fi
  log "starting cluster..."
  test -x build/meta/meta_server || cmake --build build -j --target meta_server storage_node
  (cd client/go && go build -o "$RUN/compute-gateway" ./cmd/compute-gateway \
    && go build -o "$RUN/kv-example" ./examples/basic)
  # 高 term / 脏 WAL 会导致选主困难、成功率卡在 ~50%；默认 fresh 演练
  if [[ "${FAULT_DRILL_FRESH:-1}" == "1" ]]; then
    log "FAULT_DRILL_FRESH=1: wipe data/ for clean raft state"
    rm -rf "$ROOT/data"
    mkdir -p "$ROOT/data"
  fi
  python3 scripts/daemon_cluster.py
  wait_port "$GW_PORT" 30
}

build_tools() {
  (cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos \
    && go build -o "$RUN/kv-example" ./examples/basic)
}

# 短时冒烟，打印 overall_success 数值
smoke_pct() {
  local qps="${1:-40}"
  local out
  out="$("$RUN/kv-chaos" -addr "$GW" -duration 3s -qps "$qps" -report 10s 2>&1 | tee -a "$LOG")"
  echo "$out" | awk -F= '/overall_success/{gsub(/%/,"",$2); print $2+0; exit}'
}

# dur=0 → 跑到 stop_load（模拟用户一直读写）
start_load() {
  local name="$1" dur="$2" qps="$3"
  local workers="${4:-${CHAOS_WORKERS:-2}}"
  local wr="${CHAOS_WRITE_RATIO:-0.5}"
  local dur_flag="$dur"
  [[ "$dur" == "0" || "$dur" == "0s" ]] && dur_flag=0
  "$RUN/kv-chaos" -addr "$GW" -duration "$dur_flag" -qps "$qps" \
    -workers "$workers" -write-ratio "$wr" -report 2s -prefix "drill-$name" \
    >"$RUN/chaos-$name.log" 2>&1 &
  echo $! >"$RUN/chaos-$name.pid"
}

stop_load() {
  local name="$1"
  if [[ -f "$RUN/chaos-$name.pid" ]]; then
    local p
    p="$(cat "$RUN/chaos-$name.pid")"
    kill "$p" 2>/dev/null || true
    wait "$p" 2>/dev/null || true
    rm -f "$RUN/chaos-$name.pid"
  fi
}

chaos_overall() {
  local name="$1"
  local f="$RUN/chaos-$name.log"
  awk -F= '/overall_success/{gsub(/%/,"",$2); print $2+0}' "$f" 2>/dev/null | tail -1 || true
}

kill_component() {
  local name="$1"
  local p
  p="$(pid_of "$name")"
  if [[ -z "$p" ]]; then
    log "warn: $name pid missing"
    return
  fi
  log "KILL $name pid=$p"
  kill "$p" 2>/dev/null || true
  sleep 0.3
  kill -9 "$p" 2>/dev/null || true
  rm -f "$RUN/$name.pid"
}

restart_storage() {
  local idx="$1" conf port
  case "$idx" in
    0) conf=conf/storage.conf; port=9100 ;;
    1) conf=conf/storage-1.conf; port=9101 ;;
    2) conf=conf/storage-2.conf; port=9102 ;;
    *) log "bad idx $idx"; return 1 ;;
  esac
  log "RESTART storage-$idx"
  # kill -9 后 RocksDB LOCK 可能残留
  find "$ROOT/data/storage-$idx" -name LOCK -delete 2>/dev/null || true
  # 端口仍被占则强杀
  for pid in $(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true); do
    kill -9 "$pid" 2>/dev/null || true
  done
  sleep 0.3
  ./build/storage/storage_node "$conf" >"$RUN/storage-$idx.log" 2>&1 &
  echo $! >"$RUN/storage-$idx.pid"
  disown || true
  wait_port "$port" 30
}

restart_gateway() {
  log "RESTART gateway"
  for pid in $(lsof -t -iTCP:"$GW_PORT" -sTCP:LISTEN 2>/dev/null || true); do
    kill -9 "$pid" 2>/dev/null || true
  done
  sleep 0.3
  # 与 daemon_cluster 一致：6 Store
  "$RUN/compute-gateway" -listen "0.0.0.0:${GW_PORT}" \
    -storages "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105" \
    -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
    >"$RUN/gateway.log" 2>&1 &
  echo $! >"$RUN/gateway.pid"
  disown || true
  wait_port "$GW_PORT" 25
}

restart_meta() {
  log "RESTART meta quorum"
  for i in 0 1 2; do
    local conf="conf/bootstrap.conf"
    [[ "$i" -eq 1 ]] && conf="conf/meta-1.conf"
    [[ "$i" -eq 2 ]] && conf="conf/meta-2.conf"
    ./build/meta/meta_server "$conf" >"$RUN/meta-$i.log" 2>&1 &
    echo $! >"$RUN/meta-$i.pid"
    disown || true
  done
  sleep 2
}

restart_meta_one() {
  local idx="$1"
  local conf="conf/bootstrap.conf"
  [[ "$idx" -eq 1 ]] && conf="conf/meta-1.conf"
  [[ "$idx" -eq 2 ]] && conf="conf/meta-2.conf"
  log "RESTART meta-$idx"
  ./build/meta/meta_server "$conf" >"$RUN/meta-$idx.log" 2>&1 &
  echo $! >"$RUN/meta-$idx.pid"
  disown || true
  sleep 1
}

check_ge() {
  local val="$1" thr="$2" msg="$3"
  if python3 -c "import sys; sys.exit(0 if float('$val' or 0) >= float('$thr') else 1)"; then
    pass "$msg (${val}%)"
  else
    fail "$msg (${val}%)"
  fi
}

# 破坏性步骤后：轮询冒烟直到达标或超时（选主 / Meta 换主需要时间）
# 注意：只把百分比打到 stdout，日志走 log（tee），避免 $(wait_smoke_ge) 捕获脏数据
wait_smoke_ge() {
  local thr="$1" sec="${2:-45}" label="${3:-health}"
  local deadline pct=0
  deadline=$((SECONDS + sec))
  while (( SECONDS < deadline )); do
    pct="$(smoke_pct 30 || echo 0)"
    # 只保留数字
    pct="$(printf '%s' "$pct" | tr -cd '0-9.')"
    [[ -z "$pct" ]] && pct=0
    log "wait $label: success=${pct}% (need >=${thr}%)"
    if python3 -c "import sys; sys.exit(0 if float('$pct' or 0) >= float('$thr') else 1)"; then
      printf '%s\n' "$pct"
      return 0
    fi
    sleep 2
  done
  printf '%s\n' "$pct"
  return 1
}

check_between() {
  local val="$1" lo="$2" hi="$3" msg="$4"
  if python3 -c "import sys; v=float('$val' or 0); sys.exit(0 if float('$lo') <= v <= float('$hi') else 1)"; then
    pass "$msg (${val}%)"
  else
    fail "$msg (${val}%)"
  fi
}

# ---- 演练步骤 --------------------------------------------------------------
ensure_cluster
build_tools

cleanup_bg() { stop_load bg 2>/dev/null || true; }
trap cleanup_bg EXIT

log "========== 0) 启动全程持续用户读写（后台） =========="
# duration=0：直到 stop；故障注入期间业务流量不中断
start_load bg 0 "${CHAOS_QPS:-40}" "${CHAOS_WORKERS:-4}"
sleep 3
if [[ -f "$RUN/chaos-bg.pid" ]] && kill -0 "$(cat "$RUN/chaos-bg.pid")" 2>/dev/null; then
  pass "background continuous R/W started (qps=${CHAOS_QPS:-40} workers=${CHAOS_WORKERS:-4})"
else
  fail "background chaos failed to start"
  tail -30 "$RUN/chaos-bg.log" 2>/dev/null || true
fi

log "========== 1) 基线可用性 =========="
# daemon 双 fork 后选主需要时间；等到真正可用再开测
base="$(wait_smoke_ge 95 90 baseline || true)"
log "baseline success=${base}%"
check_ge "$base" 95 "baseline >= 95%"

log "========== 1b) F2 杀 Region Leader + seed Get（I14） =========="
# 解析 Region 0（或首 Region）Leader node_id；杀进程 → 新 *L + f2key 可读
F2_LEADER=""
if command -v curl >/dev/null && curl -sf -m 2 "http://127.0.0.1:19100/api/route" -o "$RUN/f2-route.json" 2>/dev/null; then
  F2_LEADER="$(python3 - <<'PY'
import json
route=json.load(open(".run/f2-route.json"))
regs=sorted(route.get("regions",[]), key=lambda x:int(x.get("id",0)))
if not regs:
    raise SystemExit(0)
r=regs[0]
for p in r.get("replicas",[]):
    if p.get("is_leader"):
        print(p["node_id"]); break
PY
)"
fi
if [[ -z "$F2_LEADER" ]] && [[ -x ./build/tools/skv ]]; then
  F2_LEADER="$(./build/tools/skv route 2>/dev/null | python3 - <<'PY' || true
import sys,re
text=sys.stdin.read()
# 找首个 *L：storage-N@...*L
m=re.search(r"(storage-\d+)@[^*\s]*\*L", text)
print(m.group(1) if m else "")
PY
)"
fi
if [[ -n "$F2_LEADER" ]]; then
  log "F2 leader=$F2_LEADER seed f2key"
  if [[ -x ./build/tools/skv ]]; then
    ./build/tools/skv put f2key f2v >/dev/null 2>&1 || true
  fi
  OLD_L="$F2_LEADER"
  kill_component "$F2_LEADER"
  # 等新 Leader（≤15s）
  NEW_L=""
  for _ in $(seq 1 30); do
    sleep 0.5
    if curl -sf -m 2 "http://127.0.0.1:19100/api/route" -o "$RUN/f2-route2.json" 2>/dev/null; then
      NEW_L="$(python3 - <<'PY'
import json
route=json.load(open(".run/f2-route2.json"))
regs=sorted(route.get("regions",[]), key=lambda x:int(x.get("id",0)))
if not regs: raise SystemExit(0)
r=regs[0]
for p in r.get("replicas",[]):
    if p.get("is_leader"):
        print(p["node_id"]); break
PY
)"
      if [[ -n "$NEW_L" && "$NEW_L" != "$OLD_L" ]]; then
        break
      fi
    fi
  done
  if [[ -n "$NEW_L" && "$NEW_L" != "$OLD_L" ]]; then
    pass "F2 new leader $NEW_L (was $OLD_L)"
  else
    # 旧 Leader 可能仍标 *L（心跳滞后）；只要 Get 成功也算部分通过
    log "warn: F2 leader label not flipped yet (NEW_L=$NEW_L); check Get"
  fi
  if [[ -x ./build/tools/skv ]] && ./build/tools/skv get f2key 2>&1 | grep -q f2v; then
    pass "F2 seed get f2key=f2v after leader kill"
  else
    fail "F2 seed get f2key after leader kill"
  fi
  # 重启旧 Leader
  case "$OLD_L" in
    storage-0) restart_storage 0 ;;
    storage-1) restart_storage 1 ;;
    storage-2) restart_storage 2 ;;
    storage-*)
      idx="${OLD_L#storage-}"
      log "RESTART $OLD_L (idx=$idx)"
      find "$ROOT/data/$OLD_L" -name LOCK -delete 2>/dev/null || true
      conf="conf/storage-${idx}.conf"
      [[ "$idx" == "0" ]] && conf="conf/storage.conf"
      port=$((9100 + idx))
      ./build/storage/storage_node "$conf" >"$RUN/$OLD_L.log" 2>&1 &
      echo $! >"$RUN/$OLD_L.pid"
      disown || true
      wait_port "$port" 30 || true
      ;;
  esac
  f2rec="$(wait_smoke_ge 85 40 f2-recovered || true)"
  check_ge "$f2rec" 85 "F2 after leader restart"
else
  log "warn: skip F2 (cannot resolve region leader; start web for /api/route)"
fi

log "========== 2) 杀掉 storage-1（3 副本剩 2，应仍可用） =========="
# 额外短负载窗口，便于单独统计本段成功率（后台 bg 仍在跑）
start_load degrade 20s 40
sleep 2
kill_component storage-1
sleep 12
stop_load degrade
overall="$(chaos_overall degrade)"
if [[ -z "${overall}" ]]; then
  overall="$(python3 - <<'PY'
import re
vals=[]
for line in open(".run/chaos-degrade.log", errors="ignore"):
    m=re.search(r"success=([0-9.]+)%", line)
    if m: vals.append(float(m.group(1)))
print(sum(vals)/len(vals) if vals else 0)
PY
)"
fi
log "during storage-1 down success≈${overall}%"
# 多数派仍在：允许短暂选主抖动，整体应较高
check_ge "$overall" 70 "storage-1 down: majority still serves"

log "========== 3) 恢复 storage-1（持久化装载 + AE/快照追赶） =========="
# 宕机期间写入；重启后 smoke 应恢复（raft WAL + AE/InstallSnapshot 追赶）
restart_storage 1
# 日志应出现 recovered / catch-up 相关痕迹
if grep -qE "recovered term=|installed snapshot|compacted to" "$RUN/storage-1.log" 2>/dev/null; then
  pass "storage-1 log shows raft recover/catch-up"
else
  log "warn: no explicit recover log line yet (cold start ok); continue"
fi
rec="$(wait_smoke_ge 90 40 storage-1-recovered || true)"
check_ge "$rec" 90 "storage-1 recovered"

log "========== 4) 杀掉 1 个 Meta（3 副本剩 2，管控面仍可用） =========="
kill_component meta-1
sleep 2
# 数据面不受影响
meta_down="$(smoke_pct 30 || echo 0)"
check_ge "$meta_down" 90 "one meta down: data plane OK"
# 管控面：向仍存活的 meta 拉路由
if meta_route_ok; then
  pass "meta majority still serves GET_ROUTE"
else
  fail "meta GET_ROUTE unavailable after killing meta-1"
fi
# 本步只验证「少数派 Meta 宕机仍可用」；meta-1 热重加入放到 step 9。
log "defer meta-1 restart until step 9"

log "========== 5) 杀掉 Gateway 再恢复 =========="
kill_component gateway
sleep 1
if "$RUN/kv-example" "$GW" >/dev/null 2>&1; then
  fail "gateway killed but client still works"
else
  pass "gateway down: client cannot connect"
fi
restart_gateway
gw_back="$(wait_smoke_ge 90 45 gateway-recovered || true)"
check_ge "$gw_back" 90 "gateway recovered"

log "========== 6) 滚动重启 3 个 Storage =========="
start_load rolling 45s 30
for i in 0 1 2; do
  kill_component "storage-$i"
  sleep 2
  restart_storage "$i"
  sleep 3
done
stop_load rolling
roll="$(awk -F= '/overall_success/{gsub(/%/,"",$2); print $2+0}' "$RUN/chaos-rolling.log" | tail -1 || true)"
if [[ -z "${roll}" ]]; then
  roll="$(python3 - <<'PY'
import re
vals=[]
for line in open(".run/chaos-rolling.log", errors="ignore"):
    m=re.search(r"success=([0-9.]+)%", line)
    if m: vals.append(float(m.group(1)))
print(sum(vals)/len(vals) if vals else 0)
PY
)"
fi
log "rolling overall≈${roll}%"
check_ge "$roll" 35 "rolling restart kept partial service"

final="$(wait_smoke_ge 90 60 post-rolling || true)"
check_ge "$final" 90 "final health OK"

log "========== 7) AE 故障注入（DROP+DELAY）后恢复 =========="
# 对 storage-0 注入丢包/延迟；负载应降级但仍有进展；取消注入后恢复
if [[ -f "$RUN/storage-0.pid" ]]; then
  old_pid="$(cat "$RUN/storage-0.pid")"
  kill_component storage-0
  SKV_FAULT_DROP_AE_PCT=30 SKV_FAULT_DELAY_AE_MS=20 \
    ./build/storage/storage_node conf/storage.conf >"$RUN/storage-0.log" 2>&1 &
  echo $! >"$RUN/storage-0.pid"
  disown || true
  wait_port 9100 30
  fault_pct="$(smoke_pct 30 || echo 0)"
  log "with AE fault inject success≈${fault_pct}%"
  # 注入期间允许更低阈值（复制变慢），但不应归零
  check_ge "$fault_pct" 20 "AE fault inject still serves"
  kill_component storage-0
  restart_storage 0
  inj_rec="$(wait_smoke_ge 85 45 after-ae-clear || true)"
  check_ge "$inj_rec" 85 "after clearing AE fault"
else
  log "warn: skip AE inject (no storage-0)"
fi

log "========== 8) Watch + 副本读冒烟 =========="
(cd client/go && go build -o "$RUN/kv-watch" ./examples/watch \
  && go build -o "$RUN/kv-stale" ./examples/stale_read)
if "$RUN/kv-watch" "$GW" >>"$LOG" 2>&1; then
  pass "watch example"
else
  fail "watch example"
fi
if "$RUN/kv-stale" "$GW" >>"$LOG" 2>&1; then
  pass "stale_read example"
else
  fail "stale_read example"
fi

log "========== 9) 恢复 meta-1（ClusterState 快照 + persist；要求 rejoin >=85%） =========="
if ! alive meta-1; then
  restart_meta_one 1
  rejoin="$(wait_smoke_ge 85 60 meta-1-rejoin || true)"
  check_ge "$rejoin" 85 "meta-1 rejoin data plane"
  if meta_route_ok; then
    pass "after meta-1 rejoin: GET_ROUTE reachable"
  else
    fail "after meta-1 rejoin: GET_ROUTE unavailable"
  fi
else
  log "meta-1 already alive; skip"
fi

log "========== 10) 停止全程负载并验收连续读写成功率 =========="
stop_load bg
trap - EXIT
bg_overall="$(chaos_overall bg)"
if [[ -z "$bg_overall" ]]; then
  bg_overall="$(python3 - <<'PY'
import re
vals=[]
for line in open(".run/chaos-bg.log", errors="ignore"):
    m=re.search(r"success=([0-9.]+)%", line)
    if m: vals.append(float(m.group(1)))
print(f"{sum(vals)/len(vals):.2f}" if vals else "0")
PY
)"
fi
log "continuous R/W overall_success≈${bg_overall}% (log=$RUN/chaos-bg.log)"
# 全程含杀节点/滚动重启，门槛低于纯稳态
check_ge "$bg_overall" 55 "continuous user R/W across whole drill >=55%"

log "=========================================="
log "RESULT: passed=$ok_steps failed=$fail_steps"
log "detail: $LOG"
[[ "$fail_steps" -eq 0 ]] || exit 1
echo "FAULT DRILL PASSED"
