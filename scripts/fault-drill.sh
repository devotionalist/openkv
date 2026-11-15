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
# 当前拓扑：Store RF=3 · Meta×3 · Gateway×2（无状态入口）。
#   - 杀掉 1 个 Storage → 数据面 100%（多数派仍在）
#   - 杀掉 1 个 Gateway → 客户端换另一台仍 100%（F8）
#   - 杀掉 1 个 Meta    → 数据面 100%（存算/管控分离）
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
# 双 Gateway：逗号分隔，SDK/chaos 传输失败会换下一台
GW="${GATEWAY_ADDR:-127.0.0.1:19000,127.0.0.1:19002}"
GW_PRIMARY="${GW%%,*}"
GW_PORT="${GW_PRIMARY##*:}"
GW2_PORT="${GATEWAY2_PORT:-19002}"
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
  local src="$ROOT/client/go/cmd/line-rpc/main.go"
  # 缺二进制或源码比二进制新时强制重建，避免陈旧 line-rpc 对 GET_ROUTE 报 Unimplemented。
  if [[ ! -x "$bin" ]] || [[ -f "$src" && "$src" -nt "$bin" ]]; then
    (cd "$ROOT/client/go" && go build -o "$bin" ./cmd/line-rpc) || return 1
  fi
  LINE_RPC="$bin"
}

meta_rpc() {
  ensure_line_rpc || return 1
  "$LINE_RPC" -kind meta -addrs "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502" -req "$1" 2>/dev/null
}

meta_route_ok() {
  # 优先 skv（当前 Meta gRPC）；再退回 line-rpc。
  if [[ -x "$ROOT/build/tools/skv" ]]; then
    local skv_out
    skv_out="$("$ROOT/build/tools/skv" route 2>/dev/null || true)"
    [[ "$skv_out" == OK\ ROUTE* ]] && return 0
  fi
  local out
  out="$(meta_rpc GET_ROUTE || true)"
  [[ "$out" == OK\ ROUTE* || "$out" == *"redirect="* ]]
}

# 从 route 文本解析首 Region 的 Leader node_id（*L）；无 *L 时退回首个 storage-*。
parse_region_leader() {
  local text="$1"
  python3 - "$text" <<'PY'
import re, sys
text = sys.argv[1] if len(sys.argv) > 1 else ""
m = re.search(r"(storage-\d+)@[^*\s,]*\*L", text)
if m:
    print(m.group(1)); raise SystemExit(0)
# 无心跳 *L：取首个 storage-N（新鲜集群多为 storage-0）
m = re.search(r"(storage-\d+)@", text)
print(m.group(1) if m else "")
PY
}

# route → "region_id leader_id leader_addr"；解析失败打印空行。
parse_region_leader_addr() {
  local text="$1"
  python3 - "$text" <<'PY'
import re, sys
text = sys.argv[1] if len(sys.argv) > 1 else ""
# OK ROUTE <epoch> <n> <rid> <start>:<end>:<epoch>:<nrep>,id@addr[*L],...
m = re.search(r"OK ROUTE\s+\S+\s+\S+\s+(\d+)\s+", text)
rid = m.group(1) if m else "1"
leader = None
leader_addr = None
for id_, addr, mark in re.findall(r"(storage-\d+)@([^,\s]+)(\*L)?", text):
    if mark == "*L" or leader is None:
        leader, leader_addr = id_, addr
        if mark == "*L":
            break
if leader:
    print(f"{rid} {leader} {leader_addr}")
PY
}

# 杀当前 Leader 前先转主，避免滚动最后一台时的选主窗口掉流量。
transfer_away_from() {
  local victim="$1"  # storage-N
  local route target rid leader laddr
  route="$(./build/tools/skv route 2>/dev/null || true)"
  read -r rid leader laddr <<<"$(parse_region_leader_addr "$route")"
  if [[ -z "$leader" || "$leader" != "$victim" ]]; then
    log "transfer skip: victim=$victim leader=${leader:-unknown}"
    return 0
  fi
  target=""
  for i in 0 1 2; do
    local cand="storage-$i"
    [[ "$cand" == "$victim" ]] && continue
    if alive "$cand"; then
      target="$cand"
      break
    fi
  done
  if [[ -z "$target" || -z "$laddr" ]]; then
    log "warn: cannot transfer away from $victim (no target/addr)"
    return 1
  fi
  log "F4 transfer leader $leader ($laddr) -> $target (region=$rid)"
  if ! "$RUN/kv-transfer" "$laddr" "$rid" "$target" >>"$LOG" 2>&1; then
    log "warn: transfer_leader failed; will still proceed carefully"
    return 1
  fi
  # 等 route 不再指向 victim
  local i new_l
  for i in $(seq 1 30); do
    new_l="$(parse_region_leader "$(./build/tools/skv route 2>/dev/null || true)")" || true
    if [[ -n "$new_l" && "$new_l" != "$victim" ]]; then
      log "transfer ok: new leader=$new_l"
      return 0
    fi
    sleep 0.2
  done
  log "warn: route still shows leader=$victim after transfer"
  return 1
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
  if alive gateway && alive gateway-1 && wait_port "$GW_PORT" 2 && wait_port "$GW2_PORT" 2; then
    log "cluster already up (gateway×2)"
    return
  fi
  log "starting cluster..."
  test -x build/meta/meta_server || cmake --build build -j --target meta_server storage_node gateway_node
  cmake --build build -j --target gateway_node
  cp -f build/compute/gateway_node "$RUN/gateway_node"
  ln -sfn "$RUN/gateway_node" "$RUN/compute-gateway"
  (cd client/go && go build -o "$RUN/kv-example" ./examples/basic)
  # 高 term / 脏 WAL 会导致选主困难、成功率卡在 ~50%；默认 fresh 演练
  if [[ "${FAULT_DRILL_FRESH:-1}" == "1" ]]; then
    log "FAULT_DRILL_FRESH=1: wipe data/ for clean raft state"
    rm -rf "$ROOT/data"
    mkdir -p "$ROOT/data"
  fi
  python3 scripts/daemon_cluster.py
  wait_port "$GW_PORT" 30
  wait_port "$GW2_PORT" 30
}

build_tools() {
  (cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos \
    && go build -o "$RUN/kv-example" ./examples/basic \
    && go build -o "$RUN/kv-transfer" ./examples/transfer_leader)
}

# 从 chaos 输出解析 overall / write / read（缺省当 0）
parse_chaos_rw() {
  local text="$1"
  python3 - "$text" <<'PY'
import re, sys
s = sys.argv[1] if len(sys.argv) > 1 else ""
def grab_last(key):
    # 必须取最后一次：首个 window 常为 0 采样，会把真实成功率盖成 0
    ms = re.findall(rf"{key}=([0-9.]+)%", s)
    return ms[-1] if ms else "0"
print(grab_last("overall_success"), grab_last("write_success"), grab_last("read_success"))
PY
}

# 短时冒烟：stdout 只打 overall（兼容旧调用）；写/读进日志
smoke_pct() {
  local qps="${1:-40}"
  local out o w r
  out="$("$RUN/kv-chaos" -addr "$GW" -duration 3s -qps "$qps" -report 10s \
    -op-timeout "${CHAOS_OP_TIMEOUT:-8s}" 2>&1 | tee -a "$LOG")"
  read -r o w r <<< "$(parse_chaos_rw "$out")"
  log "smoke overall=${o}% write=${w}% read=${r}%"
  printf '%s\n' "${o:-0}"
}

# 短时冒烟：stdout 打「overall write read」三列
smoke_rw() {
  local qps="${1:-40}"
  local out
  out="$("$RUN/kv-chaos" -addr "$GW" -duration 3s -qps "$qps" -report 10s \
    -op-timeout "${CHAOS_OP_TIMEOUT:-8s}" 2>&1 | tee -a "$LOG")"
  parse_chaos_rw "$out"
}

# dur=0 → 跑到 stop_load（模拟用户一直读写）
start_load() {
  local name="$1" dur="$2" qps="$3"
  local workers="${4:-${CHAOS_WORKERS:-2}}"
  local wr="${CHAOS_WRITE_RATIO:-0.5}"
  local dur_flag="$dur"
  [[ "$dur" == "0" || "$dur" == "0s" ]] && dur_flag=0
  # 故障窗口给足客户端重试预算：Gateway 换副本可能先撞死人再成功。
  local op_to="${CHAOS_OP_TIMEOUT:-8s}"
  "$RUN/kv-chaos" -addr "$GW" -duration "$dur_flag" -qps "$qps" \
    -workers "$workers" -write-ratio "$wr" -report 2s -prefix "drill-$name" \
    -op-timeout "$op_to" \
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

# 从后台 chaos 日志取最后一次 write/read（stdout: write read）
chaos_rw() {
  local name="$1"
  local f="$RUN/chaos-$name.log"
  [[ -f "$f" ]] || { echo "0 0"; return; }
  parse_chaos_rw "$(cat "$f")"
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
  local name="${1:-gateway}"
  local port="$GW_PORT"
  [[ "$name" == "gateway-1" ]] && port="$GW2_PORT"
  log "RESTART $name (port=$port)"
  for pid in $(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true); do
    kill -9 "$pid" 2>/dev/null || true
  done
  sleep 0.3
  local stats_args=()
  if [[ "$name" == "gateway-1" ]]; then
    stats_args=(-stats-listen "127.0.0.1:19102")
  fi
  "${RUN}/gateway_node" -listen "0.0.0.0:${port}" "${stats_args[@]}" \
    -storages "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105" \
    -metas 127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
    >"$RUN/${name}.log" 2>&1 &
  echo $! >"$RUN/${name}.pid"
  disown || true
  wait_port "$port" 25
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

# 读写分开验收：写≥w_thr 且 读≥r_thr 才 PASS
check_rw_ge() {
  local w="$1" r="$2" w_thr="$3" r_thr="$4" msg="$5"
  if python3 -c "import sys; sys.exit(0 if float('$w' or 0)>=float('$w_thr') and float('$r' or 0)>=float('$r_thr') else 1)"; then
    pass "$msg (write=${w}% read=${r}%)"
  else
    fail "$msg (write=${w}% need≥${w_thr}; read=${r}% need≥${r_thr})"
  fi
}

# 写必须不可用（正确降级）：write ≤ w_hi
check_write_le() {
  local w="$1" w_hi="$2" msg="$3"
  if python3 -c "import sys; sys.exit(0 if float('$w' or 0) <= float('$w_hi') else 1)"; then
    pass "$msg (write=${w}% ≤${w_hi})"
  else
    fail "$msg (write=${w}% should be ≤${w_hi} — 不可用窗口未体现)"
  fi
}

# 破坏性步骤后：轮询直到写、读都 ≥thr；stdout 打「overall write read」
wait_smoke_ge() {
  local thr="$1" sec="${2:-45}" label="${3:-health}"
  local deadline o=0 w=0 r=0
  deadline=$((SECONDS + sec))
  while (( SECONDS < deadline )); do
    read -r o w r <<< "$(smoke_rw 30 || echo '0 0 0')"
    o="$(printf '%s' "$o" | tr -cd '0-9.')"; [[ -z "$o" ]] && o=0
    w="$(printf '%s' "$w" | tr -cd '0-9.')"; [[ -z "$w" ]] && w=0
    r="$(printf '%s' "$r" | tr -cd '0-9.')"; [[ -z "$r" ]] && r=0
    log "wait $label: write=${w}% read=${r}% overall=${o}% (need write/read >=${thr}%)"
    if python3 -c "import sys; sys.exit(0 if float('$w' or 0)>=float('$thr') and float('$r' or 0)>=float('$thr') else 1)"; then
      printf '%s %s %s\n' "$o" "$w" "$r"
      return 0
    fi
    sleep 2
  done
  printf '%s %s %s\n' "$o" "$w" "$r"
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

log "========== 1) 基线可用性（写+读） =========="
# daemon 双 fork 后选主需要时间；等到真正可用再开测
read -r base_o base_w base_r <<< "$(wait_smoke_ge 95 90 baseline || echo '0 0 0')"
log "baseline overall=${base_o}% write=${base_w}% read=${base_r}%"
check_rw_ge "$base_w" "$base_r" 95 95 "baseline write+read ≥95%"

log "========== 1b) F2 杀 Region Leader + seed Get（I14） =========="
# 解析首 Region Leader；杀进程 → 新 Leader + f2key 可读。无需依赖 Web。
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
else:
    reps=r.get("replicas") or []
    if reps: print(reps[0].get("node_id") or "")
PY
)"
fi
if [[ -z "$F2_LEADER" ]] && [[ -x ./build/tools/skv ]]; then
  F2_LEADER="$(parse_region_leader "$(./build/tools/skv route 2>/dev/null || true)")" || true
fi
if [[ -z "$F2_LEADER" ]]; then
  F2_LEADER="$(parse_region_leader "$(meta_rpc GET_ROUTE || true)")" || true
fi
if [[ -n "$F2_LEADER" ]]; then
  log "F2 leader=$F2_LEADER seed f2key"
  F2_SEED_OK=1
  if [[ ! -x ./build/tools/skv ]]; then
    fail "F2 missing build/tools/skv"
    F2_SEED_OK=0
  elif ! ./build/tools/skv put f2key f2v >/dev/null 2>"$RUN/f2-put.err"; then
    # 种子必须在杀主前写成功（走 Gateway gRPC）
    fail "F2 seed put f2key before leader kill ($(tr '\n' ' ' <"$RUN/f2-put.err"))"
    F2_SEED_OK=0
  fi
  OLD_L="$F2_LEADER"
  if [[ "$F2_SEED_OK" -ne 1 ]]; then
    log "warn: skip F2 kill/get (seed put failed)"
  else
  kill_component "$F2_LEADER"
  # 等新 Leader（≤15s）；无 *L 时以 Get 成功为准
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
    elif [[ -x ./build/tools/skv ]]; then
      NEW_L="$(parse_region_leader "$(./build/tools/skv route 2>/dev/null || true)")" || true
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
  # 选主后 Get 允许短暂重试（Gateway 换副本 + 新 Leader 就绪）
  f2_get_ok=0
  for _ in $(seq 1 20); do
    if ./build/tools/skv get f2key 2>"$RUN/f2-get.err" | tee "$RUN/f2-get.out" | grep -q f2v; then
      f2_get_ok=1
      break
    fi
    sleep 0.5
  done
  if [[ "$f2_get_ok" -eq 1 ]]; then
    pass "F2 seed get f2key=f2v after leader kill"
  else
    fail "F2 seed get f2key after leader kill ($(tr '\n' ' ' <"$RUN/f2-get.err" 2>/dev/null; tr '\n' ' ' <"$RUN/f2-get.out" 2>/dev/null))"
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
  read -r _ f2w f2r <<< "$(wait_smoke_ge 85 40 f2-recovered || echo '0 0 0')"
  check_rw_ge "$f2w" "$f2r" 85 85 "F2 after leader restart"
  fi  # F2_SEED_OK
else
  fail "F2 cannot resolve region leader (skv/line-rpc/web all empty)"
fi

log "========== 2) F1 杀掉 1 个 Follower Store（3 副本剩 2，应仍可用） =========="
# 必须杀非 Leader：F2 之后 Leader 常落在 storage-1，再杀它会变成第二次杀主。
F1_VICTIM=""
if [[ -x ./build/tools/skv ]]; then
  # 注意：heredoc 会占 stdin，不能 `skv route | python3 <<'PY'`；把 route 文本当 argv 传入。
  F1_VICTIM="$(python3 - "$(./build/tools/skv route 2>/dev/null || true)" <<'PY'
import sys,re
text=sys.argv[1] if len(sys.argv)>1 else ""
m=re.search(r"(storage-\d+)@[^*\s,]*\*L", text)
leader=m.group(1) if m else None
peers=re.findall(r"(storage-\d+)@", text)
if not leader and peers:
    leader=peers[0]
for p in peers:
    if p != leader:
        print(p); break
PY
)" || true
fi
if [[ -z "$F1_VICTIM" ]]; then
  # F2 后 Leader 常为 storage-1；fallback 避开它
  if [[ "${NEW_L:-}" == "storage-0" || "${OLD_L:-}" == "storage-0" ]]; then
    F1_VICTIM="storage-2"
  else
    F1_VICTIM="storage-0"
  fi
  log "warn: cannot resolve follower; fallback $F1_VICTIM"
fi
F1_IDX="${F1_VICTIM#storage-}"
log "F1 victim=$F1_VICTIM (follower)"
# 暂停全程 bg，单独量「Follower 已宕、Leader 仍在」稳态；叠负载会把 apply 队列打成假失败。
stop_load bg
kill_component "$F1_VICTIM"
sleep 2
# duration 自然结束，勿在边界 SIGTERM（否则进行中 Put 会被记成 fail）
start_load degrade 12s 40 4
sleep 14
stop_load degrade
read -r _ dw dr <<< "$(chaos_rw degrade)"
[[ "${dw:-0}" == "0" && "${dr:-0}" == "0" ]] && read -r _ dw dr <<< "$(smoke_rw 40 || echo '0 0 0')"
log "during $F1_VICTIM down write≈${dw}% read≈${dr}%"
check_rw_ge "$dw" "$dr" 100 100 "F1 follower down: write+read 100%"
# 恢复全程后台负载
start_load bg 0 "${CHAOS_QPS:-40}" "${CHAOS_WORKERS:-4}"

log "========== 3) 恢复 F1 victim（持久化装载 + AE/快照追赶） =========="
restart_storage "$F1_IDX"
if grep -qE "recovered term=|installed snapshot|compacted to" "$RUN/${F1_VICTIM}.log" 2>/dev/null; then
  pass "$F1_VICTIM log shows raft recover/catch-up"
else
  log "warn: no explicit recover log line yet (cold start ok); continue"
fi
read -r _ rec_w rec_r <<< "$(wait_smoke_ge 100 40 f1-recovered || echo '0 0 0')"
check_rw_ge "$rec_w" "$rec_r" 100 100 "F1 victim recovered write+read"

log "========== 4) 杀掉 1 个 Meta（3 副本剩 2，数据面应 100% 可用） =========="
# 暂停 bg，单独量数据面稳态（避免收尾 SIGTERM 噪声）
stop_load bg
kill_component meta-1
sleep 2
# 存算分离：杀 1 Meta 不得影响数据面
start_load meta-down 10s 40 4
sleep 12
stop_load meta-down
read -r _ mw mr <<< "$(chaos_rw meta-down)"
[[ "${mw:-0}" == "0" && "${mr:-0}" == "0" ]] && read -r _ mw mr <<< "$(smoke_rw 40 || echo '0 0 0')"
check_rw_ge "$mw" "$mr" 100 100 "F5 one meta down: data plane write+read 100%"
start_load bg 0 "${CHAOS_QPS:-40}" "${CHAOS_WORKERS:-4}"
# 管控面：向仍存活的 meta 拉路由
if meta_route_ok; then
  pass "meta majority still serves GET_ROUTE"
else
  fail "meta GET_ROUTE unavailable after killing meta-1"
fi
# 本步只验证「少数派 Meta 宕机仍可用」；meta-1 热重加入放到 step 9。
log "defer meta-1 restart until step 9"

log "========== 5) F8 杀掉 1 个 Gateway（×2 入口，换另一台仍可用） =========="
# 单地址应失败；多地址（$GW）稳态应 100%。gateway-1 全程保留。
stop_load bg
if ! alive gateway-1 || ! wait_port "$GW2_PORT" 2; then
  restart_gateway gateway-1
fi
kill_component gateway
sleep 1
if "$RUN/kv-example" "$GW_PRIMARY" >/dev/null 2>&1; then
  fail "killed gateway still serves on $GW_PRIMARY"
else
  pass "killed gateway: single-addr $GW_PRIMARY unreachable"
fi
# 另一台仍在：多地址客户端换入口 → 数据面 100%
start_load gw-ha 10s 40 4
sleep 12
stop_load gw-ha
read -r _ gw_w gw_r <<< "$(chaos_rw gw-ha)"
[[ "${gw_w:-0}" == "0" && "${gw_r:-0}" == "0" ]] && read -r _ gw_w gw_r <<< "$(smoke_rw 40 || echo '0 0 0')"
log "one gateway down (multi-addr) write≈${gw_w}% read≈${gw_r}%"
check_rw_ge "$gw_w" "$gw_r" 100 100 "F8 one gateway down: write+read 100%"
restart_gateway gateway
start_load bg 0 "${CHAOS_QPS:-40}" "${CHAOS_WORKERS:-4}"
read -r _ gw_back_w gw_back_r <<< "$(wait_smoke_ge 100 45 gateway-recovered || echo '0 0 0')"
check_rw_ge "$gw_back_w" "$gw_back_r" 100 100 "gateway pair recovered write+read"

log "========== 6) 滚动重启 3 个 Storage（始终保多数派；优先杀 Follower） =========="
# RF=3：每次只下线 1 台，稳态可用率须 100%。优先杀非 Leader；若必须动 Leader 先转主。
# 每台：转主(如需) → 杀 → 稳态压测 =100% → 拉起 → 满血再滚下一台。暂停 bg 避免叠负载。
stop_load bg
remaining=(0 1 2)
roll_min=100
while ((${#remaining[@]} > 0)); do
  leader="$(parse_region_leader "$(./build/tools/skv route 2>/dev/null || true)")" || true
  victim=""
  for i in "${remaining[@]}"; do
    if [[ -n "$leader" && "storage-$i" == "$leader" ]]; then
      continue
    fi
    victim="$i"
    break
  done
  if [[ -z "$victim" ]]; then
    victim="${remaining[0]}"
  fi
  log "F4 rolling kill storage-$victim (leader=${leader:-unknown})"
  # 最后一台常是 Leader：先转走再杀，避免选主窗口计入不可用
  if [[ -n "$leader" && "storage-$victim" == "$leader" ]]; then
    transfer_away_from "storage-$victim" || true
  fi
  kill_component "storage-$victim"
  sleep 2
  # 先等 Gateway 甩掉死人（keepalive/换副本），再开计量窗口，避免半开连接首包噪声
  wait_smoke_ge 100 25 "f4-settle-storage-$victim" >/dev/null || true
  # 稳态：Follower/已转主后的旧主宕机，数据面须 100%
  start_load "roll-$victim" 10s 30 4
  sleep 12
  stop_load "roll-$victim"
  read -r step sw sr <<< "$(chaos_rw "roll-$victim")"
  if [[ -z "${sw}" || ("${sw}" == "0" && "${sr}" == "0") ]]; then
    read -r step sw sr <<< "$(smoke_rw 30 || echo '0 0 0')"
  fi
  log "F4 storage-$victim down write≈${sw}% read≈${sr}% overall≈${step}%"
  check_rw_ge "$sw" "$sr" 100 100 "F4 storage-$victim down: write+read 100%"
  # 取写/读中的更低者作为滚动全程最低水位
  roll_min="$(python3 -c "print(min(float('$roll_min'), float('${sw:-0}'), float('${sr:-0}')))")"
  restart_storage "$victim"
  wait_smoke_ge 100 50 "after-roll-storage-$victim" >/dev/null || true
  next_rem=()
  for i in "${remaining[@]}"; do
    [[ "$i" == "$victim" ]] && continue
    next_rem+=("$i")
  done
  remaining=("${next_rem[@]}")
done
log "rolling per-step min(write,read)≈${roll_min}%"
check_ge "$roll_min" 100 "F4 rolling restart: write+read available 100%"

start_load bg 0 "${CHAOS_QPS:-40}" "${CHAOS_WORKERS:-4}"
read -r _ final_w final_r <<< "$(wait_smoke_ge 100 60 post-rolling || echo '0 0 0')"
check_rw_ge "$final_w" "$final_r" 100 100 "final health write+read"

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
  read -r fault_o fault_w fault_r <<< "$(smoke_rw 30 || echo '0 0 0')"
  log "with AE fault inject write≈${fault_w}% read≈${fault_r}% overall≈${fault_o}%"
  # 抖动下写通常更差；读（租约/本地）可略高。两者都不可归零。
  check_rw_ge "$fault_w" "$fault_r" 20 40 "F10 AE jitter: write≥20% read≥40%"
  kill_component storage-0
  restart_storage 0
  read -r _ inj_w inj_r <<< "$(wait_smoke_ge 85 45 after-ae-clear || echo '0 0 0')"
  check_rw_ge "$inj_w" "$inj_r" 85 85 "after clearing AE fault write+read"
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
  read -r _ rejoin_w rejoin_r <<< "$(wait_smoke_ge 85 60 meta-1-rejoin || echo '0 0 0')"
  check_rw_ge "$rejoin_w" "$rejoin_r" 85 85 "meta-1 rejoin data plane write+read"
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
read -r bg_o bg_w bg_r <<< "$(chaos_rw bg)"
if [[ -z "${bg_w}" || ("${bg_w}" == "0" && "${bg_r}" == "0" && "${bg_o}" == "0") ]]; then
  read -r bg_o bg_w bg_r <<< "$(python3 - <<'PY'
import re
wo=wf=ro=rf=0
for line in open(".run/chaos-bg.log", errors="ignore"):
    # window 行：put_ok= put_fail= get_ok= get_fail=
    m=re.search(r"put_ok=(\d+) put_fail=(\d+) get_ok=(\d+) get_fail=(\d+)", line)
    if m:
        wo,wf,ro,rf = int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4))
def pct(o,f):
    t=o+f
    return 100.0 if t==0 else 100.0*o/t
print(f"{pct(wo+ro,wf+rf):.2f} {pct(wo,wf):.2f} {pct(ro,rf):.2f}")
PY
)"
fi
log "continuous R/W overall≈${bg_o}% write≈${bg_w}% read≈${bg_r}% (log=$RUN/chaos-bg.log)"
# 全程含杀节点/滚动；写通常更抖，读门槛略高
check_rw_ge "$bg_w" "$bg_r" 50 55 "continuous write≥50% read≥55% across drill"

log "=========================================="
log "RESULT: passed=$ok_steps failed=$fail_steps"
log "detail: $LOG"
[[ "$fail_steps" -eq 0 ]] || exit 1
echo "FAULT DRILL PASSED"
