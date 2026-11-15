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
GW="${GATEWAY_ADDR:-127.0.0.1:19000,127.0.0.1:19002}"
SKV="${SKV:-./build/tools/skv}"
mkdir -p "$RUN"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

"$SKV" status >/dev/null || fail "cluster down"
(cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos \
  && go build -o "$RUN/kv-example" ./examples/basic)

# 用监听端口找活进程，避免 .run/*.pid 陈旧误冻错人
pid_of() {
  local name="$1" port="" pid=""
  case "$name" in
    storage-0) port=9100 ;; storage-1) port=9101 ;; storage-2) port=9102 ;;
    storage-3) port=9103 ;; storage-4) port=9104 ;; storage-5) port=9105 ;;
    *) ;;
  esac
  if [[ -n "$port" ]]; then
    pid="$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -1 || true)"
  fi
  if [[ -z "$pid" ]]; then
    pid="$(cat "$RUN/$name.pid" 2>/dev/null || true)"
  fi
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "$pid"
  fi
}

# 从 skv route 取 Region 0：Leader 排前，再两个 Follower
PEERS_STR=$(python3 - "$("$SKV" route 2>/dev/null || true)" <<'PY'
import re, sys
s = sys.argv[1] if len(sys.argv) > 1 else ""
# OK ROUTE ... 0:::1:3,storage-0@127.0.0.1:9100*L,storage-1@...,storage-2@...
m = re.search(r"(?:^|\s)0:[^,\s]*:(\d+),([^\s]+)", s)
if not m:
    sys.exit(0)
peers = m.group(2).split(",")
leader, followers = None, []
for p in peers:
    mm = re.match(r"(storage-\d+)@", p)
    if not mm:
        continue
    name = mm.group(1)
    if "*L" in p:
        leader = name
    else:
        followers.append(name)
if not leader and peers:
    mm = re.match(r"(storage-\d+)@", peers[0])
    leader = mm.group(1) if mm else None
    followers = [re.match(r"(storage-\d+)@", x).group(1) for x in peers[1:] if re.match(r"(storage-\d+)@", x)]
out = []
if leader:
    out.append(leader)
out.extend(followers[:2])
print(" ".join(out))
PY
)
[[ -n "$PEERS_STR" ]] || fail "need skv route for R0 peers"
# shellcheck disable=SC2086
set -- $PEERS_STR
[[ $# -ge 3 ]] || fail "need 3 peers for R0, got: $PEERS_STR"
A="$1"; B="$2"; C="$3"
echo "R0 peers: A=$A(leader) B=$B C=$C"

# F11 前提：三副本都活；缺一台时「冻 1」会变成丢多数派
ensure_peer_up() {
  local n="$1" pid
  pid="$(pid_of "$n")"
  if [[ -n "$pid" ]]; then
    # 若仍停在 T 状态，先 CONT
    if ps -o state= -p "$pid" 2>/dev/null | grep -q T; then
      kill -CONT "$pid" 2>/dev/null || true
      sleep 1
    fi
    return 0
  fi
  echo "==> restart down peer $n before F11"
  "$SKV" restart "$n" >/dev/null 2>&1 || true
  sleep 2
  [[ -n "$(pid_of "$n")" ]] || fail "peer $n still down"
}
ensure_peer_up "$A"
ensure_peer_up "$B"
ensure_peer_up "$C"
# 等 Leader 标出来
for _ in $(seq 1 15); do
  if "$SKV" route 2>/dev/null | grep -q '\*L'; then break; fi
  sleep 1
done
# 刷新 Leader/Follower 顺序（restart 后可能换主）
PEERS_STR=$(python3 - "$("$SKV" route 2>/dev/null || true)" <<'PY'
import re, sys
s = sys.argv[1] if len(sys.argv) > 1 else ""
m = re.search(r"(?:^|\s)0:[^,\s]*:(\d+),([^\s]+)", s)
if not m:
    sys.exit(0)
peers = m.group(2).split(",")
leader, followers = None, []
for p in peers:
    mm = re.match(r"(storage-\d+)@", p)
    if not mm:
        continue
    name = mm.group(1)
    if "*L" in p:
        leader = name
    else:
        followers.append(name)
out = ([leader] if leader else []) + followers[:2]
print(" ".join(x for x in out if x))
PY
)
set -- $PEERS_STR
A="$1"; B="$2"; C="$3"
echo "R0 peers (ready): A=$A(leader) B=$B C=$C"
[[ -n "$A" && -n "$B" && -n "$C" ]] || fail "R0 peers incomplete after ensure"

# stdout: overall write read
# 冻副本时 AE 可能拖慢提交，op-timeout 放宽，避免把「慢」误判成「不可用」
smoke_rw() {
  local dur="${1:-6s}" qps="${2:-30}"
  local out
  out="$("$RUN/kv-chaos" -addr "$GW" -duration "$dur" -qps "$qps" -workers 2 \
    -op-timeout 8s -report 20s 2>&1 || true)"
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
  read -r o w r <<< "$(smoke_rw)"
  printf '%s\n' "${o:-0}"
}

echo "==> baseline (write+read)"
BASE_W=0 BASE_R=0
for _ in $(seq 1 12); do
  read -r _ BASE_W BASE_R <<< "$(smoke_rw 5s 40)"
  if python3 -c "import sys; sys.exit(0 if float('${BASE_W:-0}')>=80 and float('${BASE_R:-0}')>=80 else 1)"; then
    break
  fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${BASE_W:-0}')>=80 and float('${BASE_R:-0}')>=80 else 1)" \
  || fail "baseline write=$BASE_W read=$BASE_R"
ok "baseline write=${BASE_W}% read=${BASE_R}%"

echo "==> F11-approx: STOP one follower ($B), majority write+read OK"
PB=$(pid_of "$B")
[[ -n "$PB" ]] || fail "no live pid $B"
[[ "$B" != "$A" ]] || fail "follower B must not equal leader A"
# 另一 Follower 必须仍活，否则冻 1 = 丢多数派
[[ -n "$(pid_of "$C")" ]] || fail "peer $C down before one-STOP (would false-fail majority)"
kill -STOP "$PB"
sleep 1
ps -o pid,state,command -p "$PB" "$(pid_of "$A")" "$(pid_of "$C")" 2>/dev/null || true
sleep 1
read -r _ ONE_W ONE_R <<< "$(smoke_rw 8s 20)"
# 冻 1：写可能变慢但仍应用；读门槛单独看（线性读常比写稳或反之）
python3 -c "import sys; sys.exit(0 if float('${ONE_W:-0}')>=40 and float('${ONE_R:-0}')>=40 else 1)" \
  || fail "one STOP should still serve write+read, got write=$ONE_W read=$ONE_R"
kill -CONT "$PB"
ok "F11 one STOP write=${ONE_W}% read=${ONE_R}%"

echo "==> F11-approx: STOP two ($B,$C) → expect write near-zero (读可另记)"
PB=$(pid_of "$B"); PC=$(pid_of "$C")
[[ -n "$PB" && -n "$PC" ]] || fail "need live pids for $B and $C"
kill -STOP "$PB"
kill -STOP "$PC"
sleep 2
read -r _ TWO_W TWO_R <<< "$(smoke_rw 8s 20)"
# 正确降级：只认写可用性（读另记）。写须明显掉下来，禁止用 overall 混过。
python3 -c "import sys; one=float('${ONE_W:-100}'); two=float('${TWO_W:-100}'); sys.exit(0 if two<=30 or two<=one-25 else 1)" \
  || fail "two STOP must degrade writes (one_w=$ONE_W two_w=$TWO_W read=$TWO_R)"
ok "F11 two STOP write degraded (write=${TWO_W}% read=${TWO_R}%)"
kill -CONT "$PB" 2>/dev/null || true
kill -CONT "$PC" 2>/dev/null || true
sleep 6
REC_W=0 REC_R=0
for _ in $(seq 1 10); do
  read -r _ REC_W REC_R <<< "$(smoke_rw 5s 30)"
  if python3 -c "import sys; sys.exit(0 if float('${REC_W:-0}')>=70 and float('${REC_R:-0}')>=70 else 1)"; then break; fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${REC_W:-0}')>=70 and float('${REC_R:-0}')>=70 else 1)" \
  || fail "CONT recover write=$REC_W read=$REC_R"
ok "F11 recovered write=${REC_W}% read=${REC_R}%"

echo "==> F3: kill two peers ($A,$B), expect write unavailable; restart one"
"$SKV" kill "$A" 2>/dev/null || true
"$SKV" kill "$B" 2>/dev/null || true
sleep 2
read -r _ DEAD_W DEAD_R <<< "$(smoke_rw 6s 20)"
python3 -c "import sys; sys.exit(0 if float('${DEAD_W:-100}')<=30 else 1)" \
  || fail "F3 must be write-unavailable (write≤30%), got write=$DEAD_W read=$DEAD_R — do not false-PASS"
ok "F3 correctly write-unavailable (write=${DEAD_W}% read=${DEAD_R}%)"
"$SKV" restart "$A" 2>/dev/null || true
sleep 8
BACK_W=0 BACK_R=0
for _ in $(seq 1 12); do
  read -r _ BACK_W BACK_R <<< "$(smoke_rw 5s 30)"
  if python3 -c "import sys; sys.exit(0 if float('${BACK_W:-0}')>=60 and float('${BACK_R:-0}')>=60 else 1)"; then break; fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('${BACK_W:-0}')>=60 and float('${BACK_R:-0}')>=60 else 1)" \
  || fail "F3 recover write=$BACK_W read=$BACK_R"
"$SKV" restart "$B" 2>/dev/null || true
sleep 5
ok "F3 recovered after restarting majority (write=${BACK_W}% read=${BACK_R}%)"

echo
echo "ALL PASS: it-partition"
