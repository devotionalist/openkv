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

# F14：storage-1 数据目录只读 → 多数派仍写/读；恢复权限后追赶。
# 轻量 F15：宕机拉大落后后重启，期望 recover/AE（不强毁文件以免不可恢复）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
GW="${GATEWAY_ADDR:-127.0.0.1:19000}"
SKV="${SKV:-./build/tools/skv}"
DATA1="$ROOT/data/storage-1"
mkdir -p "$RUN"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

cleanup() {
  chmod -R u+w "$DATA1" 2>/dev/null || true
}
trap cleanup EXIT

"$SKV" status >/dev/null || fail "cluster down"
(cd client/go && go build -o "$RUN/kv-chaos" ./examples/chaos)

# stdout: overall write read
smoke_rw() {
  local out
  out="$("$RUN/kv-chaos" -addr "$GW" -duration 4s -qps 30 -report 10s 2>&1)"
  python3 - "$out" <<'PY'
import re, sys
s = sys.argv[1]
def grab_last(k):
    ms = re.findall(rf"{k}=([0-9.]+)%", s)
    return ms[-1] if ms else "0"
print(grab_last("overall_success"), grab_last("write_success"), grab_last("read_success"))
PY
}

check_rw_ge() {
  local w="$1" r="$2" w_thr="$3" r_thr="$4" msg="$5"
  python3 -c "import sys; sys.exit(0 if float('$w' or 0)>=float('$w_thr') and float('$r' or 0)>=float('$r_thr') else 1)" \
    || fail "$msg (write=${w}% need≥${w_thr}; read=${r}% need≥${r_thr})"
  ok "$msg (write=${w}% read=${r}%)"
}

echo "==> baseline write+read (wait recover)"
BASE_W=0 BASE_R=0
for _ in $(seq 1 15); do
  read -r _ BASE_W BASE_R <<< "$(smoke_rw)"
  if python3 -c "import sys; sys.exit(0 if float('${BASE_W:-0}')>=85 and float('${BASE_R:-0}')>=85 else 1)"; then
    break
  fi
  sleep 2
done
check_rw_ge "$BASE_W" "$BASE_R" 85 85 "baseline write+read"

echo "==> F14 chmod a-w data/storage-1 + restart"
"$SKV" kill storage-1 2>/dev/null || true
sleep 0.5
chmod a-w "$DATA1" 2>/dev/null || chmod -R a-w "$DATA1"
# 尝试重启：可能失败或半活；多数派应仍可写/读
set +e
"$SKV" restart storage-1 >/tmp/it_disk_restart.txt 2>&1
set -e
sleep 3
read -r _ DOWN_W DOWN_R <<< "$(smoke_rw)"
echo "majority during readonly peer write≈${DOWN_W}% read≈${DOWN_R}%"
check_rw_ge "$DOWN_W" "$DOWN_R" 70 70 "F14 majority write+read while storage-1 readonly"

echo "==> restore permissions + restart catch-up"
"$SKV" kill storage-1 2>/dev/null || true
chmod -R u+w "$DATA1" 2>/dev/null || true
find "$DATA1" -name LOCK -delete 2>/dev/null || true
"$SKV" restart storage-1 >/dev/null 2>&1 || true
REC_W=0 REC_R=0
for _ in $(seq 1 20); do
  sleep 2
  read -r _ REC_W REC_R <<< "$(smoke_rw)"
  echo "  recover write≈${REC_W}% read≈${REC_R}%"
  if python3 -c "import sys; sys.exit(0 if float('${REC_W:-0}')>=70 and float('${REC_R:-0}')>=70 else 1)"; then
    break
  fi
done
check_rw_ge "$REC_W" "$REC_R" 70 70 "F14 recovered write+read"

echo "==> light F15: kill storage-1, write window, restart"
"$SKV" kill storage-1 2>/dev/null || true
sleep 1
"$RUN/kv-chaos" -addr "$GW" -duration 6s -qps 40 -prefix diskf15 >/dev/null 2>&1 || true
find "$DATA1" -name LOCK -delete 2>/dev/null || true
"$SKV" restart storage-1 >/dev/null 2>&1 || true
sleep 5
if grep -qE "recovered term=|installed snapshot|compacted to|AE|append" "$RUN/storage-1.log" 2>/dev/null; then
  ok "F15-ish recover log present"
else
  echo "warn: no explicit recover log line (acceptable if smoke OK)"
fi
F15_W=0 F15_R=0
for _ in $(seq 1 12); do
  read -r _ F15_W F15_R <<< "$(smoke_rw)"
  if python3 -c "import sys; sys.exit(0 if float('${F15_W:-0}')>=70 and float('${F15_R:-0}')>=70 else 1)"; then
    break
  fi
  sleep 2
done
check_rw_ge "$F15_W" "$F15_R" 70 70 "F15 catch-up write+read"

echo
echo "ALL PASS: it-disk-fault"
