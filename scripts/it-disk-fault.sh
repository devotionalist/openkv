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

# F14：storage-1 数据目录只读 → 多数派仍服务；恢复权限后追赶。
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

smoke() {
  local pct
  # 取 DONE 行的 overall_success（避免 window 行误导）
  pct="$("$RUN/kv-chaos" -addr "$GW" -duration 4s -qps 30 -report 10s 2>&1 \
    | awk -F= '/DONE/{next} /overall_success/{gsub(/%/,"",$2); v=$2+0} END{print v+0}')"
  # 上面 END 在无 DONE 时也行；再优先匹配带 overall 的最后一行
  if [[ -z "$pct" || "$pct" == "0" ]]; then
    pct="$("$RUN/kv-chaos" -addr "$GW" -duration 4s -qps 30 -report 10s 2>&1 \
      | awk '/overall_success=/{gsub(/%/,"",$0); if (match($0,/[0-9.]+/)) print substr($0,RSTART,RLENGTH)}' | tail -1)"
  fi
  printf '%s' "${pct:-0}"
}

echo "==> baseline (wait recover)"
BASE=0
for _ in $(seq 1 15); do
  BASE=$(smoke)
  if python3 -c "import sys; sys.exit(0 if float('$BASE' or 0)>=85 else 1)"; then
    break
  fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('$BASE' or 0)>=85 else 1)" || fail "baseline $BASE%"
ok "baseline ${BASE}%"

echo "==> F14 chmod a-w data/storage-1 + restart"
"$SKV" kill storage-1 2>/dev/null || true
sleep 0.5
chmod a-w "$DATA1" 2>/dev/null || chmod -R a-w "$DATA1"
# 尝试重启：可能失败或半活；多数派应仍可写
set +e
"$SKV" restart storage-1 >/tmp/it_disk_restart.txt 2>&1
set -e
sleep 3
DOWN=$(smoke)
echo "majority smoke during readonly peer≈${DOWN}%"
python3 -c "import sys; sys.exit(0 if float('$DOWN')>=70 else 1)" \
  || fail "majority should still serve (>=70%), got ${DOWN}%"
ok "F14 majority serves while storage-1 readonly"

echo "==> restore permissions + restart catch-up"
"$SKV" kill storage-1 2>/dev/null || true
chmod -R u+w "$DATA1" 2>/dev/null || true
find "$DATA1" -name LOCK -delete 2>/dev/null || true
"$SKV" restart storage-1 >/dev/null 2>&1 || true
REC=0
for _ in $(seq 1 20); do
  sleep 2
  REC=$(smoke)
  echo "  recover smoke≈${REC}%"
  if python3 -c "import sys; sys.exit(0 if float('$REC' or 0)>=70 else 1)"; then
    break
  fi
done
python3 -c "import sys; sys.exit(0 if float('$REC' or 0)>=70 else 1)" || fail "recover $REC%"
ok "F14 recovered ${REC}%"

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
F15=0
for _ in $(seq 1 12); do
  F15=$(smoke)
  if python3 -c "import sys; sys.exit(0 if float('$F15' or 0)>=70 else 1)"; then
    break
  fi
  sleep 2
done
python3 -c "import sys; sys.exit(0 if float('$F15' or 0)>=70 else 1)" || fail "F15 smoke $F15%"
ok "F15 catch-up smoke ${F15}%"

echo
echo "ALL PASS: it-disk-fault"
