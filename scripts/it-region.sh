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

# I30–I36：Split → place scatter → 双侧 key 硬可读 → merge。
# 路由一律走 `skv route`（兼容 gRPC Meta，不直接打 TCP 行协议）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SKV="${SKV:-./build/tools/skv}"
WEB="${WEB:-http://127.0.0.1:19100}"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

assert_get() {
  local key="$1" want="$2"
  local out
  out="$("$SKV" get "$key" 2>&1 || true)"
  echo "$out" | grep -q "$want" || fail "get $key want=$want got=$out"
}

region_count() {
  "$SKV" route 2>/dev/null | python3 -c 'import sys,re; t=sys.stdin.read(); m=re.search(r"OK ROUTE \d+ (\d+)", t); print(m.group(1) if m else 0)'
}

# 解析 start_key==m 的 region id（route token: id:starthex:endhex:...）
# 注意：不可对 python 用 heredoc+管道（heredoc 会占掉 stdin）。
right_region_id() {
  local route
  route="$("$SKV" route 2>/dev/null)"
  ROUTE_LINE="$route" python3 -c '
import os,sys
line=os.environ.get("ROUTE_LINE","").strip()
parts=line.split()
for tok in parts[5:]:
  cols=tok.split(":")
  if len(cols)<4: continue
  try:
    start=bytes.fromhex(cols[1]).decode() if cols[1] else ""
  except Exception:
    start=""
  if start=="m":
    print(cols[0]); break
else:
  sys.stderr.write("no right region start=m in: "+line+"\n"); sys.exit(1)
'
}

peers_of() {
  local rid="$1" route
  route="$("$SKV" route 2>/dev/null)"
  RID="$rid" ROUTE_LINE="$route" python3 -c '
import os,re
rid=os.environ["RID"]
line=os.environ.get("ROUTE_LINE","")
for tok in line.split()[5:]:
  if not tok.startswith(rid+":"): continue
  idx=tok.find(",")
  if idx<0: continue
  ids=[]
  for p in tok[idx+1:].split(","):
    m=re.match(r"(storage-\d+)@", p)
    if m: ids.append(m.group(1))
  print(" ".join(ids)); break
'
}

"$SKV" status >/dev/null || fail "cluster down"

# 多 Region 时先 merge 回 1，并确认全 keyspace 可写（避免历史 merge 毒化）
RC=$(region_count)
if [[ "$RC" != "1" ]]; then
  echo "==> coalesce regions ($RC → 1) before scatter test"
  for _ in $(seq 1 10); do
    RC=$(region_count)
    [[ "$RC" == "1" ]] && break
    # 取两个 region id
    IDS=$("$SKV" route 2>/dev/null | python3 -c 'import sys,re; t=sys.stdin.read();
ids=[]
for tok in t.split()[5:]:
  ids.append(tok.split(":")[0])
print(" ".join(ids[:2]) if len(ids)>=2 else "")')
    set -- $IDS
    [[ $# -ge 2 ]] || break
    echo "  merge $1 $2"
    "$SKV" merge "$1" "$2" >/dev/null 2>&1 || true
    sleep 1
  done
fi
RC=$(region_count)
[[ "$RC" == "1" ]] || fail "need 1 region to start scatter test, got $RC"

# 探针：合并后高位 key 必须可写（否则 Storage 范围未抬到 ∞）
if ! "$SKV" put mmm probe-mid >/dev/null 2>&1; then
  fail "put mmm failed after coalesce — Storage range likely stale; SKV_FRESH=1 skv start"
fi
"$SKV" del mmm >/dev/null 2>&1 || true

echo "==> seed aaa/mmm/zzz"
"$SKV" put aaa left-val >/dev/null
"$SKV" put mmm mid-val >/dev/null
"$SKV" put zzz right-val >/dev/null
assert_get aaa left-val
assert_get mmm mid-val
assert_get zzz right-val

echo "==> I30 split region 0 at m"
SPLIT_OUT=$("$SKV" split 0 m 2>&1 || true)
echo "$SPLIT_OUT"
for _ in $(seq 1 40); do
  RC=$(region_count)
  [[ "$RC" -ge 2 ]] && break
  sleep 0.3
done
RC=$(region_count)
[[ "$RC" -ge 2 ]] || fail "expect >=2 regions, got $RC"
if ! echo "$SPLIT_OUT" | grep -q 'split -> OK'; then
  "$SKV" split 0 m 2>&1 || true
  sleep 1
fi
assert_get aaa left-val
assert_get mmm mid-val
assert_get zzz right-val
ok "I30 split both sides readable"

RIGHT=$(right_region_id) || fail "right region"
ok "right region=$RIGHT"

echo "==> ensure storage-3/4/5 Active"
for n in storage-3 storage-4 storage-5; do
  for _ in $(seq 1 40); do
    ST=$("$SKV" store-status "$n" 2>&1 || "$SKV" status 2>&1 || true)
    if echo "$ST" | grep -qiE "$n.*Active|state=1|Active"; then break; fi
    "$SKV" join "$n" >/dev/null 2>&1 || true
    curl -sf -m 1 -X POST "$WEB/api/join-node" -H 'Content-Type: application/json' \
      -d "{\"node_id\":\"$n\"}" >/dev/null 2>&1 || true
    sleep 0.4
  done
done
ok "join attempted for 3/4/5"

echo "==> I31 place scatter R$RIGHT → storage-3,4,5"
PLACE_OUT=$("$SKV" place "$RIGHT" storage-3,storage-4,storage-5 2>&1 || true)
echo "$PLACE_OUT"
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  PEERS=$(peers_of "$RIGHT" || true)
  echo "  peers R$RIGHT: $PEERS"
  if [[ "$PEERS" == *"storage-3"* && "$PEERS" == *"storage-4"* && "$PEERS" == *"storage-5"* ]]; then
    # 恰好三副本且都是 3/4/5
    echo "$PEERS" | python3 -c 'import sys; p=set(sys.stdin.read().split()); assert p=={"storage-3","storage-4","storage-5"}, p'
    break
  fi
  sleep 2
done
PEERS=$(peers_of "$RIGHT")
echo "$PEERS" | python3 -c 'import sys; p=set(sys.stdin.read().split()); assert p=={"storage-3","storage-4","storage-5"}, p' \
  || fail "scatter peers want {3,4,5} got: $PEERS"

assert_get zzz right-val
assert_get aaa left-val
assert_get mmm mid-val
"$SKV" put zzz2 after-scatter >/dev/null
assert_get zzz2 after-scatter
ok "I31 scatter both halves readable"

echo "==> I33 align peers then merge 0 + $RIGHT"
# 跨不同 peer 集合直接 merge 易丢右半；先把左 Region place 到与右相同的 3/4/5
ALIGN=$("$SKV" place 0 storage-3,storage-4,storage-5 2>&1 || true)
echo "$ALIGN" | tail -5
sleep 2
MRG=$("$SKV" merge 0 "$RIGHT" 2>&1 || true)
echo "$MRG"
sleep 2
assert_get aaa left-val
assert_get zzz right-val
assert_get zzz2 after-scatter
ok "I33 merge data intact"

echo
echo "ALL PASS: it-region"
