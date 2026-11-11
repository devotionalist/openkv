#!/usr/bin/env bash
# 压测前把 key 空间切成 N 片 Region，拉开多 Raft 管道。
# 用法:
#   SKV_META=127.0.0.1:18500 SKV_STORAGES=127.0.0.1:19100 \
#     bash scripts/bench-multi-region.sh [prefix] [regions] [keys]
# 例（k3s port-forward meta/storage 后）:
#   bash scripts/bench-multi-region.sh bench-mr 4 200
#   集群内: examples/bench -prefix bench-mr -keys 200 -mode put ...
#
# 环境:
#   SKV / SKV_META / SKV_STORAGES — 见 docs/ops/SKV_CLI.md
#   SEED=1 — 先 skv put 铺 keys（慢；默认 0，分裂点不必真实存在）
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKV="${SKV:-$ROOT/build/tools/skv}"
PREFIX="${1:-bench-mr}"
REGIONS="${2:-4}"
KEYS="${3:-200}"
SEED="${SEED:-0}"

if [[ ! -x "$SKV" ]]; then
  echo "missing skv at $SKV (build tools/skv first)" >&2
  exit 1
fi
if [[ "$REGIONS" -lt 2 ]]; then
  echo "regions must be >= 2" >&2
  exit 1
fi

if [[ "$SEED" == "1" ]]; then
  echo "==> seed $KEYS keys under $PREFIX/ via skv put"
  for ((i = 0; i < KEYS; i++)); do
    printf -v k "%s/%08d" "$PREFIX" "$i"
    "$SKV" put "$k" "seed" >/dev/null || true
  done
else
  echo "==> skip seed (SEED=0); split keys are lexical boundaries only"
fi

echo "==> split into ~$REGIONS regions"
step=$((KEYS / REGIONS))
if [[ "$step" -lt 1 ]]; then step=1; fi
rid=0
for ((r = 1; r < REGIONS; r++)); do
  idx=$((r * step))
  if [[ "$idx" -ge KEYS ]]; then idx=$((KEYS - 1)); fi
  printf -v split_key "%s/%08d" "$PREFIX" "$idx"
  echo "  split region $rid at $split_key"
  out="$("$SKV" split "$rid" "$split_key" 2>&1 || true)"
  echo "  $out"
  if echo "$out" | grep -q '^OK SPLIT'; then
    right="$(echo "$out" | awk '/^OK SPLIT/{print $4; exit}')"
    if [[ -n "${right:-}" ]]; then rid="$right"; fi
  fi
  sleep 0.2
done

echo "==> route snapshot"
"$SKV" route 2>/dev/null || true
n="$("$SKV" route 2>/dev/null | awk '{print $4; exit}' || echo 0)"
echo "route region_count field≈$n (expect >= $REGIONS)"
echo "done: use bench -prefix $PREFIX -keys $KEYS"
echo "tip: 若 put 报 key_not_in_region，重启 storage 以 recover 区间元数据"
