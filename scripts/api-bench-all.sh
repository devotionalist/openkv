#!/usr/bin/env bash
# 集群内全客户端 API 压测（kv-bench -mode all）。
# 用法：
#   ./scripts/api-bench-all.sh
#   WORKERS=8 DURATION=5s KEYS=50 ADDR=gateway.openkv.svc.cluster.local:19000 ./scripts/api-bench-all.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NS="${NS:-openkv}"
ADDR="${ADDR:-gateway.openkv.svc.cluster.local:19000}"
WORKERS="${WORKERS:-8}"
DURATION="${DURATION:-5s}"
WARMUP="${WARMUP:-500ms}"
KEYS="${KEYS:-50}"
VSIZE="${VSIZE:-64}"
POD="${POD:-okv-bench}"
BIN_HOST="${BIN_HOST:-$ROOT/.run/kv-bench-linux}"
OUT="${OUT:-$ROOT/.run/api-bench-all.SUMMARY.txt}"

mkdir -p "$ROOT/.run"

echo "==> build kv-bench-linux"
(cd "$ROOT/client/go" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o "$BIN_HOST" ./examples/bench)

if ! kubectl -n "$NS" get pod "$POD" >/dev/null 2>&1; then
  echo "==> create pod/$POD"
  kubectl -n "$NS" run "$POD" --restart=Never --image=alpine:3.20 -- sleep 3600
fi
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=90s

echo "==> copy bench → $POD"
kubectl -n "$NS" cp "$BIN_HOST" "$POD:/kv-bench"
kubectl -n "$NS" exec "$POD" -- chmod +x /kv-bench

echo "==> -mode all workers=$WORKERS duration=$DURATION keys=$KEYS addr=$ADDR"
set +e
kubectl -n "$NS" exec "$POD" -- /kv-bench \
  -addr "$ADDR" \
  -mode all \
  -workers "$WORKERS" \
  -duration "$DURATION" \
  -warmup "$WARMUP" \
  -keys "$KEYS" \
  -vsize "$VSIZE" | tee "$OUT"
EC=${PIPESTATUS[0]}
set -e

echo
echo "==> SUMMARY saved: $OUT (exit=$EC)"
exit "$EC"
