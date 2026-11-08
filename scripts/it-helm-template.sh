#!/usr/bin/env bash
# Helm chart 模板门禁：不需要集群。
# 用法：bash scripts/it-helm-template.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHART="$ROOT/deploy/helm/openkv"
OUT="${TMPDIR:-/tmp}/openkv-helm-template-$$.yaml"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing tool: $1" >&2
    exit 2
  }
}

need helm

helm lint "$CHART"
helm template openkv "$CHART" \
  --namespace openkv \
  --set namespace.create=true \
  --set persistence.enabled=false \
  >"$OUT"

# 基本断言
grep -q 'kind: StatefulSet' "$OUT"
grep -q 'name: meta' "$OUT"
grep -q 'name: storage' "$OUT"
grep -q 'kind: Deployment' "$OUT"
grep -q 'name: gateway' "$OUT"
grep -q 'stats-listen=0.0.0.0:19001' "$OUT"
grep -q 'name: stats' "$OUT"
grep -q 'advertise_addr\|OPENKV_ROLE' "$OUT" || grep -q 'OPENKV_ROLE' "$OUT"
grep -q 'meta-0.meta-headless.openkv.svc' "$OUT"

# client dry-run（有 kubectl 时）
if command -v kubectl >/dev/null 2>&1; then
  kubectl apply --dry-run=client -f "$OUT" >/dev/null
  echo "kubectl dry-run: OK"
else
  echo "kubectl dry-run: SKIP (no kubectl)"
fi

rm -f "$OUT"
echo "it-helm-template: PASS"
