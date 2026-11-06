#!/usr/bin/env bash
# kind + Helm 冒烟：构建镜像 → load → helm install → port-forward → kv-example
# 依赖：docker, kind, helm, kubectl；可选已编好的 .run/kv-example
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLUSTER="${OPSKV_KIND_CLUSTER:-opskv}"
NS=opskv
TAG="${OPSKV_IMAGE_TAG:-dev}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing tool: $1" >&2
    exit 2
  }
}

need docker
need kind
need helm
need kubectl

echo "==> kind cluster: $CLUSTER"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER"
fi
kubectl cluster-info --context "kind-${CLUSTER}" >/dev/null

echo "==> build images"
docker build -f "$ROOT/deploy/Dockerfile" -t "opskv:${TAG}" "$ROOT"
docker build -f "$ROOT/deploy/Dockerfile.gateway" -t "opskv-gateway:${TAG}" "$ROOT"
kind load docker-image "opskv:${TAG}" --name "$CLUSTER"
kind load docker-image "opskv-gateway:${TAG}" --name "$CLUSTER"

echo "==> helm upgrade --install"
helm upgrade --install opskv "$ROOT/deploy/helm/opskv" \
  --namespace "$NS" --create-namespace \
  --set namespace.create=false \
  --set image.repository=opskv \
  --set image.tag="$TAG" \
  --set image.pullPolicy=Never \
  --set gatewayImage.repository=opskv-gateway \
  --set gatewayImage.tag="$TAG" \
  --set gatewayImage.pullPolicy=Never \
  --set persistence.enabled=true \
  --wait --timeout 10m

kubectl -n "$NS" get pods,svc,sts

echo "==> port-forward gateway"
kubectl -n "$NS" port-forward svc/gateway 19000:19000 >/tmp/opskv-pf-gw.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT
sleep 2

EX="${ROOT}/.run/kv-example"
if [[ ! -x "$EX" ]]; then
  if [[ -x "$ROOT/client/go/kv-example" ]]; then
    EX="$ROOT/client/go/kv-example"
  else
    (cd "$ROOT/client/go" && go build -o "$ROOT/.run/kv-example" ./examples/kv)
    EX="$ROOT/.run/kv-example"
  fi
fi

echo "==> kv-example"
"$EX" 127.0.0.1:19000
echo "k8s-kind-smoke: PASS"
