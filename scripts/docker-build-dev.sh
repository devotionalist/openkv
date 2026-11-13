#!/usr/bin/env bash
# 本地/Colima 开发镜像：先保证 builder，再编 openkv / gateway。
# 用法：
#   ./scripts/docker-build-dev.sh              # builder(如缺) + openkv + gateway
#   ./scripts/docker-build-dev.sh builder      # 只重建基础镜像
#   ./scripts/docker-build-dev.sh openkv       # 只编 meta/storage
#   ./scripts/docker-build-dev.sh gateway      # 只编 gateway
#   ./scripts/docker-build-dev.sh all --no-cache-builder
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILDER_IMAGE="${OPENKV_BUILDER_IMAGE:-openkv-builder:dev}"
export DOCKER_BUILDKIT=1

need_builder() {
  docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1
}

build_builder() {
  local extra=()
  if [[ "${1:-}" == "--no-cache" ]]; then
    extra+=(--no-cache)
  fi
  echo "==> building $BUILDER_IMAGE"
  docker build "${extra[@]}" -f deploy/Dockerfile.builder -t "$BUILDER_IMAGE" .
}

build_openkv() {
  echo "==> building openkv:dev (FROM $BUILDER_IMAGE)"
  docker build --build-arg "BUILDER_IMAGE=$BUILDER_IMAGE" \
    -f deploy/Dockerfile -t openkv:dev .
}

build_gateway() {
  echo "==> building openkv-gateway:dev (FROM $BUILDER_IMAGE)"
  docker build --build-arg "BUILDER_IMAGE=$BUILDER_IMAGE" \
    -f deploy/Dockerfile.gateway -t openkv-gateway:dev .
}

target="${1:-all}"
shift || true
no_cache_builder=0
for a in "$@"; do
  case "$a" in
    --no-cache-builder) no_cache_builder=1 ;;
  esac
done

case "$target" in
  builder)
    if [[ "$no_cache_builder" -eq 1 ]]; then build_builder --no-cache; else build_builder; fi
    ;;
  openkv)
    need_builder || build_builder
    build_openkv
    ;;
  gateway)
    need_builder || build_builder
    build_gateway
    ;;
  all)
    if [[ "$no_cache_builder" -eq 1 ]] || ! need_builder; then
      if [[ "$no_cache_builder" -eq 1 ]]; then build_builder --no-cache; else build_builder; fi
    else
      echo "==> reuse $BUILDER_IMAGE"
    fi
    build_openkv
    build_gateway
    ;;
  *)
    echo "usage: $0 [all|builder|openkv|gateway] [--no-cache-builder]" >&2
    exit 2
    ;;
esac

echo "OK"
docker images --format 'table {{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedSince}}\t{{.Size}}' \
  | awk 'NR==1 || $1 ~ /^openkv/'
