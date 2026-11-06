#!/usr/bin/env bash
# 门禁：storage 入口 conf 区分 bootstrap 投票组 vs 待加入（对齐 conf/storage-3.conf）
# 用法：bash scripts/it-entrypoint-storage-conf.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EP="$ROOT/deploy/docker-entrypoint.sh"
TMP="${TMPDIR:-/tmp}/opskv-entrypoint-conf-$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

run_conf() {
  local pod="$1"
  OPSKV_ROLE=storage \
    POD_NAME="$pod" \
    OPSKV_NAMESPACE=opskv \
    OPSKV_META_REPLICAS=3 \
    OPSKV_STORAGE_REPLICAS=4 \
    OPSKV_REPLICA_FACTOR=3 \
    OPSKV_CONF_DIR="$TMP/$pod" \
    OPSKV_DATA_DIR="$TMP/$pod-data" \
    OPSKV_CONF_ONLY=1 \
    bash "$EP" >"$TMP/$pod.out"
}

run_conf storage-0
run_conf storage-3

grep -q 'bootstrap_regions=0' "$TMP/storage-0.out"
grep -q 'storage-0@' "$TMP/storage-0.out"
grep -q 'storage-2@' "$TMP/storage-0.out"
! grep -q 'storage-3@' "$TMP/storage-0.out"

# 待加入：空 bootstrap；peers 仅自己
grep -q 'bootstrap_regions=$' "$TMP/storage-3.out" || grep -q 'bootstrap_regions=\s*$' "$TMP/storage-3.out"
grep -q 'raft_peers=storage-3@storage-3.storage-headless.opskv.svc.cluster.local:9100' "$TMP/storage-3.out"
! grep -q 'storage-0@' "$TMP/storage-3.out"

echo "it-entrypoint-storage-conf: PASS"
