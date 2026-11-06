#!/usr/bin/env bash
# OpsKV 容器入口：根据 OPSKV_ROLE / POD_NAME 生成 conf 并启动。
# 环境变量（Helm 注入）：
#   OPSKV_ROLE=meta|storage
#   POD_NAME=meta-0 / storage-1
#   OPSKV_NAMESPACE, OPSKV_META_HEADLESS, OPSKV_STORAGE_HEADLESS
#   OPSKV_META_REPLICAS, OPSKV_STORAGE_REPLICAS
#   OPSKV_META_PORT=8500, OPSKV_STORAGE_PORT=9100
#   OPSKV_SHARD_COUNT, OPSKV_REPLICA_FACTOR
set -euo pipefail

# 显式 command（Compose/调试）优先
if [[ $# -gt 0 ]]; then
  exec "$@"
fi

ROLE="${OPSKV_ROLE:-storage}"
NS="${OPSKV_NAMESPACE:-opskv}"
META_HS="${OPSKV_META_HEADLESS:-meta-headless}"
STOR_HS="${OPSKV_STORAGE_HEADLESS:-storage-headless}"
META_N="${OPSKV_META_REPLICAS:-3}"
STOR_N="${OPSKV_STORAGE_REPLICAS:-3}"
META_PORT="${OPSKV_META_PORT:-8500}"
STOR_PORT="${OPSKV_STORAGE_PORT:-9100}"
SHARDS="${OPSKV_SHARD_COUNT:-8}"
RF="${OPSKV_REPLICA_FACTOR:-3}"
CONF_DIR="${OPSKV_CONF_DIR:-/etc/opskv}"
DATA_DIR="${OPSKV_DATA_DIR:-/data}"
CONF="${CONF_DIR}/node.conf"

mkdir -p "$CONF_DIR" "$DATA_DIR"

ordinal_from_pod() {
  local name="${1:-}"
  if [[ "$name" =~ -([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "0"
  fi
}

fqdn() {
  # $1=pod-name $2=headless-svc
  echo "${1}.${2}.${NS}.svc.cluster.local"
}

peer_csv() {
  # $1=prefix $2=headless $3=count $4=port
  local prefix="$1" hs="$2" n="$3" port="$4"
  local out="" i
  for ((i = 0; i < n; i++)); do
    local id="${prefix}-${i}"
    local addr
    addr="$(fqdn "$id" "$hs"):${port}"
    if [[ -n "$out" ]]; then out+=","; fi
    out+="${id}@${addr}"
  done
  echo "$out"
}

POD_NAME="${POD_NAME:-${HOSTNAME:-storage-0}}"
ORDINAL="$(ordinal_from_pod "$POD_NAME")"

if [[ "$ROLE" == "meta" ]]; then
  NODE_ID="meta-${ORDINAL}"
  ADV="$(fqdn "$NODE_ID" "$META_HS"):${META_PORT}"
  META_PEERS="$(peer_csv meta "$META_HS" "$META_N" "$META_PORT")"
  STOR_PEERS="$(peer_csv storage "$STOR_HS" "$STOR_N" "$STOR_PORT")"
  cat >"$CONF" <<EOF
node_id=${NODE_ID}
listen_addr=0.0.0.0:${META_PORT}
meta_addr=0.0.0.0:${META_PORT}
data_dir=${DATA_DIR}
shard_count=${SHARDS}
replica_factor=${RF}
raft_election_timeout_ms=1500
raft_heartbeat_ms=100
raft_compact_threshold=256
meta_peers=${META_PEERS}
storage_peers=${STOR_PEERS}
compute_peers=
EOF
  echo "starting meta_server id=${NODE_ID} adv=${ADV}"
  exec /opt/opskv/meta_server "$CONF"
fi

# storage
# 对齐 conf/storage.conf vs storage-3.conf：
#   ordinal < RF  → 初始投票组：bootstrap_regions=0，raft_peers=前 RF 个
#   ordinal >= RF → 待加入：bootstrap_regions 空，raft_peers=仅自己（等 JOIN+PLACE/balance）
NODE_ID="storage-${ORDINAL}"
ADV="$(fqdn "$NODE_ID" "$STOR_HS"):${STOR_PORT}"
# meta_addr：连 headless 的任意/首副本；客户端应跟随 redirect
META_ADDR="$(fqdn meta-0 "$META_HS"):${META_PORT}"
BOOTSTRAP_N="$RF"
if [[ "$BOOTSTRAP_N" -gt "$STOR_N" ]]; then
  BOOTSTRAP_N="$STOR_N"
fi
if [[ "$ORDINAL" -lt "$BOOTSTRAP_N" ]]; then
  RAFT_PEERS="$(peer_csv storage "$STOR_HS" "$BOOTSTRAP_N" "$STOR_PORT")"
  BOOTSTRAP_REGIONS="0"
else
  RAFT_PEERS="${NODE_ID}@${ADV}"
  BOOTSTRAP_REGIONS=""
fi
cat >"$CONF" <<EOF
node_id=${NODE_ID}
listen_addr=0.0.0.0:${STOR_PORT}
advertise_addr=${ADV}
meta_addr=${META_ADDR}
data_dir=${DATA_DIR}
shard_count=${SHARDS}
raft_election_timeout_ms=1500
raft_heartbeat_ms=100
raft_peers=${RAFT_PEERS}
bootstrap_regions=${BOOTSTRAP_REGIONS}
EOF
echo "starting storage_node id=${NODE_ID} adv=${ADV} meta=${META_ADDR} bootstrap_regions=${BOOTSTRAP_REGIONS:-<empty>} raft_peers=${RAFT_PEERS}"
if [[ "${OPSKV_CONF_ONLY:-}" == "1" ]]; then
  cat "$CONF"
  exit 0
fi
exec /opt/opskv/storage_node "$CONF"
