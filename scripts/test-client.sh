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

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${RUN_DIR:-$ROOT/.run}"
ADDR="${1:-127.0.0.1:19000}"

echo "==> wait gRPC $ADDR"
for i in $(seq 1 60); do
  if nc -z "${ADDR%%:*}" "${ADDR##*:}" 2>/dev/null; then
    break
  fi
  sleep 0.25
  if [[ $i -eq 60 ]]; then
    echo "gateway not ready; check $RUN_DIR/gateway.log"
    tail -n 40 "$RUN_DIR/gateway.log" 2>/dev/null || true
    exit 1
  fi
done

echo "==> example client Put/Get"
"$RUN_DIR/kv-example" "$ADDR"

echo "==> extended client checks"
cd "$ROOT/client/go"
go run ./examples/e2e -addr "$ADDR"

echo "client tests OK"
