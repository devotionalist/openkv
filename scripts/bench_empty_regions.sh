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

# O3：单节点空 Region 批量 ENSURE 烟雾（默认 1000；万级可 SKV_N=10000）。
# 用法：先启动单 Storage，再：
#   STORAGE=127.0.0.1:9100 SKV_N=1000 ./scripts/bench_empty_regions.sh
set -euo pipefail
STORAGE="${STORAGE:-127.0.0.1:9100}"
N="${SKV_N:-1000}"
NODE_ID="${NODE_ID:-storage-0}"

tcp() {
  local req="$1"
  python3 - <<PY
import socket
s=socket.create_connection(("${STORAGE%:*}", int("${STORAGE##*:}")), timeout=5)
s.sendall(b"""$req\n""")
print(s.recv(4096).decode().strip())
s.close()
PY
}

echo "bench empty regions n=$N storage=$STORAGE"
start=$(date +%s)
ok=0
for ((i=0; i<N; i++)); do
  r=$(tcp "ADMIN_ENSURE_REGION $i ${NODE_ID}@${STORAGE}" || true)
  if [[ "$r" == OK* ]]; then ok=$((ok+1)); fi
  if (( i % 100 == 99 )); then echo "  progress $((i+1))/$N ok=$ok"; fi
done
end=$(date +%s)
echo "done ok=$ok/${N} elapsed=$((end-start))s"
