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

# Gateway Scan / ListPrefix 冒烟（无旁路索引）
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
GATEWAY="${GATEWAY:-127.0.0.1:19000}"
GW_HOST="${GATEWAY%%:*}"
GW_PORT="${GATEWAY##*:}"
python3 -c "import socket;s=socket.create_connection(('$GW_HOST',$GW_PORT),1);s.close()" 2>/dev/null \
  || { echo "FAIL: gateway $GATEWAY down"; exit 1; }
mkdir -p .run
(cd client/go && go build -o "$ROOT/.run/scan-smoke" ./examples/scan)
"$ROOT/.run/scan-smoke" "$GATEWAY"
echo "ALL PASS: it-scan"
