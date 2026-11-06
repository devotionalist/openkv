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

# I42: List 数据结构验收
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
GATEWAY="${GATEWAY:-127.0.0.1:19000}"
SKV="${SKV:-./build/tools/skv}"
"$SKV" status >/dev/null || { echo "FAIL: cluster down"; exit 1; }
(cd client/go && go build -o "$ROOT/.run/ds-smoke" ./cmd/ds-smoke)
GATEWAY="$GATEWAY" DS_SUITE=list ./.run/ds-smoke
echo "ALL PASS: it-ds-list"
