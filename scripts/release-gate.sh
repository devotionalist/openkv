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

# =============================================================================
# 发版门禁：单元 → e2e → it-web / it-region / it-txn → fault-drill（含 F2）
#   make release-gate
# 周跑额外：make integ-extra（磁盘/分区）
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN="${RUN_DIR:-$ROOT/.run}"
LOG="$RUN/release-gate.log"
mkdir -p "$RUN"
: >"$LOG"

step() { echo "==> $*" | tee -a "$LOG"; }

step "Stage 0: unit tests"
cmake --build build -j --target opskv_unit_tests 2>&1 | tee -a "$LOG" | tail -5
ctest --test-dir build --output-on-failure 2>&1 | tee -a "$LOG"
step "unit OK"

step "Stage 1–2: e2e (client Put/Get)"
bash scripts/run-e2e.sh 2>&1 | tee -a "$LOG"
step "e2e OK"

# e2e 结束后集群可能已停；后续 IT 依赖常驻集群 + Web
step "ensure cluster + web for IT"
if ! ./build/tools/skv status >/dev/null 2>&1; then
  SKV_ADMIN=1 bash scripts/start-cluster.sh 2>&1 | tee -a "$LOG"
fi
# it-web 可自启 Web；此处尽量预热
if ! curl -sf -m 1 http://127.0.0.1:19100/api/ping >/dev/null 2>&1; then
  SKV_ADMIN=1 bash -c 'source /dev/null; true'
fi

# Region scatter 须在 Web split/merge 之前（避免 merge 后高位 key 范围毒化）
step "Stage 4: it-region (I30–I36 scatter hard-assert)"
bash scripts/it-region.sh 2>&1 | tee -a "$LOG"
step "it-region OK"

step "Stage 3: it-txn (I20–I22)"
bash scripts/it-txn.sh 2>&1 | tee -a "$LOG"
step "it-txn OK"

step "Stage 6: it-web (I50–I54 + DS)"
bash scripts/it-web.sh 2>&1 | tee -a "$LOG"
step "it-web OK"

step "Stage 5: fault-drill with continuous user R/W (+F2)"
bash scripts/fault-drill.sh 2>&1 | tee -a "$LOG"
step "fault-drill OK"

echo
echo "RELEASE GATE PASSED" | tee -a "$LOG"
echo "log: $LOG"
echo "optional weekly: make integ-extra  # F14 / F3 / F11-approx"
