#!/usr/bin/env bash
# 本机验证 Raft 模块故障矩阵（内存多节点，不启 OpenKV）。
# 用法：
#   bash scripts/raft-fault-local.sh           # 跑一轮全套
#   bash scripts/raft-fault-local.sh 10        # 全套重复 10 轮（压偶然双主/卡死）
#   bash scripts/raft-fault-local.sh 5 X*      # 只重复混沌 X* 用例
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/build/tests/openkv_unit_tests"
REPEAT="${1:-1}"
FILTER="${2:-RaftFaultScenario*}"
if [[ ! -x "$BIN" ]]; then
  echo "missing $BIN — build first: cmake --build build --target openkv_unit_tests"
  exit 1
fi
echo "== raft-fault-local filter=$FILTER repeat=$REPEAT =="
exec "$BIN" --gtest_filter="$FILTER" --gtest_repeat="$REPEAT" --gtest_break_on_failure
