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

# Generate in-repo Go stubs for Meta/Storage Admin (web / line-rpc).
# KV/DS Go stubs live in openkv-client-go — sync there:
#   (cd ../openkv-client-go && ./scripts/sync-proto-from-openkv.sh && ./scripts/gen.sh)

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/client/go/gen"
mkdir -p "$OUT"

protoc -I "$ROOT/proto" \
  --go_out="$OUT" --go_opt=paths=source_relative \
  "$ROOT/proto/storage/v1/storage.proto" \
  "$ROOT/proto/meta/v1/meta.proto"

if command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  protoc -I "$ROOT/proto" \
    --go-grpc_out="$OUT" --go-grpc_opt=paths=source_relative \
    "$ROOT/proto/storage/v1/storage.proto" \
    "$ROOT/proto/meta/v1/meta.proto" || {
      echo "warn: protoc-gen-go-grpc failed; keep hand-written *_grpc.pb.go if present" >&2
    }
fi

echo "generated Meta/Storage stubs under $OUT"
echo "note: kv/ds Go client stubs → openkv-client-go (see scripts/sync-proto-from-openkv.sh there)"
