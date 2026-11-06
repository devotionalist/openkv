// Copyright 2026 devotionalist
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hash

// FNV-1a 64-bit，与 C++ opskv::fnv1a64 保持一致
func FNV1a64(data []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range data {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func ShardOf(key []byte, shardCount uint32) uint32 {
	if shardCount == 0 {
		return 0
	}
	return uint32(FNV1a64(key) % uint64(shardCount))
}
