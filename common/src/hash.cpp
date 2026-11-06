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

// @file hash.cpp
// 哈希工具编译单元：将 header-only 的 fnv1a64 锚定到本 TU，保证静态/动态链接行为一致。
//
// 逻辑均在 hash.hpp；本文件无业务代码。

#include "opskv/hash.hpp"

// Header-only hash helpers; TU kept for linker stability.
namespace opskv {
namespace {
[[maybe_unused]] const auto kHashAnchor = &fnv1a64;
}
}  // namespace opskv
