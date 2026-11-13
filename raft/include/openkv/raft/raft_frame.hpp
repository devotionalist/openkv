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

// @file raft_frame.hpp
// 职责：历史 Raft 二进制帧 API 的兼容别名；实现已迁到 openkv::net::skv_frame。
// 架构：无逻辑，仅 using 导出，避免旧 include 路径断裂。
// 协作：新代码直接 #include "openkv/net/skv_frame.hpp"。

#pragma once

#include "openkv/net/skv_frame.hpp"

namespace openkv::raft {

using net::DecodedFrame;
using net::FrameType;
using net::decode_frame_header;
using net::encode_frame;
using net::is_binary_frame;
using net::kFrameMagic;

}  // namespace openkv::raft
