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
// P1：Raft 二进制帧 — 实现已迁到 opskv::net::skv_frame（跨模块共用）。

#pragma once

#include "opskv/net/skv_frame.hpp"

namespace opskv::raft {

using net::DecodedFrame;
using net::FrameType;
using net::decode_frame_header;
using net::encode_frame;
using net::is_binary_frame;
using net::kFrameMagic;

}  // namespace opskv::raft
