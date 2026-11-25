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

// @file skv_frame.hpp
// SKV1 二进制长度前缀帧（与行协议并存），供 Meta/Storage/Raft 共用。
// 帧：magic(4)='SKV1' | type(u8) | shard(u32 LE) | payload_len(u32 LE) | payload

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace openkv::net {

inline constexpr char kFrameMagic[4] = {'S', 'K', 'V', '1'};  // 帧魔数

/* 帧类型：Raft 消息与通用响应。 */
enum class FrameType : uint8_t {
  kVote = 1,             // RequestVote
  kPreVote = 2,          // PreVote
  kAppendEntries = 3,    // AppendEntries
  kInstallSnapshot = 4,  // 整包快照
  kSnapChunk = 5,        // 分片快照
  kResp = 10,            // 通用应答
};

/**
 * 缓冲是否以 SKV1 magic 开头（尚不足以判定完整帧）。
 * @param buf 待检字节
 * @return 是否像二进制帧
 */
inline bool is_binary_frame(std::string_view buf) {
  return buf.size() >= 4 && buf[0] == 'S' && buf[1] == 'K' && buf[2] == 'V' && buf[3] == '1';
}

/**
 * 编码一帧（小端 shard / len）。
 * @param type 帧类型
 * @param shard Region / 分片 id（Meta 单组可为 0）
 * @param payload 载荷
 * @return 完整帧字节
 */
inline std::string encode_frame(FrameType type, uint32_t shard, std::string_view payload) {
  std::string out;
  out.resize(4 + 1 + 4 + 4 + payload.size());
  std::memcpy(out.data(), kFrameMagic, 4);
  out[4] = static_cast<char>(type);
  std::memcpy(out.data() + 5, &shard, 4);
  uint32_t len = static_cast<uint32_t>(payload.size());
  std::memcpy(out.data() + 9, &len, 4);
  if (!payload.empty()) {
    std::memcpy(out.data() + 13, payload.data(), payload.size());
  }
  return out;
}

/* 解析后的帧头 + payload 视图（指向原缓冲，勿悬空）。 */
struct DecodedFrame {
  FrameType type{FrameType::kResp};  // 帧类型
  uint32_t shard{0};                 // Region / 分片 id
  std::string_view payload;          // 载荷视图
};

/**
 * 解析帧头与完整 payload；需要至少 13 字节头；不完整或非法返回 false。
 * @param buf 待解析字节缓冲
 * @param out 写出解析结果；不可空
 * @param total_size 写出完整帧字节数（头+payload）
 * @return 是否已拿到完整合法帧
 */
inline bool decode_frame_header(std::string_view buf, DecodedFrame* out, size_t* total_size) {
  if (buf.size() < 13 || !is_binary_frame(buf)) {
    return false;
  }
  out->type = static_cast<FrameType>(static_cast<uint8_t>(buf[4]));
  std::memcpy(&out->shard, buf.data() + 5, 4);
  uint32_t len = 0;
  std::memcpy(&len, buf.data() + 9, 4);
  *total_size = 13 + len;
  if (buf.size() < *total_size) {
    return false;
  }
  out->payload = buf.substr(13, len);
  return true;
}

}  // namespace openkv::net
