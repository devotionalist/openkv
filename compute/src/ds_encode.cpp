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

// @file ds_encode.cpp
// DS blob 编解码实现。

#include "openkv/compute/ds_encode.hpp"

#include <algorithm>
#include <cstring>

namespace openkv::compute::ds {
namespace {

constexpr unsigned char kMarker[] = {0x00, 0x01, 'd', 's', 0x00};
constexpr char kMagic[] = {'S', 'K', 'V', 'D'};

void put_u32_be(std::string* buf, uint32_t v) {
  const char n[4] = {static_cast<char>((v >> 24) & 0xff), static_cast<char>((v >> 16) & 0xff),
                     static_cast<char>((v >> 8) & 0xff), static_cast<char>(v & 0xff)};
  buf->append(n, 4);
}

uint32_t get_u32_be(std::string_view& r) {
  if (r.size() < 4) throw CorruptError();
  const uint32_t v = (static_cast<uint32_t>(static_cast<unsigned char>(r[0])) << 24) |
                     (static_cast<uint32_t>(static_cast<unsigned char>(r[1])) << 16) |
                     (static_cast<uint32_t>(static_cast<unsigned char>(r[2])) << 8) |
                     static_cast<uint32_t>(static_cast<unsigned char>(r[3]));
  r.remove_prefix(4);
  return v;
}

void put_bytes(std::string* buf, std::string_view b) {
  put_u32_be(buf, static_cast<uint32_t>(b.size()));
  buf->append(b.data(), b.size());
}

std::string get_bytes(std::string_view& r) {
  const uint32_t ln = get_u32_be(r);
  if (ln > (64u << 20) || r.size() < ln) throw CorruptError();
  std::string out(r.substr(0, ln));
  r.remove_prefix(ln);
  return out;
}

std::string wrap(char typ, std::string_view payload) {
  std::string out;
  out.reserve(5 + payload.size());
  out.append(kMagic, 4);
  out.push_back(typ);
  out.append(payload.data(), payload.size());
  return out;
}

std::pair<char, std::string_view> unwrap(std::string_view blob) {
  if (blob.size() < 5 || std::memcmp(blob.data(), kMagic, 4) != 0) throw CorruptError();
  return {blob[4], blob.substr(5)};
}

/// 空 blob → 空 payload（与 Go checkType 一致）；非空则校验类型。
std::string_view check_type(std::string_view blob, char want) {
  if (blob.empty()) return {};
  auto [typ, payload] = unwrap(blob);
  if (typ != want) throw WrongTypeError(typ, want);
  return payload;
}

void put_f64_be(std::string* buf, double v) {
  static_assert(sizeof(double) == 8, "need IEEE754 double");
  uint64_t bits = 0;
  std::memcpy(&bits, &v, 8);
  char n[8];
  for (int i = 0; i < 8; ++i) {
    n[i] = static_cast<char>((bits >> ((7 - i) * 8)) & 0xff);
  }
  buf->append(n, 8);
}

double get_f64_be(std::string_view& r) {
  if (r.size() < 8) throw CorruptError();
  uint64_t bits = 0;
  for (int i = 0; i < 8; ++i) {
    bits = (bits << 8) | static_cast<unsigned char>(r[i]);
  }
  r.remove_prefix(8);
  double v = 0;
  std::memcpy(&v, &bits, 8);
  return v;
}

}  // namespace

std::string meta_key(std::string_view ukey) {
  std::string out;
  out.reserve(ukey.size() + sizeof(kMarker));
  out.append(ukey.data(), ukey.size());
  out.append(reinterpret_cast<const char*>(kMarker), sizeof(kMarker));
  return out;
}

std::string encode_hash(const Hash& h) {
  std::string payload;
  put_u32_be(&payload, static_cast<uint32_t>(h.size()));
  for (const auto& [k, v] : h) {
    put_bytes(&payload, k);
    put_bytes(&payload, v);
  }
  return wrap(kTypeHash, payload);
}

Hash decode_hash(std::string_view blob) {
  auto payload = check_type(blob, kTypeHash);
  if (payload.empty() && blob.empty()) return {};
  Hash h;
  if (blob.empty()) return h;
  const uint32_t cnt = get_u32_be(payload);
  for (uint32_t i = 0; i < cnt; ++i) {
    auto fk = get_bytes(payload);
    auto fv = get_bytes(payload);
    h.emplace(std::move(fk), std::move(fv));
  }
  return h;
}

std::string encode_set(const Set& s) {
  std::string payload;
  put_u32_be(&payload, static_cast<uint32_t>(s.size()));
  for (const auto& m : s) put_bytes(&payload, m);
  return wrap(kTypeSet, payload);
}

Set decode_set(std::string_view blob) {
  auto payload = check_type(blob, kTypeSet);
  if (blob.empty()) return {};
  Set s;
  const uint32_t cnt = get_u32_be(payload);
  for (uint32_t i = 0; i < cnt; ++i) s.insert(get_bytes(payload));
  return s;
}

std::string encode_list(const List& l) {
  std::string payload;
  put_u32_be(&payload, static_cast<uint32_t>(l.size()));
  for (const auto& v : l) put_bytes(&payload, v);
  return wrap(kTypeList, payload);
}

List decode_list(std::string_view blob) {
  auto payload = check_type(blob, kTypeList);
  if (blob.empty()) return {};
  List l;
  const uint32_t cnt = get_u32_be(payload);
  l.reserve(cnt);
  for (uint32_t i = 0; i < cnt; ++i) l.push_back(get_bytes(payload));
  return l;
}

std::optional<std::pair<size_t, size_t>> normalize_range(int64_t n, int64_t start, int64_t stop) {
  if (n <= 0) return std::nullopt;
  if (start < 0) start = n + start;
  if (stop < 0) stop = n + stop;
  if (start < 0) start = 0;
  if (stop < 0) return std::nullopt;
  if (start >= n) return std::nullopt;
  if (stop >= n) stop = n - 1;
  if (start > stop) return std::nullopt;
  return std::make_pair(static_cast<size_t>(start), static_cast<size_t>(stop) + 1);
}

std::vector<ZEntry> z_sorted(const ZSet& z) {
  std::vector<ZEntry> ents;
  ents.reserve(z.size());
  for (const auto& [m, s] : z) ents.push_back({m, s});
  std::sort(ents.begin(), ents.end(), [](const ZEntry& a, const ZEntry& b) {
    if (a.score != b.score) return a.score < b.score;
    return a.member < b.member;
  });
  return ents;
}

std::string encode_zset(const ZSet& z) {
  auto ents = z_sorted(z);
  std::string payload;
  put_u32_be(&payload, static_cast<uint32_t>(ents.size()));
  for (const auto& e : ents) {
    put_bytes(&payload, e.member);
    put_f64_be(&payload, e.score);
  }
  return wrap(kTypeZSet, payload);
}

ZSet decode_zset(std::string_view blob) {
  auto payload = check_type(blob, kTypeZSet);
  if (blob.empty()) return {};
  ZSet z;
  const uint32_t cnt = get_u32_be(payload);
  for (uint32_t i = 0; i < cnt; ++i) {
    auto m = get_bytes(payload);
    const double s = get_f64_be(payload);
    z.emplace(std::move(m), s);
  }
  return z;
}

}  // namespace openkv::compute::ds
