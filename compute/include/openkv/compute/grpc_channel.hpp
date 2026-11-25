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

// @file grpc_channel.hpp
// 进程级 gRPC Channel / Stub 缓存（Compute 出站共用）。

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <string_view>

#include "meta/v1/meta.grpc.pb.h"
#include "storage/v1/storage.grpc.pb.h"

namespace openkv::compute {

/**
 * 从 "… redirect=<addr> …" 解析重定向地址；无则空串。
 * @param resp 响应正文或错误串
 */
std::string parse_redirect(std::string_view resp);

/**
 * 按 addr 复用 insecure Channel；无则创建并放入进程级缓存。
 * @param addr 对端地址 host:port
 */
std::shared_ptr<grpc::Channel> cached_channel(const std::string& addr);
/**
 * 丢弃该 addr 的缓存 Channel 与全部 stub（传输错误后调用）。
 * @param addr 对端地址 host:port
 */
void drop_cached_channel(const std::string& addr);

/**
 * 设 deadline，并挂上环境 Admin token。
 * @param ctx gRPC ClientContext
 * @param timeout_ms 超时毫秒
 */
void attach_deadline_and_token(grpc::ClientContext* ctx, int timeout_ms);

/**
 * 按 addr 复用 StoreService stub。
 * @param addr Storage 地址
 * @return 共享 stub
 */
std::shared_ptr<::storage::v1::StoreService::Stub> cached_store_stub(const std::string& addr);
/**
 * 按 addr 复用 AdminService stub。
 * @param addr Storage 地址
 * @return 共享 stub
 */
std::shared_ptr<::storage::v1::AdminService::Stub> cached_admin_stub(const std::string& addr);
/**
 * 按 addr 复用 MetaService stub。
 * @param addr Meta 地址
 * @return 共享 stub
 */
std::shared_ptr<::meta::v1::MetaService::Stub> cached_meta_stub(const std::string& addr);

}  // namespace openkv::compute
