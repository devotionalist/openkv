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

// @file gateway_watch.cpp
// Watch：Callback ServerWriteReactor；轮询逻辑跑在 WorkPool。

#include "openkv/compute/gateway_server.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>

namespace openkv::compute {
namespace {

bool contains(std::string_view hay, std::string_view needle) {
  return hay.find(needle) != std::string_view::npos;
}

bool has_prefix(std::string_view s, std::string_view p) {
  return s.size() >= p.size() && s.substr(0, p.size()) == p;
}

class WatchReactor final : public grpc::ServerWriteReactor<kv::v1::WatchEvent> {
 public:
  void enqueue_or_finish(kv::v1::WatchEvent ev) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (cancelled_) return;
      q_.push_back(std::move(ev));
    }
    try_write();
  }

  bool wait_drain() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return cancelled_ || q_.empty(); });
    return !cancelled_;
  }

  void mark_done() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      done_ = true;
    }
    try_write();
  }

  void OnWriteDone(bool ok) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      writing_ = false;
      if (!ok) cancelled_ = true;
    }
    cv_.notify_all();
    if (!ok) {
      Finish(grpc::Status(grpc::StatusCode::CANCELLED, "watch write failed"));
      return;
    }
    try_write();
  }

  void OnCancel() override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      cancelled_ = true;
    }
    cv_.notify_all();
  }

  void OnDone() override { delete this; }

 private:
  void try_write() {
    std::lock_guard<std::mutex> lock(mu_);
    if (writing_ || cancelled_) return;
    if (q_.empty()) {
      if (done_) Finish(grpc::Status::OK);
      return;
    }
    pending_ = std::move(q_.front());
    q_.pop_front();
    writing_ = true;
    StartWrite(&pending_);
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<kv::v1::WatchEvent> q_;
  kv::v1::WatchEvent pending_;
  bool writing_{false};
  bool done_{false};
  bool cancelled_{false};
};

}  // namespace

bool GatewayServer::match_watch(const kv::v1::WatchRequest& req, std::string_view key) const {
  if (!req.key().empty()) return key == req.key();
  if (!req.key_prefix().empty()) return has_prefix(key, req.key_prefix());
  return true;
}

std::vector<uint32_t> GatewayServer::regions_for_watch(const kv::v1::WatchRequest& req) {
  if (!req.key().empty()) return {region_of(req.key())};
  auto t = routes_.snapshot();
  if (t.regions.empty()) return {0};
  std::vector<uint32_t> ids;
  ids.reserve(t.regions.size());
  for (const auto& [id, _] : t.regions) ids.push_back(id);
  return ids;
}

void GatewayServer::WatchBody(grpc::CallbackServerContext* ctx, const kv::v1::WatchRequest& req,
                              const std::function<bool(const kv::v1::WatchEvent&)>& write_ev) {
  uint64_t since = req.start_revision();
  if (since == 0) {
    for (uint32_t rid : regions_for_watch(req)) {
      try {
        with_leader(rid, [&](StoreClient& c) {
          const uint64_t head = c.watch_head(rid);
          if (head > since) since = head;
        });
      } catch (...) {
      }
    }
  }

  if (!req.key().empty()) {
    kv::v1::TxnGetRequest tg_req;
    tg_req.set_key(req.key());
    tg_req.set_start_ts(std::numeric_limits<uint64_t>::max());
    kv::v1::TxnGetResponse tg;
    auto st = TxnGetBody(&tg_req, &tg);
    if (st.ok() && tg.found() && !tg.locked()) {
      uint64_t rev = since;
      if (rev == 0) rev = 1;
      kv::v1::WatchEvent ev;
      ev.set_type(kv::v1::WatchEvent::PUT);
      ev.set_key(req.key());
      ev.set_value(tg.value());
      ev.set_revision(rev);
      if (!write_ev(ev)) return;
    }
  }

  while (ctx && !ctx->IsCancelled()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (ctx->IsCancelled()) break;
    for (uint32_t rid : regions_for_watch(req)) {
      std::vector<WatchEvent> batch;
      uint64_t next = 0;
      try {
        with_leader(rid, [&](StoreClient& c) {
          auto r = c.watch_poll(rid, since, 64);
          batch = std::move(r.first);
          next = r.second;
        });
      } catch (const std::exception& e) {
        if (contains(e.what(), "splitting") || contains(e.what(), "not_leader")) {
          refresh_routes();
        }
        continue;
      }
      for (const auto& ev : batch) {
        if (!match_watch(req, ev.key)) continue;
        if (req.start_revision() > 0 && ev.revision <= req.start_revision()) continue;
        kv::v1::WatchEvent out;
        out.set_type(ev.deleted ? kv::v1::WatchEvent::DELETE : kv::v1::WatchEvent::PUT);
        out.set_key(ev.key);
        out.set_value(ev.value);
        out.set_revision(ev.revision);
        if (!write_ev(out)) return;
      }
      if (next > since) since = next;
    }
  }
}

grpc::ServerWriteReactor<kv::v1::WatchEvent>* GatewayServer::Watch(
    grpc::CallbackServerContext* ctx, const kv::v1::WatchRequest* req) {
  auto* reactor = new WatchReactor();
  note_client_rpc();
  kv::v1::WatchRequest req_copy = *req;
  pool_->post([this, ctx, reactor, req = std::move(req_copy)]() mutable {
    WatchBody(ctx, req, [reactor](const kv::v1::WatchEvent& ev) {
      reactor->enqueue_or_finish(ev);
      return reactor->wait_drain();
    });
    reactor->mark_done();
  });
  return reactor;
}

}  // namespace openkv::compute
