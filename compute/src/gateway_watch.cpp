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
// Watch：ServerWriteReactor + store_watch_*_async；间隔 sleep 走 WorkPool。
// Reactor 经 enable_shared_from_this / weak_ptr 护住：客户端取消后 OnDone 删除
// 对象时，在途 poll 回调不得再碰已释放的 reactor（曾致 gateway exit 139）。

#include "openkv/compute/gateway_server.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <memory>
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

/// 写队列与取消标志；生命周期长于 gRPC reactor（回调只握 weak_ptr）。
struct WatchBridge {
  std::mutex mu;
  std::condition_variable cv;
  std::deque<kv::v1::WatchEvent> q;
  kv::v1::WatchEvent pending;
  bool writing{false};
  bool done{false};
  bool cancelled{false};
  bool finished{false};
  /// gRPC 侧已 OnDone；此后禁止再 StartWrite / Finish。
  bool dead{false};
  class WatchReactor* reactor{nullptr};

  void stop_from_reactor() {
    std::lock_guard<std::mutex> lock(mu);
    cancelled = true;
    done = true;
    dead = true;
    reactor = nullptr;
    cv.notify_all();
  }
};

class WatchReactor final : public grpc::ServerWriteReactor<kv::v1::WatchEvent> {
 public:
  static WatchReactor* New() {
    auto* r = new WatchReactor();
    r->bridge_ = std::make_shared<WatchBridge>();
    {
      std::lock_guard<std::mutex> lock(r->bridge_->mu);
      r->bridge_->reactor = r;
    }
    // 控制块供 weak_ptr；真实释放仍在 OnDone → delete this。
    r->self_hold_ = std::shared_ptr<WatchReactor>(r, [](WatchReactor*) {});
    return r;
  }

  std::shared_ptr<WatchBridge> bridge() const { return bridge_; }
  std::weak_ptr<WatchReactor> weak_self() const { return self_hold_; }

  void enqueue_or_finish(kv::v1::WatchEvent ev) {
    {
      std::lock_guard<std::mutex> lock(bridge_->mu);
      if (bridge_->cancelled || bridge_->finished || bridge_->dead) return;
      bridge_->q.push_back(std::move(ev));
    }
    try_write();
  }

  bool wait_drain() {
    std::unique_lock<std::mutex> lock(bridge_->mu);
    bridge_->cv.wait(lock, [&] {
      return bridge_->cancelled || bridge_->dead || bridge_->q.empty();
    });
    return !bridge_->cancelled && !bridge_->dead;
  }

  void mark_done() {
    {
      std::lock_guard<std::mutex> lock(bridge_->mu);
      if (bridge_->dead) return;
      bridge_->done = true;
    }
    try_write();
  }

  void OnWriteDone(bool ok) override {
    bool need_finish = false;
    grpc::Status finish_st = grpc::Status::OK;
    {
      std::lock_guard<std::mutex> lock(bridge_->mu);
      if (bridge_->dead) return;
      bridge_->writing = false;
      if (!ok) bridge_->cancelled = true;
      if (!ok || bridge_->cancelled) {
        if (!bridge_->finished) {
          bridge_->finished = true;
          need_finish = true;
          finish_st = grpc::Status(grpc::StatusCode::CANCELLED,
                                   ok ? "cancelled" : "watch write failed");
        }
      }
    }
    bridge_->cv.notify_all();
    if (need_finish) {
      Finish(finish_st);
      return;
    }
    try_write();
  }

  void OnCancel() override {
    bool need_finish = false;
    {
      std::lock_guard<std::mutex> lock(bridge_->mu);
      if (bridge_->dead) return;
      bridge_->cancelled = true;
      bridge_->done = true;
      if (!bridge_->writing && !bridge_->finished) {
        bridge_->finished = true;
        need_finish = true;
      }
    }
    bridge_->cv.notify_all();
    if (need_finish) {
      Finish(grpc::Status(grpc::StatusCode::CANCELLED, "cancelled"));
    }
  }

  void OnDone() override {
    bridge_->stop_from_reactor();
    self_hold_.reset();
    delete this;
  }

 private:
  WatchReactor() = default;

  void try_write() {
    bool need_finish = false;
    grpc::Status finish_st = grpc::Status::OK;
    {
      std::lock_guard<std::mutex> lock(bridge_->mu);
      if (bridge_->dead || bridge_->writing || bridge_->finished) return;
      if (bridge_->cancelled) {
        bridge_->finished = true;
        need_finish = true;
        finish_st = grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
      } else if (bridge_->q.empty()) {
        if (!bridge_->done) return;
        bridge_->finished = true;
        need_finish = true;
        finish_st = grpc::Status::OK;
      } else {
        bridge_->pending = std::move(bridge_->q.front());
        bridge_->q.pop_front();
        bridge_->writing = true;
        StartWrite(&bridge_->pending);
        return;
      }
    }
    if (need_finish) Finish(finish_st);
  }

  std::shared_ptr<WatchBridge> bridge_;
  std::shared_ptr<WatchReactor> self_hold_;
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

void GatewayServer::watch_async(kv::v1::WatchRequest req,
                                const std::function<bool(const kv::v1::WatchEvent&)>& write_ev,
                                std::function<void()> on_done, std::function<bool()> still_open) {
  struct State {
    GatewayServer* self;
    kv::v1::WatchRequest req;
    std::function<bool(const kv::v1::WatchEvent&)> write_ev;
    std::function<void()> on_done;
    std::function<bool()> still_open;
    std::atomic<bool> stopped{false};
    uint64_t since{0};
    std::vector<uint32_t> regions;
    size_t head_idx{0};
    size_t poll_idx{0};
    std::shared_ptr<std::function<void()>> after_heads;
    std::shared_ptr<std::function<void()>> poll_round;

    bool live() const {
      if (stopped.load(std::memory_order_acquire)) return false;
      return !still_open || still_open();
    }

    void stop() {
      stopped.store(true, std::memory_order_release);
      if (on_done) {
        auto cb = std::move(on_done);
        on_done = nullptr;
        cb();
      }
    }
  };
  auto st = std::make_shared<State>();
  st->self = this;
  st->req = std::move(req);
  st->write_ev = write_ev;
  st->on_done = std::move(on_done);
  st->still_open = std::move(still_open);
  st->since = st->req.start_revision();
  st->regions = regions_for_watch(st->req);
  st->after_heads = std::make_shared<std::function<void()>>();
  st->poll_round = std::make_shared<std::function<void()>>();

  *st->poll_round = [st]() {
    if (!st->live()) {
      st->stop();
      return;
    }
    if (st->poll_idx >= st->regions.size()) {
      st->poll_idx = 0;
      if (st->self->pool_) {
        st->self->pool_->post([st]() {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          if (st->live()) (*st->poll_round)();
          else st->stop();
        });
      } else {
        std::thread([st]() {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          if (st->live()) (*st->poll_round)();
          else st->stop();
        }).detach();
      }
      return;
    }
    const uint32_t rid = st->regions[st->poll_idx++];
    auto addr_opt = st->self->pick_leader_addr(rid);
    if (!addr_opt) {
      (*st->poll_round)();
      return;
    }
    st->self->note_to_store(*addr_opt);
    store_watch_poll_async(
        *addr_opt, rid, st->since, 64, 15000,
        [st, rid](grpc::Status sst, StoreWatchPollResult out) {
          auto handle = [st, rid, sst, out = std::move(out)]() mutable {
            if (!st->live()) {
              st->stop();
              return;
            }
            if (!sst.ok()) {
              (*st->poll_round)();
              return;
            }
            if (!out.redirect_to.empty() || contains(out.error, "splitting") ||
                contains(out.error, "not_leader")) {
              if (!out.redirect_to.empty() && out.redirect_to != "no_region") {
                st->self->remember_leader(rid, out.redirect_to);
              }
              st->self->refresh_routes();
              (*st->poll_round)();
              return;
            }
            for (const auto& ev : out.events) {
              if (!st->live()) {
                st->stop();
                return;
              }
              if (!st->self->match_watch(st->req, ev.key)) continue;
              if (st->req.start_revision() > 0 && ev.revision <= st->req.start_revision()) continue;
              kv::v1::WatchEvent o;
              o.set_type(ev.deleted ? kv::v1::WatchEvent::DELETE : kv::v1::WatchEvent::PUT);
              o.set_key(ev.key);
              o.set_value(ev.value);
              o.set_revision(ev.revision);
              if (!st->write_ev(o)) {
                st->stop();
                return;
              }
            }
            if (out.max_revision > st->since) st->since = out.max_revision;
            (*st->poll_round)();
          };
          // write_ev 可能阻塞等 OnWriteDone；放到 WorkPool，勿堵 gRPC CQ。
          if (st->self->pool_) {
            st->self->pool_->post(std::move(handle));
          } else {
            handle();
          }
        });
  };

  auto start_poll = [st]() {
    if (!st->live()) {
      st->stop();
      return;
    }
    if (!st->req.key().empty()) {
      auto tg_req = std::make_shared<kv::v1::TxnGetRequest>();
      tg_req->set_key(st->req.key());
      tg_req->set_start_ts(std::numeric_limits<uint64_t>::max());
      auto tg = std::make_shared<kv::v1::TxnGetResponse>();
      st->self->txn_get_async(tg_req.get(), tg.get(), 0, [st, tg_req, tg](grpc::Status) {
        if (!st->live()) {
          st->stop();
          return;
        }
        if (tg->found() && !tg->locked()) {
          uint64_t rev = st->since;
          if (rev == 0) rev = 1;
          kv::v1::WatchEvent ev;
          ev.set_type(kv::v1::WatchEvent::PUT);
          ev.set_key(st->req.key());
          ev.set_value(tg->value());
          ev.set_revision(rev);
          if (!st->write_ev(ev)) {
            st->stop();
            return;
          }
        }
        (*st->poll_round)();
      });
      return;
    }
    (*st->poll_round)();
  };

  *st->after_heads = [st, start_poll]() {
    if (st->head_idx >= st->regions.size() || st->req.start_revision() != 0) {
      start_poll();
      return;
    }
    start_poll();
  };

  if (st->since != 0) {
    start_poll();
    return;
  }

  // Lift since to max watch_head across regions.
  struct HeadState {
    std::shared_ptr<State> st;
    size_t idx{0};
    std::shared_ptr<std::function<void()>> step;
  };
  auto hs = std::make_shared<HeadState>();
  hs->st = st;
  hs->step = std::make_shared<std::function<void()>>();
  *hs->step = [hs, start_poll]() {
    if (!hs->st->live()) {
      hs->st->stop();
      return;
    }
    if (hs->idx >= hs->st->regions.size()) {
      start_poll();
      return;
    }
    const uint32_t rid = hs->st->regions[hs->idx++];
    auto addr_opt = hs->st->self->pick_leader_addr(rid);
    if (!addr_opt) {
      (*hs->step)();
      return;
    }
    hs->st->self->note_to_store(*addr_opt);
    store_watch_head_async(*addr_opt, rid, 5000,
                           [hs](grpc::Status sst, StoreWatchHeadResult out) {
                             if (!hs->st->live()) {
                               hs->st->stop();
                               return;
                             }
                             if (sst.ok() && out.error.empty() && out.redirect_to.empty()) {
                               if (out.revision > hs->st->since) hs->st->since = out.revision;
                             } else if (!out.redirect_to.empty()) {
                               hs->st->self->remember_leader(hs->st->regions[hs->idx - 1],
                                                             out.redirect_to);
                             }
                             (*hs->step)();
                           });
  };
  (*hs->step)();
}

grpc::ServerWriteReactor<kv::v1::WatchEvent>* GatewayServer::Watch(
    grpc::CallbackServerContext* ctx, const kv::v1::WatchRequest* req) {
  (void)ctx;
  auto* reactor = WatchReactor::New();
  note_client_rpc();
  kv::v1::WatchRequest req_copy = *req;
  std::weak_ptr<WatchReactor> weak = reactor->weak_self();
  auto bridge = reactor->bridge();
  watch_async(
      std::move(req_copy),
      [weak, bridge](const kv::v1::WatchEvent& ev) {
        auto r = weak.lock();
        if (!r) return false;
        {
          std::lock_guard<std::mutex> lock(bridge->mu);
          if (bridge->cancelled || bridge->dead) return false;
        }
        r->enqueue_or_finish(ev);
        return r->wait_drain();
      },
      [weak]() {
        if (auto r = weak.lock()) r->mark_done();
      },
      [bridge]() {
        std::lock_guard<std::mutex> lock(bridge->mu);
        return !bridge->cancelled && !bridge->dead;
      });
  return reactor;
}

}  // namespace openkv::compute
