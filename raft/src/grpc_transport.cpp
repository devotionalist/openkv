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

// @file grpc_transport.cpp
// Raft 出站 gRPC：Unary / InstallSnapshot 一律 Callback API（async / ClientWriteReactor）。

#include "openkv/raft/grpc_transport.hpp"

#include "openkv/codec.hpp"
#include "openkv/logging.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_callback.h>
#include "storage/v1/storage.grpc.pb.h"
#include "storage/v1/storage.pb.h"

namespace openkv::raft {

namespace {

constexpr int kMaxQueue = 4096;
constexpr int kGrpcMsgBytes = 64 << 20;       // 64MiB 安全上限
constexpr size_t kDefaultSnapChunk = 256 << 10;  // 256KiB 分片，避开默认 4MiB 限制

std::vector<std::string> split_pipe(const std::string& s) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : s) {
    if (c == '|') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

int priority_of(const std::string& type, const std::string& payload) {
  if (type == "RequestPreVote" || type == "RequestVote") return 30;
  if (type == "AppendEntries") {
    // 空心跳提高优先级，避免被复制/快照堵住导致 check_quorum 误判。
    int bars = 0;
    size_t i = 0;
    for (; i < payload.size() && bars < 4; ++i) {
      if (payload[i] == '|') ++bars;
    }
    if (bars == 4 && i < payload.size() && payload[i] == '0' &&
        (i + 1 >= payload.size() || payload[i + 1] == '|')) {
      return 25;
    }
    return 20;
  }
  if (type == "InstallSnapshot") return 10;
  return 0;
}

bool needs_fail_callback(const std::string& type) {
  return type == "AppendEntries" || type == "InstallSnapshot";
}

}  // namespace

struct GrpcRaftTransport::Impl {
  struct Job {
    uint32_t shard{0};
    std::string self_id;
    std::string peer_id;
    std::string type;
    std::string payload;
    int priority{0};
    uint64_t seq{0};
  };

  struct JobLess {
    bool operator()(const Job& a, const Job& b) const {
      if (a.priority != b.priority) return a.priority < b.priority;
      return a.seq > b.seq;
    }
  };

  std::mutex mu_;
  std::unordered_map<std::string, std::string> peers_;
  std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> nodes_;
  std::unordered_map<uint32_t, std::string> self_ids_;
  std::unordered_map<std::string, std::shared_ptr<::storage::v1::RaftService::Stub>> stubs_;

  mutable std::mutex qmu_;
  std::condition_variable qcv_;
  std::priority_queue<Job, std::vector<Job>, JobLess> q_;
  std::atomic<uint64_t> next_seq_{0};
  std::atomic<uint64_t> dropped_{0};
  std::atomic<bool> stop{true};
  std::vector<std::thread> workers_;
  size_t snap_chunk_bytes_{kDefaultSnapChunk};

  std::shared_ptr<::storage::v1::RaftService::Stub> stub_for(const std::string& addr) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& s = stubs_[addr];
    if (!s) {
      grpc::ChannelArguments args;
      args.SetMaxReceiveMessageSize(kGrpcMsgBytes);
      args.SetMaxSendMessageSize(kGrpcMsgBytes);
      auto ch = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args);
      s = ::storage::v1::RaftService::NewStub(ch);
    }
    return s;
  }

  size_t queue_depth() const {
    std::lock_guard<std::mutex> lock(qmu_);
    return q_.size();
  }

  void enqueue(Job j) {
    {
      std::lock_guard<std::mutex> lock(qmu_);
      j.seq = ++next_seq_;
      if (static_cast<int>(q_.size()) >= kMaxQueue) {
        q_.pop();
        dropped_.fetch_add(1, std::memory_order_relaxed);
      }
      q_.push(std::move(j));
    }
    qcv_.notify_one();
  }

  void worker_loop() {
    while (!stop.load()) {
      Job job;
      {
        std::unique_lock<std::mutex> lk(qmu_);
        qcv_.wait_for(lk, std::chrono::milliseconds(50),
                      [&] { return stop.load() || !q_.empty(); });
        if (stop.load() && q_.empty()) break;
        if (q_.empty()) continue;
        job = q_.top();
        q_.pop();
      }
      dispatch(job);
    }
  }

  class InstallSnapshotReactor final : public grpc::ClientWriteReactor<::storage::v1::SnapshotChunk> {
   public:
    InstallSnapshotReactor(std::shared_ptr<::storage::v1::RaftService::Stub> stub, Job job,
                           std::string addr, uint64_t send_term, uint64_t last_idx,
                           uint64_t last_term, std::string bin, size_t chunk_sz)
        : stub_(std::move(stub)),
          job_(std::move(job)),
          addr_(std::move(addr)),
          send_term_(send_term),
          last_idx_(last_idx),
          last_term_(last_term),
          bin_(std::move(bin)),
          chunk_sz_(chunk_sz == 0 ? kDefaultSnapChunk : chunk_sz),
          total_(bin_.size()),
          done_(std::make_shared<std::promise<grpc::Status>>()) {
      ctx_.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
      stub_->async()->InstallSnapshot(&ctx_, &resp_, this);
      write_next();
      StartCall();
    }

    std::future<grpc::Status> get_future() { return done_->get_future(); }
    const ::storage::v1::InstallSnapshotResponse& resp() const { return resp_; }

    void OnWriteDone(bool ok) override {
      if (!ok) {
        SKV_WARN << "InstallSnapshot write fail to=" << job_.peer_id << " addr=" << addr_
                 << " offset=" << offset_;
        if (!last_was_done_) StartWritesDone();
        return;
      }
      offset_ += last_n_;
      if (last_was_done_) return;  // StartWriteLast 已隐含 WritesDone
      write_next();
    }

    void OnDone(const grpc::Status& st) override {
      try {
        done_->set_value(st);
      } catch (...) {
      }
    }

   private:
    void write_next() {
      const size_t n = total_ == 0 ? 0 : std::min(chunk_sz_, static_cast<size_t>(total_ - offset_));
      const bool done = (offset_ + n >= total_);
      chunk_.Clear();
      chunk_.set_shard_id(job_.shard);
      chunk_.set_leader_id(job_.self_id);
      chunk_.set_term(send_term_);
      chunk_.set_last_included_index(last_idx_);
      chunk_.set_last_included_term(last_term_);
      chunk_.set_offset(offset_);
      if (n > 0) chunk_.set_data(bin_.data() + offset_, n);
      chunk_.set_done(done);
      last_n_ = n;
      last_was_done_ = done;
      if (done) {
        StartWriteLast(&chunk_, grpc::WriteOptions());
      } else {
        StartWrite(&chunk_);
      }
    }

    std::shared_ptr<::storage::v1::RaftService::Stub> stub_;
    Job job_;
    std::string addr_;
    uint64_t send_term_{0};
    uint64_t last_idx_{0};
    uint64_t last_term_{0};
    std::string bin_;
    size_t chunk_sz_{kDefaultSnapChunk};
    uint64_t total_{0};
    uint64_t offset_{0};
    size_t last_n_{0};
    bool last_was_done_{false};
    grpc::ClientContext ctx_;
    ::storage::v1::InstallSnapshotResponse resp_;
    ::storage::v1::SnapshotChunk chunk_;
    std::shared_ptr<std::promise<grpc::Status>> done_;
  };

  void dispatch(const Job& job) {
    std::shared_ptr<RaftNode> node;
    std::string addr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = nodes_.find(job.shard);
      if (it == nodes_.end() || !it->second) return;
      node = it->second;
      auto pit = peers_.find(job.peer_id);
      if (pit == peers_.end() || pit->second.empty()) {
        if (needs_fail_callback(job.type)) node->on_rpc_transport_fail(job.peer_id);
        return;
      }
      addr = pit->second;
    }
    auto stub = stub_for(addr);
    if (!stub) {
      if (needs_fail_callback(job.type)) node->on_rpc_transport_fail(job.peer_id);
      return;
    }

    try {
      if (job.type == "RequestVote" || job.type == "RequestPreVote") {
        auto parts = split_pipe(job.payload);
        if (parts.size() < 3) return;
        auto ctx = std::make_shared<grpc::ClientContext>();
        ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        auto req = std::make_shared<::storage::v1::RequestVoteRequest>();
        req->set_shard_id(job.shard);
        req->set_candidate_id(job.self_id);
        req->set_term(std::stoull(parts[0]));
        req->set_last_log_index(std::stoull(parts[1]));
        req->set_last_log_term(std::stoull(parts[2]));
        auto resp = std::make_shared<::storage::v1::RequestVoteResponse>();
        const bool pre = (job.type == "RequestPreVote");
        auto done = std::make_shared<std::promise<grpc::Status>>();
        auto fut = done->get_future();
        auto cb = [done, stub, ctx, req, resp](grpc::Status st) {
          try {
            done->set_value(std::move(st));
          } catch (...) {
          }
        };
        if (pre) {
          stub->async()->RequestPreVote(ctx.get(), req.get(), resp.get(), std::move(cb));
        } else {
          stub->async()->RequestVote(ctx.get(), req.get(), resp.get(), std::move(cb));
        }
        auto st = fut.get();
        if (!st.ok()) return;
        if (pre) {
          node->on_request_pre_vote_reply(job.peer_id, resp->term(), resp->vote_granted());
        } else {
          node->on_request_vote_reply(job.peer_id, resp->term(), resp->vote_granted());
        }
        return;
      }

      if (job.type == "AppendEntries") {
        auto parts = split_pipe(job.payload);
        if (parts.size() < 5) {
          node->on_rpc_transport_fail(job.peer_id);
          return;
        }
        auto ctx = std::make_shared<grpc::ClientContext>();
        ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto req = std::make_shared<::storage::v1::AppendEntriesRequest>();
        req->set_shard_id(job.shard);
        req->set_leader_id(job.self_id);
        req->set_term(std::stoull(parts[0]));
        req->set_prev_log_index(std::stoull(parts[1]));
        req->set_prev_log_term(std::stoull(parts[2]));
        req->set_leader_commit(std::stoull(parts[3]));
        const size_t n = static_cast<size_t>(std::stoull(parts[4]));
        size_t idx = 5;
        for (size_t i = 0; i < n && idx + 2 < parts.size(); ++i) {
          auto* e = req->add_entries();
          e->set_term(std::stoull(parts[idx]));
          e->set_index(std::stoull(parts[idx + 1]));
          std::string bin;
          if (openkv::from_hex(parts[idx + 2], &bin)) {
            e->set_data(bin);
          } else {
            e->set_data(parts[idx + 2]);
          }
          idx += 3;
        }
        auto resp = std::make_shared<::storage::v1::AppendEntriesResponse>();
        auto done = std::make_shared<std::promise<grpc::Status>>();
        auto fut = done->get_future();
        stub->async()->AppendEntries(ctx.get(), req.get(), resp.get(),
                                     [done, stub, ctx, req, resp](grpc::Status st) {
                                       try {
                                         done->set_value(std::move(st));
                                       } catch (...) {
                                       }
                                     });
        auto st = fut.get();
        if (!st.ok()) {
          node->on_rpc_transport_fail(job.peer_id);
          return;
        }
        node->on_append_entries_reply(job.peer_id, resp->term(), resp->success(),
                                      resp->match_index());
        return;
      }

      if (job.type == "InstallSnapshot") {
        auto parts = split_pipe(job.payload);
        if (parts.size() < 4) {
          node->on_rpc_transport_fail(job.peer_id);
          return;
        }
        const uint64_t send_term = node->soft_state().term;
        const uint64_t last_idx = std::stoull(parts[1]);
        const uint64_t last_term = std::stoull(parts[2]);
        std::string bin;
        if (!openkv::from_hex(parts[3], &bin)) bin = parts[3];
        // reactor 须活到 OnDone；用 unique_ptr 在 wait 后销毁。
        auto reactor = std::make_unique<InstallSnapshotReactor>(
            stub, job, addr, send_term, last_idx, last_term, std::move(bin), snap_chunk_bytes_);
        auto fut = reactor->get_future();
        auto st = fut.get();
        if (!st.ok()) {
          SKV_WARN << "InstallSnapshot Finish fail to=" << job.peer_id
                   << " code=" << st.error_code() << " " << st.error_message();
          node->on_rpc_transport_fail(job.peer_id);
          return;
        }
        const auto& resp = reactor->resp();
        const uint64_t reply_term = resp.term() != 0 ? resp.term() : send_term;
        const uint64_t match =
            resp.match_index() != 0 ? resp.match_index() : (resp.ok() ? last_idx : 0);
        if (!resp.ok()) {
          SKV_WARN << "InstallSnapshot rejected to=" << job.peer_id << " term=" << reply_term;
        } else {
          SKV_INFO << "InstallSnapshot rpc ok to=" << job.peer_id << " match=" << match
                   << " term=" << reply_term;
        }
        node->on_install_snapshot_reply(job.peer_id, reply_term, resp.ok(), match);
        return;
      }

      // 未知类型：若误入队带 inflight 的消息则 fail，避免永久卡住。
      if (needs_fail_callback(job.type)) {
        SKV_WARN << "grpc raft unknown type=" << job.type << " peer=" << job.peer_id;
        node->on_rpc_transport_fail(job.peer_id);
      }
    } catch (const std::exception& e) {
      SKV_WARN << "grpc raft dispatch err: " << e.what();
      if (needs_fail_callback(job.type)) node->on_rpc_transport_fail(job.peer_id);
    }
  }
};

GrpcRaftTransport::GrpcRaftTransport() : impl_(std::make_unique<Impl>()) {}
GrpcRaftTransport::~GrpcRaftTransport() { stop(); }

void GrpcRaftTransport::set_peer(const std::string& id, const std::string& addr) {
  std::lock_guard<std::mutex> lock(impl_->mu_);
  impl_->peers_[id] = addr;
}

std::string GrpcRaftTransport::address_of(const std::string& id) const {
  std::lock_guard<std::mutex> lock(impl_->mu_);
  auto it = impl_->peers_.find(id);
  return it == impl_->peers_.end() ? std::string{} : it->second;
}

void GrpcRaftTransport::register_node(uint32_t shard_id, const std::string& self_id,
                                      std::shared_ptr<RaftNode> node) {
  std::lock_guard<std::mutex> lock(impl_->mu_);
  impl_->nodes_[shard_id] = node;
  impl_->self_ids_[shard_id] = self_id;
}

void GrpcRaftTransport::unregister_node(uint32_t shard_id) {
  std::lock_guard<std::mutex> lock(impl_->mu_);
  impl_->nodes_.erase(shard_id);
  impl_->self_ids_.erase(shard_id);
}

TransportSend GrpcRaftTransport::bind(uint32_t shard_id) {
  return [this, shard_id](const std::string& peer_id, const std::string& type,
                          const std::string& payload) {
    if (type == "RequestVote" || type == "RequestPreVote") {
      out_raft_.add(peer_id);
    } else if (type == "AppendEntries") {
      auto parts = split_pipe(payload);
      uint64_t nent = 0;
      if (parts.size() >= 5) {
        try {
          nent = std::stoull(parts[4]);
        } catch (...) {
          nent = 0;
        }
      }
      if (nent > 0) {
        out_sync_.add(peer_id);
      } else {
        out_raft_.add(peer_id);
      }
    } else if (type == "InstallSnapshot") {
      out_sync_.add(peer_id);
    }
    std::string self;
    {
      std::lock_guard<std::mutex> lock(impl_->mu_);
      auto it = impl_->self_ids_.find(shard_id);
      if (it != impl_->self_ids_.end()) self = it->second;
    }
    Impl::Job job;
    job.shard = shard_id;
    job.self_id = std::move(self);
    job.peer_id = peer_id;
    job.type = type;
    job.payload = payload;
    job.priority = priority_of(type, payload);
    impl_->enqueue(std::move(job));
  };
}

void GrpcRaftTransport::start() {
  if (!impl_->stop.exchange(false)) return;
  constexpr int kWorkers = 4;
  for (int i = 0; i < kWorkers; ++i) {
    impl_->workers_.emplace_back([this] { impl_->worker_loop(); });
  }
  SKV_INFO << "GrpcRaftTransport started workers=" << kWorkers;
}

void GrpcRaftTransport::stop() {
  if (impl_->stop.exchange(true)) return;
  impl_->qcv_.notify_all();
  for (auto& t : impl_->workers_) {
    if (t.joinable()) t.join();
  }
  impl_->workers_.clear();
}

size_t GrpcRaftTransport::queue_depth() const { return impl_->queue_depth(); }

uint64_t GrpcRaftTransport::dropped_jobs() const {
  return impl_->dropped_.load(std::memory_order_relaxed);
}

void GrpcRaftTransport::set_snap_chunk_bytes(size_t n) {
  impl_->snap_chunk_bytes_ = n;
}

uint64_t GrpcRaftTransport::out_raft_total() const {
  uint64_t s = 0;
  for (const auto& kv : out_raft_.snapshot()) s += kv.second;
  return s;
}

uint64_t GrpcRaftTransport::out_sync_total() const {
  uint64_t s = 0;
  for (const auto& kv : out_sync_.snapshot()) s += kv.second;
  return s;
}

}  // namespace openkv::raft
