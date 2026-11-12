> Language: [English](./networking.md) · [中文](../NETWORKING.md)

# Networking: gRPC

How OpenKV talks on the wire today: **mandatory gRPC** on every cluster path, with multi-service multiplexing on each node’s main port. Full Chinese detail: [NETWORKING.md](../NETWORKING.md). Threading model: [22-async-threading-model](../flows/22-async-threading-model.md).

## Summary

| Item | Detail |
|------|--------|
| Protocol | **gRPC** (mandatory); default local & production path |
| CMake | `OPENKV_USE_GRPC` fixed `ON` |
| Threads | gRPC Callback/CQ receive threads + `WorkPool` / BatchSystem / Transport workers |

```
Client / skv / Web
        │  gRPC  :19000
        ▼
C++ gateway_node            KVService / DSService
        │  gRPC
   ┌────┴────┐
   ▼         ▼
Meta :850x   Storage :910x
MetaService  StoreService + RaftService + AdminService
```

There is **no** public raw TCP line protocol. Ops use typed Admin / Meta RPCs (and thin CLI wrappers).

### Default ports

| Role | Address |
|------|---------|
| Gateway | `127.0.0.1:19000` |
| Meta ×3 | `127.0.0.1:8500`–`8502` |
| Storage ×N | `127.0.0.1:9100`–… |
| Web console | `http://127.0.0.1:19100` (HTTP → gRPC) |

## Proto layout

| Path | Content |
|------|---------|
| `proto/kv/v1/` | External KV / DS / Watch (Gateway) |
| `proto/meta/v1/` | `MetaService`: route, TSO, node lifecycle, scheduling |
| `proto/storage/v1/` | `StoreService` / `RaftService` / `AdminService` |
| C++ codegen | `build/generated/` at build time |
| Go codegen | `client/go/gen/` (`make proto`) |

## Storage services (same port)

| Service | Typical caller | Role |
|---------|----------------|------|
| `StoreService` | Gateway | `Get` / `Propose` / `TransferLeadership` |
| `RaftService` | Peer Storage | AppendEntries / Vote / Snapshot |
| `AdminService` | skv / Web / ops | Split / merge / place / progress |

## Build

```bash
cmake -B build   # OPENKV_USE_GRPC forced ON
cmake --build build -j
```

Related: [flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md), [USER_GUIDE](../USER_GUIDE.md).
