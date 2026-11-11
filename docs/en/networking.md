> Language: [English](./networking.md) · [中文](../NETWORKING.md)

# Networking: gRPC and Seastar

How OpenKV talks on the wire today, and what the optional Seastar switch does. Full Chinese detail: [NETWORKING.md](../NETWORKING.md).

## Summary

| | **gRPC** | **Seastar** |
|--|----------|-------------|
| Status | **Mandatory**; default local & production path | **Optional**; default `OFF`; not on the business path |
| Role | All cluster RPC | Planned async reactor (does not change protocol) |
| CMake | `OPENKV_USE_GRPC` fixed `ON` | `-DOPENKV_USE_SEASTAR=ON` to fetch & link |

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

There is **no** public raw TCP line protocol. Legacy admin line verbs remain only as RPC body semantics (`AdminService.ExecLine` / Meta equivalents).

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
| `AdminService` | skv / Web / ops | Line-style admin & migration |

## Optional Seastar

```bash
# Linux experimental only; does not change the gRPC surface
sudo ./path/to/seastar/install-dependencies.sh
cmake -B build -DOPENKV_USE_SEASTAR=ON
# optional pin: -DOPENKV_SEASTAR_GIT_TAG=seastar-25.05.0
cmake --build build -j
```

Design notes (Chinese): [architecture/SEASTAR_NET.md](../architecture/SEASTAR_NET.md), [flows/GRPC_MANDATORY.md](../flows/GRPC_MANDATORY.md).
