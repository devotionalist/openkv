> Language: [English](./README.md) · [中文](../zh/README.md)

# OpenKV Documentation

OpenKV is a compute/storage-separated distributed KV store: **Multi-Raft + RocksDB + gRPC** (C++ Meta/Storage, Go Gateway/clients). Keys live in ordered **Range Regions** with default replication factor **3**.

## Getting started & operations

| Document | Description |
|----------|-------------|
| [Quick start](quickstart.md) | Build, local cluster, put/get in minutes |
| [User guide](user-guide.md) | Install, console, Helm, scale-out |
| [Networking](networking.md) | Mandatory gRPC; optional Seastar |
| [Architecture](architecture.md) | Topology, components, HA / scale boundaries |
| [Contributing](contributing.md) | PR-only workflow, owner approval, tags/releases |

## Chinese deep docs (design / SOP / tests)

Full Chinese catalogs (architecture, production, topology, ops, tests, flows):

| Area | Index |
|------|-------|
| Full Chinese index | [docs/zh/](../zh/README.md) |
| Architecture | [architecture/](../architecture/ARCHITECTURE.md) |
| Production requirements | [production/](../production/META_PRODUCTION.md) |
| Data topology | [topology/](../topology/DATA_TOPOLOGY_INDEX.md) |
| Ops / SOP | [ops/](../ops/REGION_OPS.md) |
| Tests | [test/](../test/TEST_CASES.md) |
| Flow diagrams | [flows/](../flows/README.md) |
| Backlog | [BACKLOG.md](../BACKLOG.md) |

License: [Apache License 2.0](../../LICENSE)
