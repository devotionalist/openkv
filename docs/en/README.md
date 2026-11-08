> Language: [English](./README.md) · [中文](../zh/README.md)

# OpenKV Documentation

**This is the default documentation language.**

OpenKV is a compute/storage-separated distributed KV store: **Multi-Raft + RocksDB + gRPC** (C++ Meta/Storage, Go Gateway/clients). Keys live in ordered **Range Regions** with default replication factor **3**.

## Getting started & operations

| Document | Description |
|----------|-------------|
| [Quick start](quickstart.md) | Build, local cluster, put/get in minutes |
| [User guide](user-guide.md) | Install, console, Helm, scale-out |
| [Networking](networking.md) | Mandatory gRPC; optional Seastar |
| [Architecture](architecture.md) | Topology, components, HA / scale boundaries |
| [Contributing](contributing.md) | PR-only workflow, owner approval, tags/releases |
| [Version CAS (ZH)](../flows/13-version-cas.md) | Single-key revision CAS (Gateway + plain Propose) |

## Additional design / SOP / test docs

Deep design notes (mostly Chinese today) are linked from the [Chinese index](../zh/README.md). Prefer the English pages above for onboarding; use the Chinese catalog for architecture / ops / flow detail until those pages are fully translated.

| Area | Link |
|------|------|
| Chinese index | [docs/zh/](../zh/README.md) |
| Architecture (ZH) | [architecture/](../architecture/ARCHITECTURE.md) |
| Production (ZH) | [production/](../production/META_PRODUCTION.md) |
| Topology (ZH) | [topology/](../topology/DATA_TOPOLOGY_INDEX.md) |
| Ops / SOP (ZH) | [ops/](../ops/REGION_OPS.md) |
| Tests (ZH) | [test/](../test/TEST_CASES.md) |
| Flows (ZH) | [flows/](../flows/README.md) |
| Backlog | [BACKLOG.md](../BACKLOG.md) |

License: [Apache License 2.0](../../LICENSE)
