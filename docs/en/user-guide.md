> Language: [English](./user-guide.md) · [中文](../USER_GUIDE.md)

# User guide

OpenKV: compute/storage-separated distributed KV — Meta / Storage (C++) + Gateway / clients / Web console (Go).

This page is the English entry for install, access, and scale. Full Chinese manual: [USER_GUIDE.md](../USER_GUIDE.md). Networking: [networking.md](networking.md).

## Components & ports

| Component | Role | Default port | Protocol |
|-----------|------|--------------|----------|
| Meta ×3 | Routing, topology, scheduling | 8500 | gRPC `MetaService` |
| Storage ×N | Data + Multi-Raft | 9100 | gRPC Store / Raft / Admin |
| Gateway | Stateless compute entry | 19000 | gRPC `KVService` / `DSService` |
| Web console | Topology / ops / Console | 19100 | HTTP → gRPC |

```
Client / skv / Web Console
        │ gRPC
        ▼
   Gateway :19000  ──gRPC──▶  Meta ×3
        │ gRPC
        ▼
   Storage ×N (RF=3; peer Raft also gRPC)
```

## Local quick path

```bash
make                 # configure + build + unit tests
make cluster-up      # local cluster (Gateway :19000)
make admin-run       # console http://127.0.0.1:19100
make cluster-down
```

Read/write:

```bash
./.run/kv-example 127.0.0.1:19000
./build/tools/skv put hello world
./build/tools/skv get hello
```

Go SDK:

```bash
make proto
cd client/go && go mod tidy
go run ./examples/basic 127.0.0.1:19000
```

```go
cli, _ := kv.Dial(ctx, "127.0.0.1:19000")
cli.Put(ctx, []byte("k"), []byte("v"))
val, found, rev, _ := cli.Get(ctx, []byte("k"))
```

Console tabs (local): Overview (topology), Ops (split/merge/balance), Console (KV + Hash/Set/List/ZSet). Screenshot: [console-overview.png](../images/console-overview.png).

## Kubernetes / Helm

Charts under `deploy/helm/`. Design notes (Chinese): [flows/K8S_HELM.md](../flows/K8S_HELM.md), [flows/K8S_SCALE.md](../flows/K8S_SCALE.md). Step-by-step Chinese: [USER_GUIDE.md §3–4](../USER_GUIDE.md).

## Scale-out

Adding a Storage process ≠ joining the cluster. Join → balance / place replicas. SOP (Chinese): [ops/SCALE_OUT_SOP.md](../ops/SCALE_OUT_SOP.md), design [architecture/DATA_SCALE.md](../architecture/DATA_SCALE.md).

## Related

- [Quick start](quickstart.md)
- [Architecture](architecture.md)
- [Chinese full index](../zh/README.md)
