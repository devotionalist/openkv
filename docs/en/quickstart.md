> Language: [English](./quickstart.md) · [中文](../zh/README.md)

# Quick start

Build locally, start a cluster, put/get a key. Defaults: **gRPC C++ required**; Seastar **OFF**; RocksDB + jemalloc bundled via FetchContent.

## Build & one-shot E2E

```bash
make          # configure + build + unit tests
make e2e      # build → start cluster → Go client → stop
```

## skv CLI

```bash
cmake --build build -j --target skv
./build/tools/skv start --fresh
./build/tools/skv put hello world
./build/tools/skv get hello
./build/tools/skv locate hello
./build/tools/skv route
./build/tools/skv stop
```

## Make helpers

```bash
make cluster-up      # gateway default :19000
make cluster-test
make admin-run       # http://127.0.0.1:19100 console
make cluster-down
# or: SKV_ADMIN=1 make cluster-up
```

## Clients

```bash
./.run/kv-example 127.0.0.1:19000
cd client/go && go run ./examples/e2e -addr 127.0.0.1:19000
```

Default ports: Meta `:8500–8502`, Storage `:9100…`, Gateway `:19000`, Web `:19100`.

Next: [User guide](user-guide.md) · [Networking](networking.md) · [Architecture](architecture.md)
