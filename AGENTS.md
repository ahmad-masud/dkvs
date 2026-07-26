# AGENTS.md

Guidance for AI agents working in the **dkvs** (Distributed Key-Value Store) repository.

## Project summary

dkvs is a minimal Go key-value store built on **gRPC** and **HashiCorp Raft**. It is intended as a readable reference implementation for learning and experimentation—not production-ready as-is.

Module path: `github.com/ahmad-masud/dkvs`

## Repository map

| Path | Purpose |
|------|---------|
| `server/` | gRPC handlers, Raft FSM, cluster bootstrap, options, logging |
| `kvstore/` | `Storage` interface and in-memory implementation with TTL + snapshots |
| `client/` | Leader-aware Go client with redirect retries |
| `proto/` | Protobuf service definition (`kvstore.proto`) and generated stubs |
| `_examples/raft_cluster/` | CLI to run single- or multi-node clusters |
| `_examples/client/` | Raw gRPC client example (no leader redirect) |
| `overview.md` | Detailed architecture and request-flow documentation |
| `README.md` | User-facing quick start, API, and operations guide |

## Architecture at a glance

- **Writes** (`Set`, `Delete`): With Raft enabled, only the leader accepts writes. Commands are JSON-marshaled, applied via `raft.Apply`, and replicated to followers through the FSM.
- **Reads** (`Get`): Served locally from each node's storage without a Raft round-trip.
- **Followers** reject writes with `codes.Unavailable` and set a `leader-address` response header.
- **Client** (`client/`): Follows `leader-address` on write failures with exponential backoff + jitter.

See `overview.md` for diagrams and deeper behavior notes.

## Where to make changes

| Task | Start here |
|------|------------|
| Add or change RPC methods | `proto/kvstore.proto` → regenerate stubs → `server/server.go` handlers |
| Change replication/consensus | `server/raft.go`, `server/server.go` |
| Change storage behavior | `kvstore/kvstore.go` or implement `kvstore.Storage` |
| Add server configuration | `server/options.go`, wire in `server/server.go` or `server/cluster.go` |
| Improve client redirect/retry | `client/client.go` |
| Add CLI flags | `_examples/raft_cluster/main.go` and `server/cluster.go` if needed |
| Add tests | Colocate with package: `*_test.go` |

## Coding conventions

- Use **functional options** for server configuration (`server.Option`, `With*` functions in `options.go`). Match this pattern for new options.
- Keep changes **focused and minimal**. Do not refactor unrelated code.
- Prefer extending existing interfaces (`kvstore.Storage`, hooks) over parallel abstractions.
- Use `context.Context` on RPC boundaries; respect cancellation and timeouts.
- Logging uses logrus via helpers in `server/logging.go`. Use structured fields (`subsys`, `method`, etc.).
- Generated proto files (`proto/*.pb.go`, `proto/*_grpc.pb.go`) are **gitignored**. Regenerate with `make build`; do not hand-edit them.

## Build, test, and run

```bash
# Regenerate proto stubs, tidy, build all packages
make build

# Run tests (CI uses -race -cover)
make test

# Build and run example cluster node
make build-node
./node -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap

# Run example client
make run-client
```

Proto regeneration (if `make build` is unavailable):

```bash
protoc --go_out=paths=source_relative:proto \
       --go-grpc_out=paths=source_relative:proto \
       --proto_path=proto proto/kvstore.proto
```

## Testing expectations

- Add or update tests when changing behavior. Tests live beside the code they cover.
- `server/server_test.go`: gRPC integration (auth, hooks, TTL).
- `server/raft_integration_test.go`: 3-node ephemeral Raft cluster.
- `client/client_test.go`: leader redirect via header and fallback message.
- `kvstore/kvstore_test.go`: storage and TTL unit tests.
- Use ephemeral ports (`127.0.0.1:0`) and `t.TempDir()` for integration tests.
- Run `go test ./...` before finishing substantive changes.

## Known design constraints

Agents should be aware of these when modifying or extending the system:

1. **`leader-address` is the Raft transport address**, not the gRPC address. Followers set it from `raftInstance.Leader()`. Client redirects may target the wrong port unless Raft and gRPC addressing are aligned or a mapping is added.
2. **`Get` is a local read** and may be briefly stale on followers until the FSM applies committed entries.
3. **`WithAdminAddr` and `WithTLS` are placeholders**—fields exist in options but are not wired into server startup.
4. **Non-Raft peer mode** (`WithPeers`) uses best-effort async replication; it does not provide consensus guarantees.
5. **Always shut down Raft cleanly** via `Server.Shutdown()` to release BoltDB file locks (especially on Windows).

## gRPC API

Service: `proto.KVStore` — `Set`, `Get`, `Delete` (see `proto/kvstore.proto`).

Metadata headers:

| Header | Direction | Purpose |
|--------|-----------|---------|
| `leader-address` | Response | Follower redirect hint for writes |
| `x-replicated` | Request | Marks peer replication requests (non-Raft mode) |
| `authorization` | Request | `Bearer <token>` when `WithAuthToken` is enabled |

## Do not

- Commit generated proto stubs unless the project policy changes (they are gitignored).
- Skip `Server.Shutdown()` in new long-running entrypoints.
- Add production features (TLS wiring, admin API) without explicit user request—they are documented gaps.
- Create commits or pull requests unless the user asks.
- Over-engineer: this codebase favors clarity over abstraction.

## Useful references

- `overview.md` — system design, request flows, component responsibilities
- `README.md` — quick start, grpcurl examples, configuration tables
