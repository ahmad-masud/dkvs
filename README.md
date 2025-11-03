# dkvs — Distributed Key-Value Store (Raft-backed) [![Go Reference](https://pkg.go.dev/badge/github.com/ahmad-masud/dkvs.svg)](https://pkg.go.dev/github.com/ahmad-masud/dkvs) [![Build Status](https://github.com/ahmad-masud/dkvs/workflows/ci/badge.svg)](https://github.com/ahmad-masud/dkvs/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/ahmad-masud/dkvs)](https://goreportcard.com/report/github.com/ahmad-masud/dkvs) [![License: MIT](https://img.shields.io/badge/License-MIT%202.0-brightgreen.svg)](https://opensource.org/licenses/MIT) [![Release](https://img.shields.io/github/v/release/ahmad-masud/dkvs)](https://github.com/ahmad-masud/dkvs/releases)


Welcome to dkvs — a small, well-documented distributed key-value store implemented in Go with gRPC and HashiCorp Raft. It’s designed to be simple, readable, and easy to run locally for development and experimentation while providing the core primitives you’d expect from a distributed KV: leader-based strong consistency for writes, snapshots, persistence, graceful shutdown, simple auth, and observability.

This README is intentionally comprehensive. It covers architecture, how to build and run the project (single-node and small clusters), usage examples (client and server), configuration options, testing, troubleshooting notes (especially on Windows), and recommended next steps for production hardening.

---

## Table of Contents

- Project overview
- Features
- High-level architecture
- Quick start (Windows executable)
- Run a 3-node example
- Build from source
- gRPC API and proto
- Client usage (Go helper)
- Configuration options
- Authentication (Bearer token)
- Observability (Prometheus metrics)
- Snapshots and compaction
- Tests and verification
- Troubleshooting and Windows notes
- Production considerations and next steps
- Repository layout
- Contributing
- License

---

## Project overview

dkvs is a lightweight key-value store whose goal is to be a minimal, yet functional example of a distributed system with consensus. It demonstrates:

- Using HashiCorp Raft for leader election and replicated, fault-tolerant logs.
- An FSM that applies Set/Delete operations to a local storage backend.
- Snapshots and restore to limit Raft log growth.
- A clean gRPC API for clients.
- A small client that follows leader redirects and retries with backoff.
- Operational basics: graceful shutdown, simple auth, and Prometheus metrics.

This project is intentionally small and readable — great for learning, experimentation, and as a starting point for production work.

## Features

- Leader-based strong consistency for writes via HashiCorp Raft.
- In-memory KV with optional TTL; storage is pluggable.
- Snapshot/restore support to compact the Raft log.
- Graceful shutdown that closes Raft stores (avoids file locks on Windows).
- Lightweight Bearer token auth via `WithAuthToken(...)`.
- Prometheus metrics endpoint with request counts and latency histograms.
- Client helper with leader-follow and jittered exponential backoff.
- Examples for single-node and multi-node clusters.
- Unit and integration tests (including a 3-node Raft test).

## High-level architecture

- server: gRPC server with Set/Get/Delete handlers. With Raft enabled, Set/Delete are applied via `raft.Apply` and replicated; the FSM mutates the local store.
- kvstore: In-memory KV store with TTL and Snapshot/Restore. Can be swapped out via `WithStorage(...)`.
- client: Go client helper that reads the `leader-address` response header from followers and retries against the leader with backoff.
- examples: Single-node bootstrap and 3-node in-process cluster.

Request flow (Raft enabled):

1) Client calls Set/Delete on any node.
2) If node is follower: it returns UNAVAILABLE with `leader-address` header. Client retries on the leader.
3) Leader serializes command and calls `raft.Apply`.
4) Once committed, the FSM applies the command to the in-memory store on each node.
5) Response is returned to the client.

---

## Quick start (Windows, build and run the executable)

This quick start uses a single executable and avoids `go run`. Build once, then run nodes in separate terminals.

Build from repo root:

```cmd
cd path\to\dkvs
go build -o node.exe _examples\raft_cluster\main.go
```

Single-node (bootstrap) — one terminal:

```cmd
.\node.exe -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap
```

3-node cluster — three terminals (followers first, then leader):

```cmd
:: Terminal A (node1)
node.exe -id=node1 -raft-addr=127.0.0.1:12101 -grpc=:50051 -data=./data/node1

:: Terminal B (node2)
node.exe -id=node2 -raft-addr=127.0.0.1:12102 -grpc=:50052 -data=./data/node2

:: Terminal C (leader)
node.exe -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap ^
	-voter id=node1,addr=127.0.0.1:12101 ^
	-voter id=node2,addr=127.0.0.1:12102
```

Notes:
- Use forward slashes in `-data=./data/nodeX` to avoid shell escaping issues.
- Press Ctrl+C in a node’s terminal to stop it gracefully (releases file locks on Windows).

---

## Quick start (macOS/Linux)

Build from repo root:

```bash
cd path/to/dkvs
go build -o node _examples/raft_cluster/main.go
```

Single-node (bootstrap) — one terminal:

```bash
./node -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap
```

3-node cluster — three terminals (followers first, then leader):

```bash
# Terminal A (node1)
./node -id=node1 -raft-addr=127.0.0.1:12101 -grpc=:50051 -data=./data/node1

# Terminal B (node2)
./node -id=node2 -raft-addr=127.0.0.1:12102 -grpc=:50052 -data=./data/node2

# Terminal C (leader)
./node -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap \
	-voter id=node1,addr=127.0.0.1:12101 \
	-voter id=node2,addr=127.0.0.1:12102
```

Test with grpcurl (writes to leader, reads anywhere):

```bash
# Write (leader port, e.g., 50050)
grpcurl -plaintext -d '{"key":"k","value":"v"}' localhost:50050 proto.KVStore/Set

# Read (any node)
grpcurl -plaintext -d '{"key":"k"}' localhost:50052 proto.KVStore/Get

# If you hit a follower for a write, add -v and look for the response header leader-address
grpcurl -plaintext -v -d '{"key":"k","value":"v"}' localhost:50051 proto.KVStore/Set
```

Test with grpcurl (writes to leader, reads anywhere):

PowerShell examples:

```powershell
# Write (leader port, e.g., 50050)
grpcurl -plaintext -d '{"key":"k","value":"v"}' localhost:50050 proto.KVStore/Set

# Read (any node)
grpcurl -plaintext -d '{"key":"k"}' localhost:50052 proto.KVStore/Get

# If you hit a follower for a write, add -v and look for the response header leader-address
grpcurl -plaintext -v -d '{"key":"k","value":"v"}' localhost:50051 proto.KVStore/Set
```

cmd.exe examples (escape quotes inside JSON):

```cmd
grpcurl -plaintext -d "{\"key\":\"k\",\"value\":\"v\"}" localhost:50050 proto.KVStore/Set
grpcurl -plaintext -d "{\"key\":\"k\"}" localhost:50052 proto.KVStore/Get
```

If you enabled auth with `-auth <TOKEN>`, add a header:

```powershell
grpcurl -plaintext -H "authorization: Bearer YOUR_TOKEN" -d '{"key":"k","value":"v"}' localhost:50050 proto.KVStore/Set
```

If you prefer the Go helper client, a sketch looks like:

```go
package main

import (
	"context"
	"log"
	"github.com/ahmad-masud/dkvs/client"
)

func main() {
	c := client.New("127.0.0.1:50051")
	defer c.Close()

	ctx := context.Background()
	if err := c.Set(ctx, "hello", "world", 0); err != nil { log.Fatal(err) }
	val, ok, err := c.Get(ctx, "hello")
	if err != nil { log.Fatal(err) }
	log.Printf("hello=%s found=%v", val, ok)
}
```

---

## Run a 3-node example

Use the CLI under `_examples/raft_cluster` (as shown in Quick Starts). Build the single executable and start three terminals with unique `-id`, `-raft-addr`, `-grpc`, and `-data` paths. Start followers first, then the bootstrap leader. On Windows, prefer the executable method shown above over `go run`.

---

## Build from source

Prerequisites:
- Go 1.20+
- git

From the repo root:

```cmd
cd path\to\dkvs
go mod tidy
go build ./...
go test ./...
```

---

## gRPC API and proto

The service is defined in `proto/kvstore.proto`.

RPCs:
- `Set(SetRequest) returns (SetResponse)`
- `Get(GetRequest) returns (GetResponse)`
- `Delete(DeleteRequest) returns (DeleteResponse)`

Important behavior with Raft enabled:
- Followers reject writes with `UNAVAILABLE` and set a `leader-address` header; clients should retry against that leader.

Metadata headers:
- `leader-address`: provided by followers to help clients redirect to leader.
- `x-replicated`: internal header to avoid loops in non-Raft peer replication mode.
- `authorization`: set to `Bearer <token>` if `WithAuthToken` is used.

---

## Client usage (Go helper)

The `client/` package wraps the gRPC client and implements leader-follow and retry with jittered exponential backoff.

Basic usage:

```go
c, _ := client.New("127.0.0.1:50051")
defer c.Close()
c.Set(ctx, "k", "v", 0)
v, ok, _ := c.Get(ctx, "k")
```

With Bearer token auth:

```go
md := metadata.Pairs("authorization", "Bearer secret")
ctx := metadata.NewOutgoingContext(ctx, md)
c.Set(ctx, "k", "v", 0)
```

---

## Configuration options

Set via functional options in `server/options.go` when calling `server.NewServer(...)`:

- `WithStorage(storage kvstore.Storage)`: replace the default in-memory store.
- `WithDefaultTTL(d time.Duration)`: default TTL for keys when not specified.
- `WithPeers([]string)`: peer addresses for naive replication mode (non-Raft).
- `WithRaft(dataDir, nodeID, bindAddr string, peers []string, bootstrap bool)`: enable and configure Raft.
- `WithSnapshotThreshold(n int)`: trigger snapshots after N applied entries (0 = disabled).
- `WithAuthToken(token string)`: require `authorization: Bearer <token>` on all RPCs.
- `WithMetricsAddr(addr string)`: start an HTTP metrics endpoint on `addr` (Prometheus).
- `WithTLS(certFile, keyFile string)`: TLS placeholders — wire as needed for production.

---

## Authentication (Bearer token)

Use `WithAuthToken("secret")` on the server. Clients must set:

```
authorization: Bearer secret
```

This is simple and useful for trusted environments. For untrusted networks, prefer TLS/mTLS or an identity provider.

---

## Observability (Prometheus metrics)

Enable with `WithMetricsAddr(":9100")`. The server exposes `/metrics` and publishes:

- `kvstore_requests_total{method, status}` — counter of RPCs by method and gRPC status.
- `kvstore_request_duration_seconds{method}` — histogram of handler latency.

Example:

```cmd
curl http://127.0.0.1:9100/metrics
```

If embedding in a larger app, consider a custom Prometheus registry to avoid duplicate registrations.

---

## Snapshots and compaction

The FSM supports `Snapshot()` and `Restore()` using the underlying store’s snapshot interface. Use `WithSnapshotThreshold(N)` to periodically trigger snapshots and compact the Raft log.

Notes:
- Snapshots reduce recovery time and log size.
- Set threshold to 0 to disable automatic snapshots.

---

## Tests and verification

Run all tests:

```cmd
cd path\to\dkvs
go test ./...
```

Notable tests:
- `server/raft_integration_test.go`: spins up an ephemeral 3-node cluster and verifies replication.
- `server/auth_metrics_test.go`: validates Bearer token auth and metrics instrumentation.
- `kvstore` package: unit tests for store behavior and TTLs.

---

## Troubleshooting and Windows notes

- BoltDB file locks: Always call `Server.Shutdown()` to close Raft stores, especially on Windows, to avoid temp dir cleanup errors.
- Port collisions: Examples may fail if ports are in use; ensure unique ports or stop conflicting processes.
- Leader redirects: If you target a follower for writes, the server returns UNAVAILABLE with a `leader-address` header — the client helper follows this automatically.

---

## Production considerations and next steps

- TLS/mTLS for gRPC and the metrics server if exposed beyond trusted networks.
- Stronger auth (mTLS, JWT, OIDC) instead of a static Bearer token.
- Admin/ops endpoints (e.g., `/join`) for member management.
- Backups for the Raft data directory and disaster recovery exercises.
- More metrics and alerting (e.g., Raft health, snapshot frequency, store size).

---

## Repository layout

- `server/` — gRPC server, Raft init, handlers, options.
- `kvstore/` — in-memory store and snapshot/restore.
- `client/` — leader-aware Go client helper.
- `_examples/raft_cluster/` — CLI to run single-node or multi-node clusters (kept out of GoDoc).
- `proto/` — protobuf definitions and generated stubs.

---

## Contributing

Contributions are welcome. Please:
- Keep changes focused and add tests for new behavior.
- Run `go test ./...` locally before submitting.

---

## License

See `LICENSE` in the repository root.
