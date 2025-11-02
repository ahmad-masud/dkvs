package server

import (
	"time"

	"github.com/ahmad-masud/dkvs/kvstore"
)

// Option configures the Server.
type Option func(*Server)

// WithStorage allows injecting a custom storage backend.
func WithStorage(storage kvstore.Storage) Option {
	return func(s *Server) {
		s.storage = storage
	}
}

// WithPreHook sets a hook that runs before every operation.
func WithPreHook(hook PreHookFunc) Option {
	return func(s *Server) {
		s.preHook = hook
	}
}

// WithPostHook sets a hook that runs after every successful operation.
func WithPostHook(hook PostHookFunc) Option {
	return func(s *Server) {
		s.postHook = hook
	}
}

// WithDefaultTTL sets a default TTL for keys if none is specified.
func WithDefaultTTL(ttl time.Duration) Option {
	return func(s *Server) {
		s.defaultTTL = ttl
	}
}

// WithPeers configures peer addresses for replication (e.g. []string{"localhost:50052", "localhost:50053"}).
func WithPeers(peers []string) Option {
	return func(s *Server) {
		s.peers = peers
	}
}

// WithRaft configures Raft for the server. Provide a data directory, node ID, bind address
// for Raft transport and initial peer addresses (if any).
func WithRaft(dataDir, nodeID, bindAddr string, peers []string, bootstrap bool) Option {
	return func(s *Server) {
		s.raftEnabled = true
		s.raftDir = dataDir
		s.raftID = nodeID
		s.raftBind = bindAddr
		s.raftPeers = peers
		s.raftBootstrap = bootstrap
	}
}

// WithSnapshotThreshold sets the number of applied log entries after which the
// server will trigger a Raft snapshot. Set to 0 to disable automatic snapshots.
func WithSnapshotThreshold(th int) Option {
	return func(s *Server) {
		s.snapshotThreshold = th
	}
}

// WithAuthToken sets a simple bearer token that will be required on all RPCs.
// The server will reject requests missing `authorization: Bearer <token>` metadata.
func WithAuthToken(token string) Option {
	return func(s *Server) {
		s.authToken = token
	}
}

// WithAdminAddr configures an admin HTTP address for control endpoints (e.g., /join).
func WithAdminAddr(addr string) Option {
	return func(s *Server) {
		s.adminAddr = addr
	}
}

// WithTLS enables TLS for the gRPC server using the provided certificate and key files.
func WithTLS(certFile, keyFile string) Option {
	return func(s *Server) {
		s.tlsCertFile = certFile
		s.tlsKeyFile = keyFile
	}
}

// WithMetricsAddr enables Prometheus metrics to be exposed on the given address.
func WithMetricsAddr(addr string) Option {
	return func(s *Server) {
		s.metricsAddr = addr
	}
}
