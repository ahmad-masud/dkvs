package server

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// ClusterConfig holds the parameters to run a KVStore node in a Raft cluster.
type ClusterConfig struct {
	ID        string   // raft node ID
	RaftAddr  string   // raft transport address host:port
	GRPCAddr  string   // gRPC listen address
	DataDir   string   // raft data directory
	Bootstrap bool     // bootstrap this node
	Voters    []string // raw voter specs: id=<id>,addr=<host:port>
	AuthToken string   // optional bearer token
}

// RunClusterNode creates and runs a KVStore server based on ClusterConfig.
// It blocks until the internal gRPC server stops (on signal or error).
// It also performs best-effort AddVoter retries when Bootstrap is true.
func RunClusterNode(cfg ClusterConfig) error {
	if cfg.RaftAddr == "" || !strings.Contains(cfg.RaftAddr, ":") {
		return fmt.Errorf("invalid RaftAddr %q; must be host:port", cfg.RaftAddr)
	}
	if cfg.ID == "" {
		return fmt.Errorf("missing ID")
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":50050"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data/" + cfg.ID
	}

	opts := []Option{
		WithRaft(cfg.DataDir, cfg.ID, cfg.RaftAddr, nil, cfg.Bootstrap),
	}
	if cfg.AuthToken != "" {
		opts = append(opts, WithAuthToken(cfg.AuthToken))
	}

	s := NewServer(opts...)
	logger.WithFields(map[string]interface{}{
		"subsys":    "cluster",
		"id":        cfg.ID,
		"raft":      cfg.RaftAddr,
		"grpc":      cfg.GRPCAddr,
		"data":      cfg.DataDir,
		"bootstrap": cfg.Bootstrap,
		"voters":    len(cfg.Voters),
		"auth":      cfg.AuthToken != "",
	}).Info("cluster node starting")

	// Start AddVoter management if requested
	if cfg.Bootstrap && len(cfg.Voters) > 0 {
		pending := make(map[string]string)
		for _, raw := range cfg.Voters {
			vid, vaddr, err := parseVoterSpec(raw)
			if err != nil {
				logger.WithField("subsys", "cluster").WithError(err).Warnf("invalid voter %q", raw)
				continue
			}
			pending[vid] = vaddr
		}
		if len(pending) > 0 {
			go func() {
				// give raft a moment to stabilize
				time.Sleep(1500 * time.Millisecond)
				backoff := 100 * time.Millisecond
				for len(pending) > 0 {
					// Prune voters already in config
					for vid, vaddr := range pending {
						if s.HasVoter(vid, vaddr) {
							logger.WithFields(map[string]interface{}{"subsys": "cluster", "id": vid, "addr": vaddr}).Info("voter already present; skipping")
							delete(pending, vid)
						}
					}
					if len(pending) == 0 {
						logger.WithField("subsys", "cluster").Info("all specified voters already present; nothing to add")
						break
					}
					// Ensure we are leader
					if leader := s.GetLeader(); leader == "" || leader != cfg.RaftAddr {
						logger.WithFields(map[string]interface{}{"subsys": "cluster", "leader": leader, "pending": len(pending)}).Debug("not leader yet; waiting to add voters")
						time.Sleep(backoff)
						if backoff < 2*time.Second {
							backoff *= 2
							if backoff > 2*time.Second {
								backoff = 2 * time.Second
							}
						}
						continue
					}
					// Try to add any reachable peers
					for vid, vaddr := range pending {
						if c, err := net.DialTimeout("tcp", vaddr, 500*time.Millisecond); err != nil {
							logger.WithFields(map[string]interface{}{"subsys": "cluster", "peer": vaddr}).WithError(err).Debug("peer not reachable yet")
							continue
						} else {
							_ = c.Close()
						}
						logger.WithFields(map[string]interface{}{"subsys": "cluster", "id": vid, "addr": vaddr}).Info("adding voter")
						if err := s.AddVoter(vid, vaddr); err != nil {
							logger.WithFields(map[string]interface{}{"subsys": "cluster", "id": vid}).WithError(err).Warn("AddVoter failed")
							continue
						}
						logger.WithFields(map[string]interface{}{"subsys": "cluster", "id": vid}).Info("added voter")
						delete(pending, vid)
					}
					if len(pending) == 0 {
						break
					}
					time.Sleep(backoff)
					if backoff < 2*time.Second {
						backoff *= 2
						if backoff > 2*time.Second {
							backoff = 2 * time.Second
						}
					}
				}
			}()
		}
	}

	logger.WithFields(map[string]interface{}{"subsys": "cluster", "id": cfg.ID, "grpc": cfg.GRPCAddr, "raft": cfg.RaftAddr, "data": cfg.DataDir, "bootstrap": cfg.Bootstrap}).Info("raft_cluster")
	if cfg.AuthToken != "" {
		logger.WithField("subsys", "cluster").Info("auth: Bearer token required")
	}

	defer func() {
		_ = s.Shutdown()
		logger.WithField("subsys", "cluster").Info("server shutdown complete")
	}()
	return s.Listen(cfg.GRPCAddr)
}

// parseVoterSpec accepts "id=<id>,addr=<host:port>" and returns id and addr.
func parseVoterSpec(s string) (string, string, error) {
	var id, addr string
	parts := strings.Split(s, ",")
	for _, p := range parts {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch strings.ToLower(kv[0]) {
		case "id":
			id = kv[1]
		case "addr":
			addr = kv[1]
		}
	}
	if id == "" || addr == "" {
		return "", "", fmt.Errorf("invalid voter format, want id=<id>,addr=<host:port>: %q", s)
	}
	return id, addr, nil
}

// Debug helper: expose args as a string (useful in logs if desired)
func argsString() string { return strings.Join(os.Args, " ") }
