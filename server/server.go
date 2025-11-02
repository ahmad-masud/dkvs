package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ahmad-masud/dkvs/kvstore"
	"github.com/ahmad-masud/dkvs/proto"

	"github.com/hashicorp/raft"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Server is a gRPC server that handles key-value store operations.
// It wraps a Storage backend and supports optional hooks for customization.
type Server struct {
	proto.UnimplementedKVStoreServer

	storage    kvstore.Storage
	preHook    PreHookFunc
	postHook   PostHookFunc
	defaultTTL time.Duration
	// cluster peers to replicate writes to
	peers       []string
	peerClients map[string]proto.KVStoreClient
	peerConns   map[string]*grpc.ClientConn
	// Raft configuration
	raftEnabled       bool
	raftDir           string
	raftID            string
	raftBind          string
	raftPeers         []string
	raftBootstrap     bool
	raftInstance      *raft.Raft
	raftStore         io.Closer
	snapshotThreshold int
	// optional features
	authToken   string
	adminAddr   string
	tlsCertFile string
	tlsKeyFile  string
	metricsAddr string
	// prometheus metrics (optional)
	requestCounter *prometheus.CounterVec
	requestLatency *prometheus.HistogramVec
}

// NewServer creates a new Server instance with optional functional configuration.
// By default, it uses an in-memory storage backend.
func NewServer(opts ...Option) *Server {
	s := &Server{
		storage: kvstore.New(),
	}
	for _, opt := range opts {
		opt(s)
	}

	// initialize gRPC clients for peers if any were provided via options
	s.initPeerClients()
	// initialize prometheus metrics (always create so tests can inspect)
	if s.requestCounter == nil {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kvstore_requests_total",
			Help: "Total number of gRPC requests handled",
		}, []string{"method", "status"})
		if err := prometheus.Register(c); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
					c = existing
				}
			} else {
				logger.WithField("subsys", "metrics").WithError(err).Warn("failed to register requestCounter")
			}
		}
		s.requestCounter = c
	}
	if s.requestLatency == nil {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kvstore_request_duration_seconds",
			Help:    "gRPC request duration in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"})
		if err := prometheus.Register(h); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
					h = existing
				}
			} else {
				logger.WithField("subsys", "metrics").WithError(err).Warn("failed to register requestLatency")
			}
		}
		s.requestLatency = h
	}
	if s.raftEnabled {
		// initialize raft node (non-fatal: errors will be logged)
		if err := s.initRaft(); err != nil {
			logger.WithField("subsys", "raft").WithError(err).Error("failed to initialize raft")
		}
	}
	// setup basic auth pre-hook if token provided
	if s.authToken != "" {
		orig := s.preHook
		s.preHook = func(ctx context.Context, method string, req interface{}) error {
			md, _ := metadata.FromIncomingContext(ctx)
			vals := md.Get("authorization")
			if len(vals) == 0 || vals[0] != ("Bearer "+s.authToken) {
				return status.Error(codes.Unauthenticated, "missing or invalid auth token")
			}
			if orig != nil {
				return orig(ctx, method, req)
			}
			return nil
		}
		logger.WithField("subsys", "auth").Info("Bearer token auth enabled")
	}

	// start metrics HTTP endpoint if requested
	if s.metricsAddr != "" {
		go func(addr string) {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			logger.WithField("subsys", "metrics").Infof("metrics HTTP listening on %s", addr)
			if err := http.ListenAndServe(addr, mux); err != nil {
				logger.WithField("subsys", "metrics").WithError(err).Error("metrics server stopped")
			}
		}(s.metricsAddr)
	}
	logger.WithFields(map[string]interface{}{
		"subsys":     "server",
		"raft":       s.raftEnabled,
		"raft_dir":   s.raftDir,
		"raft_id":    s.raftID,
		"raft_bind":  s.raftBind,
		"snapshot_n": s.snapshotThreshold,
		"metrics":    s.metricsAddr,
		"auth":       s.authToken != "",
	}).Info("server initialized")
	return s
}

// initPeerClients dials peer addresses and stores gRPC clients for replication.
func (s *Server) initPeerClients() {
	if len(s.peers) == 0 {
		return
	}
	s.peerClients = make(map[string]proto.KVStoreClient)
	s.peerConns = make(map[string]*grpc.ClientConn)
	for _, addr := range s.peers {
		// Dial with a short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err != nil {
			logger.WithFields(map[string]interface{}{"subsys": "peer", "peer": addr}).WithError(err).Warn("failed to dial peer")
			continue
		}
		s.peerConns[addr] = conn
		s.peerClients[addr] = proto.NewKVStoreClient(conn)
	}
}

// Set stores a key-value pair into the storage backend, optionally applying a TTL (time-to-live).
// If a PreHookFunc is set, it runs before the operation.
// If a PostHookFunc is set, it runs after a successful operation.
func (s *Server) Set(ctx context.Context, req *proto.SetRequest) (resp *proto.SetResponse, err error) {
	start := time.Now()
	defer func() {
		statusLabel := codes.OK.String()
		if err != nil {
			statusLabel = status.Code(err).String()
		}
		if s.requestCounter != nil {
			s.requestCounter.WithLabelValues("Set", statusLabel).Inc()
		}
		if s.requestLatency != nil {
			s.requestLatency.WithLabelValues("Set").Observe(time.Since(start).Seconds())
		}
	}()

	if s.preHook != nil {
		if err := s.preHook(ctx, "Set", req); err != nil {
			return nil, err
		}
	}

	// Detect whether this request is already a replication from a peer.
	md, _ := metadata.FromIncomingContext(ctx)
	replicated := false
	if vals := md.Get("x-replicated"); len(vals) > 0 && vals[0] == "true" {
		replicated = true
	}

	logger.WithFields(map[string]interface{}{
		"subsys":     "handler",
		"method":     "Set",
		"key":        req.Key,
		"ttl":        req.Ttl,
		"replicated": replicated,
	}).Debug("handling Set")

	// If Raft is enabled, submit the command to Raft (leader handles replication).
	if s.raftInstance != nil {
		if s.raftInstance.State() != raft.Leader {
			leader := string(s.raftInstance.Leader())
			// Include leader address in response header so clients can follow redirects
			_ = grpc.SetHeader(ctx, metadata.Pairs("leader-address", leader))
			return nil, status.Errorf(codes.Unavailable, "not leader")
		}
		// Prepare command
		cmd := command{Op: "set", Key: req.Key, Value: req.Value, TTL: req.Ttl}
		b, err := json.Marshal(cmd)
		if err != nil {
			return nil, err
		}
		future := s.raftInstance.Apply(b, 5*time.Second)
		if err := future.Error(); err != nil {
			return nil, err
		}
		logger.WithFields(map[string]interface{}{
			"subsys": "raft",
			"apply":  "set",
			"key":    req.Key,
			"ttl":    req.Ttl,
			"took":   time.Since(start).String(),
		}).Info("applied set via raft")
		resp = &proto.SetResponse{Success: true}
		if s.postHook != nil {
			_ = s.postHook(ctx, "Set", req, resp)
		}
		return resp, nil
	}

	var ttl time.Duration
	if req.Ttl > 0 {
		ttl = time.Duration(req.Ttl) * time.Second
		s.storage.SetWithTTL(req.Key, req.Value, ttl)
	} else if s.defaultTTL > 0 {
		ttl = s.defaultTTL
		s.storage.SetWithTTL(req.Key, req.Value, ttl)
	} else {
		s.storage.Set(req.Key, req.Value)
	}

	resp = &proto.SetResponse{Success: true}

	// Post hook
	if s.postHook != nil {
		_ = s.postHook(ctx, "Set", req, resp)
	}

	// If this was not a replicated request, broadcast to peers asynchronously (non-raft mode).
	if s.raftInstance == nil {
		if !replicated && len(s.peerClients) > 0 {
			go s.replicateSet(req)
		}
	}

	return resp, nil
}

// Get retrieves the value for a given key from the storage backend.
// If a PreHookFunc is set, it runs before the operation.
// If a PostHookFunc is set, it runs after retrieving the value.
func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (resp *proto.GetResponse, err error) {
	start := time.Now()
	defer func() {
		statusLabel := codes.OK.String()
		if err != nil {
			statusLabel = status.Code(err).String()
		}
		if s.requestCounter != nil {
			s.requestCounter.WithLabelValues("Get", statusLabel).Inc()
		}
		if s.requestLatency != nil {
			s.requestLatency.WithLabelValues("Get").Observe(time.Since(start).Seconds())
		}
	}()

	if s.preHook != nil {
		if err := s.preHook(ctx, "Get", req); err != nil {
			return nil, err
		}
	}

	logger.WithFields(map[string]interface{}{
		"subsys": "handler",
		"method": "Get",
		"key":    req.Key,
	}).Debug("handling Get")

	value, found := s.storage.Get(req.Key)

	resp = &proto.GetResponse{
		Value: value,
		Found: found,
	}

	if s.postHook != nil {
		_ = s.postHook(ctx, "Get", req, resp)
	}

	return resp, nil
}

// Delete removes a key-value pair from the storage backend.
// If a PreHookFunc is set, it runs before the operation.
// If a PostHookFunc is set, it runs after a successful deletion.
func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (resp *proto.DeleteResponse, err error) {
	start := time.Now()
	defer func() {
		statusLabel := codes.OK.String()
		if err != nil {
			statusLabel = status.Code(err).String()
		}
		if s.requestCounter != nil {
			s.requestCounter.WithLabelValues("Delete", statusLabel).Inc()
		}
		if s.requestLatency != nil {
			s.requestLatency.WithLabelValues("Delete").Observe(time.Since(start).Seconds())
		}
	}()

	if s.preHook != nil {
		if err := s.preHook(ctx, "Delete", req); err != nil {
			return nil, err
		}
	}

	md, _ := metadata.FromIncomingContext(ctx)
	replicated := false
	if vals := md.Get("x-replicated"); len(vals) > 0 && vals[0] == "true" {
		replicated = true
	}

	logger.WithFields(map[string]interface{}{
		"subsys":     "handler",
		"method":     "Delete",
		"key":        req.Key,
		"replicated": replicated,
	}).Debug("handling Delete")

	// If Raft is enabled, submit delete via Raft (leader handles replication)
	if s.raftInstance != nil {
		if s.raftInstance.State() != raft.Leader {
			leader := string(s.raftInstance.Leader())
			// Include leader address in response header so clients can follow redirects
			_ = grpc.SetHeader(ctx, metadata.Pairs("leader-address", leader))
			return nil, status.Errorf(codes.Unavailable, "not leader")
		}
		cmd := command{Op: "delete", Key: req.Key}
		b, err := json.Marshal(cmd)
		if err != nil {
			return nil, err
		}
		future := s.raftInstance.Apply(b, 5*time.Second)
		if err := future.Error(); err != nil {
			return nil, err
		}
		logger.WithFields(map[string]interface{}{
			"subsys": "raft",
			"apply":  "delete",
			"key":    req.Key,
			"took":   time.Since(start).String(),
		}).Info("applied delete via raft")
		resp = &proto.DeleteResponse{Success: true}
		if s.postHook != nil {
			_ = s.postHook(ctx, "Delete", req, resp)
		}
		return resp, nil
	}

	success := s.storage.Delete(req.Key)

	resp = &proto.DeleteResponse{
		Success: success,
	}

	if s.postHook != nil {
		_ = s.postHook(ctx, "Delete", req, resp)
	}

	if !replicated && len(s.peerClients) > 0 {
		go s.replicateDelete(req)
	}

	return resp, nil
}

// replicateSet calls Set on all known peers with a replicated metadata header to avoid loops.
func (s *Server) replicateSet(req *proto.SetRequest) {
	for addr, client := range s.peerClients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		md := metadata.Pairs("x-replicated", "true")
		ctx = metadata.NewOutgoingContext(ctx, md)
		_, err := client.Set(ctx, req)
		cancel()
		if err != nil {
			logger.WithFields(map[string]interface{}{"subsys": "replicate", "peer": addr}).WithError(err).Warn("replicate Set failed")
		}
	}
}

// replicateDelete calls Delete on all known peers with replicated header.
func (s *Server) replicateDelete(req *proto.DeleteRequest) {
	for addr, client := range s.peerClients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		md := metadata.Pairs("x-replicated", "true")
		ctx = metadata.NewOutgoingContext(ctx, md)
		_, err := client.Delete(ctx, req)
		cancel()
		if err != nil {
			logger.WithFields(map[string]interface{}{"subsys": "replicate", "peer": addr}).WithError(err).Warn("replicate Delete failed")
		}
	}
}

// Listen starts the gRPC server on the specified TCP address (e.g., ":50051").
// It registers the KVStore service and begins serving incoming requests.
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(loggingInterceptor()))
	proto.RegisterKVStoreServer(grpcServer, s)

	reflection.Register(grpcServer)

	// Setup signal handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run gRPC server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(lis)
	}()

	logger.WithField("subsys", "grpc").Infof("KVStore gRPC listening on %s", addr)

	// Wait for signal
	select {
	case <-ctx.Done():
		logger.WithField("subsys", "grpc").Info("shutdown signal received; stopping gRPC server")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// GetLeader returns the current raft leader address as string (empty if no raft configured).
func (s *Server) GetLeader() string {
	if s.raftInstance == nil {
		return ""
	}
	return string(s.raftInstance.Leader())
}

// AddVoter requests the Raft leader to add a new voter to the cluster.
// This should be called on the leader node.
func (s *Server) AddVoter(peerID, peerAddr string) error {
	if s.raftInstance == nil {
		return fmt.Errorf("raft not enabled")
	}
	future := s.raftInstance.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerAddr), 0, 0)
	return future.Error()
}

// HasVoter checks if a server with the given ID or address is already in the Raft configuration.
// Returns true if present. Safe to call from any node.
func (s *Server) HasVoter(peerID, peerAddr string) bool {
	if s.raftInstance == nil {
		return false
	}
	f := s.raftInstance.GetConfiguration()
	if err := f.Error(); err != nil {
		logger.WithField("subsys", "raft").WithError(err).Warn("GetConfiguration error")
		return false
	}
	cfg := f.Configuration()
	for _, srv := range cfg.Servers {
		if string(srv.ID) == peerID || string(srv.Address) == peerAddr {
			return true
		}
	}
	return false
}

// Shutdown gracefully stops the server's raft instance (if any).
func (s *Server) Shutdown() error {
	if s.raftInstance != nil {
		future := s.raftInstance.Shutdown()
		if err := future.Error(); err != nil {
			return err
		}
		if s.raftStore != nil {
			// best effort close
			_ = s.raftStore.Close()
		}
		return nil
	}
	return nil
}
