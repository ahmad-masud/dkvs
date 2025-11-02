package client

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/ahmad-masud/dkvs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Client is a small helper that wraps a gRPC dkvs client and transparently
// follows leader redirects when Raft is enabled on the server.
type Client struct {
	mu         sync.Mutex
	addr       string
	conn       *grpc.ClientConn
	rpc        proto.KVStoreClient
	timeout    time.Duration
	maxRetries int
	dialOpts   []grpc.DialOption
}

// Option configures the client.
type Option func(*Client)

// WithTimeout sets the per-RPC timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// WithRetries sets the maximum number of retries following leader redirects.
func WithRetries(n int) Option {
	return func(c *Client) { c.maxRetries = n }
}

// New creates a new Client targeting the given address.
func New(addr string, opts ...Option) *Client {
	c := &Client{
		addr:       addr,
		timeout:    3 * time.Second,
		maxRetries: 3,
		dialOpts:   []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	for _, o := range opts {
		o(c)
	}
	// seed rand for jitter
	rand.Seed(time.Now().UnixNano())
	return c
}

func backoffDuration(attempt int) time.Duration {
	base := 100 * time.Millisecond
	// exponential backoff with cap
	max := 2 * time.Second
	d := base * time.Duration(1<<attempt)
	if d > max {
		d = max
	}
	// add jitter up to base
	j := time.Duration(rand.Intn(int(base.Milliseconds()))) * time.Millisecond
	return d + j
}

func (c *Client) dial(target string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && c.addr == target {
		return nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.rpc = nil
	}
	// Use the non-deprecated client constructor and secure-by-default options.
	// Avoid blocking dials; let calls fail fast if the connection isn't ready yet.
	conn, err := grpc.NewClient(target, c.dialOpts...)
	if err != nil {
		return err
	}
	c.conn = conn
	c.addr = target
	c.rpc = proto.NewKVStoreClient(conn)
	return nil
}

// parseLeader tries to extract a leader address from an UNAVAILABLE error message.
// The server currently returns: "not leader; leader is %s"
// parseLeaderFromHeader extracts leader address from response header metadata if present.
func parseLeaderFromHeader(md metadata.MD) (string, bool) {
	if md == nil {
		return "", false
	}
	if vals := md.Get("leader-address"); len(vals) > 0 {
		return vals[0], true
	}
	return "", false
}

func parseLeaderFallback(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.Unavailable {
		return "", false
	}
	msg := st.Message()
	const marker = "leader is "
	if idx := strings.Index(msg, marker); idx >= 0 {
		leader := strings.TrimSpace(msg[idx+len(marker):])
		return leader, leader != ""
	}
	return "", false
}

// Set stores a key -> value with optional ttl (seconds). It follows leader redirects.
func (c *Client) Set(ctx context.Context, key, value string, ttl int64) error {
	var lastErr error
	target := c.addr
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if err := c.dial(target); err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rpcCtx := ctx
		if c.timeout > 0 {
			var cancel context.CancelFunc
			rpcCtx, cancel = context.WithTimeout(ctx, c.timeout)
			defer cancel()
		}
		var hdr metadata.MD
		_, err := c.rpc.Set(rpcCtx, &proto.SetRequest{Key: key, Value: value, Ttl: ttl}, grpc.Header(&hdr))
		if err == nil {
			return nil
		}
		lastErr = err
		if leader, ok := parseLeaderFromHeader(hdr); ok && leader != "" && leader != target {
			target = leader
			// backoff before retry
			time.Sleep(backoffDuration(attempt))
			continue
		}
		if leader, ok := parseLeaderFallback(err); ok && leader != "" && leader != target {
			target = leader
			time.Sleep(backoffDuration(attempt))
			continue
		}
		// not a redirect; don't retry
		break
	}
	return lastErr
}

// Get fetches a key (no leader redirect needed for reads in this design).
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	if err := c.dial(c.addr); err != nil {
		return "", false, err
	}
	rpcCtx := ctx
	if c.timeout > 0 {
		var cancel context.CancelFunc
		rpcCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	resp, err := c.rpc.Get(rpcCtx, &proto.GetRequest{Key: key})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// Delete removes a key and follows leader redirects like Set.
func (c *Client) Delete(ctx context.Context, key string) error {
	var lastErr error
	target := c.addr
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if err := c.dial(target); err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rpcCtx := ctx
		if c.timeout > 0 {
			var cancel context.CancelFunc
			rpcCtx, cancel = context.WithTimeout(ctx, c.timeout)
			defer cancel()
		}
		var hdr metadata.MD
		_, err := c.rpc.Delete(rpcCtx, &proto.DeleteRequest{Key: key}, grpc.Header(&hdr))
		if err == nil {
			return nil
		}
		lastErr = err
		if leader, ok := parseLeaderFromHeader(hdr); ok && leader != "" && leader != target {
			target = leader
			time.Sleep(backoffDuration(attempt))
			continue
		}
		if leader, ok := parseLeaderFallback(err); ok && leader != "" && leader != target {
			target = leader
			time.Sleep(backoffDuration(attempt))
			continue
		}
		break
	}
	return lastErr
}

// Close closes any underlying connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.rpc = nil
	c.addr = ""
	return err
}
