package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ahmad-masud/dkvs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type fakeKV struct {
	proto.UnimplementedKVStoreServer
	mu          sync.Mutex
	store       map[string]string
	role        string // "leader" or "follower"
	leaderAddr  string // where to redirect when follower
	redirectVia string // "header" or "msg"
}

func newFakeKV(role, leaderAddr, redirectVia string) *fakeKV {
	return &fakeKV{store: make(map[string]string), role: role, leaderAddr: leaderAddr, redirectVia: redirectVia}
}

func (f *fakeKV) Set(ctx context.Context, req *proto.SetRequest) (*proto.SetResponse, error) {
	if f.role == "follower" {
		if f.redirectVia == "header" {
			_ = grpc.SetHeader(ctx, metadata.Pairs("leader-address", f.leaderAddr))
			return nil, status.Error(codes.Unavailable, "not leader")
		}
		return nil, status.Errorf(codes.Unavailable, "leader is %s", f.leaderAddr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[req.Key] = req.Value
	return &proto.SetResponse{Success: true}, nil
}

func (f *fakeKV) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[req.Key]
	return &proto.GetResponse{Value: v, Found: ok}, nil
}

func (f *fakeKV) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if f.role == "follower" {
		if f.redirectVia == "header" {
			_ = grpc.SetHeader(ctx, metadata.Pairs("leader-address", f.leaderAddr))
			return nil, status.Error(codes.Unavailable, "not leader")
		}
		return nil, status.Errorf(codes.Unavailable, "leader is %s", f.leaderAddr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.store, req.Key)
	return &proto.DeleteResponse{Success: true}, nil
}

func startTestServer(t *testing.T, impl proto.KVStoreServer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	proto.RegisterKVStoreServer(gs, impl)
	reflection.Register(gs)
	go gs.Serve(lis)
	return lis.Addr().String(), func() {
		gs.GracefulStop()
		_ = lis.Close()
	}
}

func TestClientSetRedirectHeader(t *testing.T) {
	// Start leader
	leaderImpl := newFakeKV("leader", "", "")
	leaderAddr, stopLeader := startTestServer(t, leaderImpl)
	defer stopLeader()

	// Start follower that redirects via header to leader
	followerImpl := newFakeKV("follower", leaderAddr, "header")
	followerAddr, stopFollower := startTestServer(t, followerImpl)
	defer stopFollower()

	c := New(followerAddr, WithTimeout(2*time.Second), WithRetries(5))
	defer c.Close()

	ctx := context.Background()
	if err := c.Set(ctx, "a", "b", 0); err != nil {
		t.Fatalf("Set with header redirect failed: %v", err)
	}

	// Verify value is stored at leader
	getResp, err := leaderImpl.Get(ctx, &proto.GetRequest{Key: "a"})
	if err != nil || !getResp.Found || getResp.Value != "b" {
		t.Fatalf("leader did not store value: resp=%+v err=%v", getResp, err)
	}
}

func TestClientDeleteRedirectFallback(t *testing.T) {
	// Start leader
	leaderImpl := newFakeKV("leader", "", "")
	leaderAddr, stopLeader := startTestServer(t, leaderImpl)
	defer stopLeader()

	// Preload a key
	_, _ = leaderImpl.Set(context.Background(), &proto.SetRequest{Key: "x", Value: "y"})

	// Start follower that redirects via message text (no header)
	followerImpl := newFakeKV("follower", leaderAddr, "msg")
	followerAddr, stopFollower := startTestServer(t, followerImpl)
	defer stopFollower()

	c := New(followerAddr, WithTimeout(2*time.Second), WithRetries(5))
	defer c.Close()

	if err := c.Delete(context.Background(), "x"); err != nil {
		t.Fatalf("Delete with fallback redirect failed: %v", err)
	}

	// Ensure key gone
	getResp2, err := leaderImpl.Get(context.Background(), &proto.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get after delete err: %v", err)
	}
	if getResp2.Found {
		t.Fatalf("expected key to be deleted, still found")
	}
}

func TestClientGetBasic(t *testing.T) {
	leaderImpl := newFakeKV("leader", "", "")
	leaderAddr, stopLeader := startTestServer(t, leaderImpl)
	defer stopLeader()

	// Put directly on leader (bypasses client) to set a value
	_, _ = leaderImpl.Set(context.Background(), &proto.SetRequest{Key: "k", Value: "v"})

	c := New(leaderAddr, WithTimeout(2*time.Second))
	defer c.Close()

	v, ok, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !ok || v != "v" {
		t.Fatalf("unexpected Get result: v=%q ok=%v", v, ok)
	}
}
