package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ahmad-masud/dkvs/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func startTestServer(t *testing.T) (proto.KVStoreClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	s := NewServer()

	grpcServer := grpc.NewServer()
	proto.RegisterKVStoreServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server exited: %v", err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := proto.NewKVStoreClient(conn)

	return client, func() {
		conn.Close()
		grpcServer.Stop()
	}
}

func startTestServerWithOptions(t *testing.T, opts ...Option) (proto.KVStoreClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	s := NewServer(opts...)

	grpcServer := grpc.NewServer()
	proto.RegisterKVStoreServer(grpcServer, s)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server exited: %v", err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := proto.NewKVStoreClient(conn)

	return client, func() {
		conn.Close()
		grpcServer.Stop()
	}
}

func TestServer_SetAndGet(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Set key
	_, err := client.Set(ctx, &proto.SetRequest{
		Key:   "foo",
		Value: "bar",
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get key
	resp, err := client.Get(ctx, &proto.GetRequest{
		Key: "foo",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !resp.Found || resp.Value != "bar" {
		t.Fatalf("unexpected Get response: %+v", resp)
	}
}

func TestServer_Delete(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Set key
	_, err := client.Set(ctx, &proto.SetRequest{
		Key:   "foo",
		Value: "bar",
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete key
	delResp, err := client.Delete(ctx, &proto.DeleteRequest{
		Key: "foo",
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !delResp.Success {
		t.Fatalf("expected Delete to succeed")
	}

	// Try getting deleted key
	getResp, err := client.Get(ctx, &proto.GetRequest{
		Key: "foo",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if getResp.Found {
		t.Fatalf("expected key to be deleted, but found: %+v", getResp)
	}
}

func TestServer_TTLExpiration(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Set key with short TTL
	_, err := client.Set(ctx, &proto.SetRequest{
		Key:   "baz",
		Value: "qux",
		Ttl:   1, // expires in 1 second
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Immediately check
	getResp, err := client.Get(ctx, &proto.GetRequest{
		Key: "baz",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !getResp.Found || getResp.Value != "qux" {
		t.Fatalf("unexpected immediate Get: %+v", getResp)
	}

	// Wait for TTL to expire
	time.Sleep(2 * time.Second)

	getResp, err = client.Get(ctx, &proto.GetRequest{
		Key: "baz",
	})
	if err != nil {
		t.Fatalf("Get failed after sleep: %v", err)
	}
	if getResp.Found {
		t.Fatalf("expected key to expire, but found: %+v", getResp)
	}
}

func TestServer_DefaultTTL(t *testing.T) {
	client, cleanup := startTestServerWithOptions(t, WithDefaultTTL(1*time.Second))
	defer cleanup()

	ctx := context.Background()
	_, err := client.Set(ctx, &proto.SetRequest{Key: "dttl", Value: "x", Ttl: 0})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// Should be present immediately
	r1, err := client.Get(ctx, &proto.GetRequest{Key: "dttl"})
	if err != nil || !r1.Found {
		t.Fatalf("expected found immediately, err=%v resp=%+v", err, r1)
	}
	// After ~1.2s should expire
	time.Sleep(1200 * time.Millisecond)
	r2, err := client.Get(ctx, &proto.GetRequest{Key: "dttl"})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if r2.Found {
		t.Fatalf("expected key to expire with default TTL")
	}
}

func TestServer_AuthEnforcement(t *testing.T) {
	client, cleanup := startTestServerWithOptions(t, WithAuthToken("secret"))
	defer cleanup()

	ctx := context.Background()
	// Without auth header
	_, err := client.Set(ctx, &proto.SetRequest{Key: "a", Value: "b"})
	if err == nil {
		t.Fatalf("expected unauthenticated error")
	}
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got: %v", err)
	}
	// With correct auth header
	md := metadata.Pairs("authorization", "Bearer secret")
	ctx2 := metadata.NewOutgoingContext(ctx, md)
	_, err = client.Set(ctx2, &proto.SetRequest{Key: "a", Value: "b"})
	if err != nil {
		t.Fatalf("Set with auth failed: %v", err)
	}
}

func TestServer_Hooks(t *testing.T) {
	blockedErr := status.Error(codes.PermissionDenied, "blocked")
	preCalled := 0
	postCalled := 0
	pre := func(ctx context.Context, method string, req interface{}) error {
		preCalled++
		return blockedErr
	}
	post := func(ctx context.Context, method string, req, resp interface{}) error {
		postCalled++
		return nil
	}
	client, cleanup := startTestServerWithOptions(t, WithPreHook(pre), WithPostHook(post))
	defer cleanup()

	ctx := context.Background()
	_, err := client.Set(ctx, &proto.SetRequest{Key: "hk", Value: "hv"})
	if err == nil {
		t.Fatalf("expected error from preHook")
	}
	if preCalled != 1 {
		t.Fatalf("expected preHook called once, got %d", preCalled)
	}
	if postCalled != 0 {
		t.Fatalf("postHook should not be called on failure")
	}

	// Now allow and verify post hook runs
	preCalled = 0
	postCalled = 0
	preAllow := func(ctx context.Context, method string, req interface{}) error { preCalled++; return nil }
	postCount := func(ctx context.Context, method string, req, resp interface{}) error { postCalled++; return nil }
	client2, cleanup2 := startTestServerWithOptions(t, WithPreHook(preAllow), WithPostHook(postCount))
	defer cleanup2()
	_, err = client2.Set(ctx, &proto.SetRequest{Key: "hk2", Value: "hv2"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if preCalled != 1 || postCalled != 1 {
		t.Fatalf("expected pre=1 post=1, got pre=%d post=%d", preCalled, postCalled)
	}
}
