package server

import (
	"context"
	"testing"
	"time"

	"github.com/ahmad-masud/dkvs/kvstore"
)

func TestWithStorage(t *testing.T) {
	customStore := kvstore.New()

	s := &Server{}
	opt := WithStorage(customStore)
	opt(s)

	if s.storage != customStore {
		t.Fatalf("expected storage to be set")
	}
}

func TestWithPreHook(t *testing.T) {
	hook := func(ctx context.Context, method string, req interface{}) error {
		return nil
	}

	s := &Server{}
	opt := WithPreHook(hook)
	opt(s)

	if s.preHook == nil {
		t.Fatalf("expected preHook to be set")
	}
}

func TestWithPostHook(t *testing.T) {
	hook := func(ctx context.Context, method string, req, resp interface{}) error {
		return nil
	}

	s := &Server{}
	opt := WithPostHook(hook)
	opt(s)

	if s.postHook == nil {
		t.Fatalf("expected postHook to be set")
	}
}

func TestWithDefaultTTL(t *testing.T) {
	ttl := 5 * time.Minute

	s := &Server{}
	opt := WithDefaultTTL(ttl)
	opt(s)

	if s.defaultTTL != ttl {
		t.Fatalf("expected defaultTTL to be set")
	}
}

func TestWithPeers(t *testing.T) {
	peers := []string{"127.0.0.1:50051", "127.0.0.1:50052"}

	s := &Server{}
	opt := WithPeers(peers)
	opt(s)

	if len(s.peers) != len(peers) || s.peers[0] != peers[0] || s.peers[1] != peers[1] {
		t.Fatalf("expected peers to be set: %#v", s.peers)
	}
}

func TestWithRaft(t *testing.T) {
	s := &Server{}
	opt := WithRaft("./data/testnode", "nodeX", "127.0.0.1:12123", nil, true)
	opt(s)

	if !s.raftEnabled {
		t.Fatalf("expected raftEnabled to be true")
	}
	if s.raftDir != "./data/testnode" || s.raftID != "nodeX" || s.raftBind != "127.0.0.1:12123" || !s.raftBootstrap {
		t.Fatalf("unexpected raft fields: dir=%s id=%s bind=%s bootstrap=%v", s.raftDir, s.raftID, s.raftBind, s.raftBootstrap)
	}
}

func TestWithSnapshotThreshold(t *testing.T) {
	s := &Server{}
	opt := WithSnapshotThreshold(42)
	opt(s)
	if s.snapshotThreshold != 42 {
		t.Fatalf("expected snapshotThreshold to be set")
	}
}

func TestWithAuthToken(t *testing.T) {
	s := &Server{}
	opt := WithAuthToken("secret")
	opt(s)
	if s.authToken != "secret" {
		t.Fatalf("expected authToken to be set")
	}
}

func TestWithAdminAddr(t *testing.T) {
	s := &Server{}
	opt := WithAdminAddr(":9090")
	opt(s)
	if s.adminAddr != ":9090" {
		t.Fatalf("expected adminAddr to be set")
	}
}

func TestWithTLS(t *testing.T) {
	s := &Server{}
	opt := WithTLS("cert.pem", "key.pem")
	opt(s)
	if s.tlsCertFile != "cert.pem" || s.tlsKeyFile != "key.pem" {
		t.Fatalf("expected TLS fields to be set")
	}
}
