package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahmad-masud/dkvs/client"
	"github.com/ahmad-masud/dkvs/proto"
	"google.golang.org/grpc"
)

// TestRaftThreeNodeReplication starts a 3-node cluster, writes on the leader,
// and verifies the value is replicated to all nodes.
func TestRaftThreeNodeReplication(t *testing.T) {
	t.Parallel()

	// create temp dir
	baseDir := t.TempDir()

	raftAddrs := make([]string, 3)
	grpcListeners := make([]net.Listener, 3)
	grpcAddrs := make([]string, 3)
	servers := make([]*Server, 3)
	grpcServers := make([]*grpc.Server, 3)

	// prepare addresses and listeners
	for i := 0; i < 3; i++ {
		// pick raft bind port (free ephemeral)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen raft port: %v", err)
		}
		raftAddrs[i] = l.Addr().String()
		l.Close()

		// grpc listener (we keep it open)
		gl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen grpc port: %v", err)
		}
		grpcListeners[i] = gl
		grpcAddrs[i] = gl.Addr().String()
	}

	// start node 0 as bootstrap leader
	for i := 0; i < 3; i++ {
		nodeDir := filepath.Join(baseDir, fmt.Sprintf("node%d", i))
		if err := os.MkdirAll(nodeDir, 0o750); err != nil {
			t.Fatalf("mkdir node dir: %v", err)
		}
		bootstrap := (i == 0)
		servers[i] = NewServer(WithRaft(nodeDir, fmt.Sprintf("node%d", i), raftAddrs[i], nil, bootstrap))

		gs := grpc.NewServer()
		grpcServers[i] = gs
		proto.RegisterKVStoreServer(gs, servers[i])
		go func(idx int) {
			_ = gs.Serve(grpcListeners[idx])
		}(i)
	}

	// wait for raft to initialize
	time.Sleep(2 * time.Second)

	// leader should be node0 (we bootstrapped it)
	leader := servers[0]
	if leader.GetLeader() == "" {
		t.Fatalf("leader not elected")
	}

	// add other nodes as voters
	for i := 1; i < 3; i++ {
		if err := leader.AddVoter(fmt.Sprintf("node%d", i), raftAddrs[i]); err != nil {
			t.Logf("AddVoter node%d failed (may already be part of cluster): %v", i, err)
		}
	}

	// give time for configuration to propagate
	time.Sleep(2 * time.Second)

	// perform a write via client to leader's gRPC endpoint
	cli := client.New(grpcAddrs[0])
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cli.Set(ctx, "integration-key", "integration-value", 0); err != nil {
		t.Fatalf("client Set failed: %v", err)
	}

	// verify replication: poll each node until value appears or timeout
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < 3; i++ {
		found := false
		for time.Now().Before(deadline) {
			c := client.New(grpcAddrs[i])
			v, ok, err := c.Get(ctx, "integration-key")
			if err == nil && ok && v == "integration-value" {
				found = true
				_ = c.Close()
				break
			}
			_ = c.Close()
			time.Sleep(100 * time.Millisecond)
		}
		if !found {
			t.Fatalf("node %d did not replicate value", i)
		}
	}

	// cleanup: shutdown raft instances first to release files, wait briefly, then stop grpc servers
	for i := 0; i < 3; i++ {
		if err := servers[i].Shutdown(); err != nil {
			t.Logf("shutdown node%d raft: %v", i, err)
		}
	}
	// allow raft shutdown to fully release DB files
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		grpcServers[i].GracefulStop()
		_ = grpcListeners[i].Close()
	}
}
