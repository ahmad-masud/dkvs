package main

import (
	"flag"
	"log"
	"strings"

	"github.com/ahmad-masud/dkvs/server"
)

// Minimal runner: parse flags and hand off to server.RunClusterNode.

type voterFlags []string

func (v *voterFlags) String() string     { return strings.Join(*v, ",") }
func (v *voterFlags) Set(s string) error { *v = append(*v, s); return nil }

func main() {
	var (
		id        = flag.String("id", "node0", "raft node ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:12100", "raft transport address host:port")
		grpcAddr  = flag.String("grpc", ":50050", "gRPC listen address")
		dataDir   = flag.String("data", "./data/node0", "data directory for raft state")
		bootstrap = flag.Bool("bootstrap", false, "bootstrap this node as initial cluster")
		authToken = flag.String("auth", "", "optional bearer token required for RPCs")
	)
	var voters voterFlags
	flag.Var(&voters, "voter", "repeated voter entries: id=<id>,addr=<host:port> (leader only)")
	flag.Parse()

	cfg := server.ClusterConfig{
		ID:        *id,
		RaftAddr:  *raftAddr,
		GRPCAddr:  *grpcAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		Voters:    voters,
		AuthToken: *authToken,
	}
	if err := server.RunClusterNode(cfg); err != nil {
		log.Fatal(err)
	}
}
