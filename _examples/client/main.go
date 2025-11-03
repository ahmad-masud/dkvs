package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ahmad-masud/dkvs/proto"
)

func main() {
	var addr string
	var key string
	var value string
	flag.StringVar(&addr, "addr", "localhost:50050", "gRPC address of a node")
	flag.StringVar(&key, "key", "example-key", "key to set/get")
	flag.StringVar(&value, "value", "hello-world", "value to set")
	flag.Parse()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s: %v", addr, err)
	}
	defer conn.Close()

	client := proto.NewKVStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Perform Set
	setResp, err := client.Set(ctx, &proto.SetRequest{Key: key, Value: value})
	if err != nil {
		log.Fatalf("Set RPC failed: %v", err)
	}
	fmt.Printf("Set success: %v\n", setResp.GetSuccess())

	// Perform Get
	getResp, err := client.Get(ctx, &proto.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Get RPC failed: %v", err)
	}
	if getResp.GetFound() {
		fmt.Printf("Get value: %s\n", getResp.GetValue())
	} else {
		fmt.Printf("Key not found: %s\n", key)
	}
}
