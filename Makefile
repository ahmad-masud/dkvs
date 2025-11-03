# Makefile for dkvs project

.PHONY: test build clean vet fmt start-nodes build-node client run-client reset-data start-fresh tail-logs

# Build the project (optional, if you add a real app later)
build:
	protoc --go_out=paths=source_relative:proto --go-grpc_out=paths=source_relative:proto --proto_path=proto proto/kvstore.proto
	go mod tidy
	go build ./...
	
# Default test target
test:
	go test -v -cover ./...

vet:
	@echo "=> go vet ./..."
	go vet ./...

fmt:
	@echo "=> gofmt -s -w . (excluding vendor)"
	@find . -name '*.go' -not -path "./vendor/*" -print0 | xargs -0 gofmt -s -w

# Clean up build artifacts (optional)
clean:
	@echo "=> go clean"
	go clean
	@echo "=> Removing data directory (./data) if present"
	@rm -rf data || true
	@echo "=> Removing generated proto Go files (proto/*.pb.go, proto/*_grpc.pb.go)"
	@rm -f proto/*.pb.go proto/*_grpc.pb.go || true
	@echo "=> Removing binaries (*.exe) and ./bin directory if present"
	@rm -f *.exe || true
	@rm -rf bin || true

# Remove all local raft data and logs for a clean cluster start
reset-data:
	@echo "=> removing ./data (raft state, logs, snapshots)"
	@rm -rf data || true
	@mkdir -p data/node0/snap/snapshots data/node1/snap/snapshots data/node2/snap/snapshots

# Wipe data and start a fresh 3-node cluster
start-fresh: reset-data start-nodes

# Tail all node logs (ctrl-c to stop)
tail-logs:
	@echo "=> tailing node logs"
	@tail -F ./data/node0/node.log ./data/node1/node.log ./data/node2/node.log

# Build the example node binary used by the start-nodes helper
build-node: build
	@echo "=> building example node (_examples/raft_cluster/main.go)"
	@go build -o node _examples/raft_cluster/main.go

# Start three terminals (macOS Terminal) running the cluster nodes in order:
# 1) node1, 2) node2, 3) node0 (leader/bootstrap)
# Uses AppleScript (osascript) to open Terminal windows and run commands in the project root.
start-nodes: build-node
	@echo "=> Starting 3 terminals: node1, node2, leader (node0)."
	@osascript \
	    -e 'tell application "Terminal" to do script "cd \"$(CURDIR)\"; ./node -id=node1 -raft-addr=127.0.0.1:12101 -grpc=:50051 -data=./data/node1"' \
	    -e 'tell application "Terminal" to set custom title of front window to "node1"'
	@sleep 1
	@osascript \
	    -e 'tell application "Terminal" to do script "cd \"$(CURDIR)\"; ./node -id=node2 -raft-addr=127.0.0.1:12102 -grpc=:50052 -data=./data/node2"' \
	    -e 'tell application "Terminal" to set custom title of front window to "node2"'
	@sleep 1
	@osascript \
	    -e 'tell application "Terminal" to do script "cd \"$(CURDIR)\"; ./node -id=node0 -raft-addr=127.0.0.1:12100 -grpc=:50050 -data=./data/node0 -bootstrap -voter id=node1,addr=127.0.0.1:12101 -voter id=node2,addr=127.0.0.1:12102"' \
	    -e 'tell application "Terminal" to set custom title of front window to "node0"'

# Build the example client binary
client: build
	@echo "=> building example client (_examples/client/main.go)"
	@go build -o examples-client _examples/client/main.go

# Run the example client with defaults; override with `make run-client ADDR=... KEY=... VALUE=...`
run-client: client
	@echo "=> running example client"
	@./examples-client -addr=$${ADDR:-localhost:50051} -key=$${KEY:-example-key} -value=$${VALUE:-hello-world}

