# Makefile for dkvs project

.PHONY: test build clean vet fmt

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
