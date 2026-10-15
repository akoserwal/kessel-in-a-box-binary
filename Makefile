BINARY_NAME := kessel-box
BIN_DIR := bin
DOCKER_IMAGE := quay.io/project-kessel/kessel-box:latest

.PHONY: all build run verify test test-e2e test-all cross-build docker-build clean

all: build

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/kessel-box
	@ls -lh $(BIN_DIR)/$(BINARY_NAME)

run: build
	./$(BIN_DIR)/$(BINARY_NAME) start --grpc-port=9000 --http-port=8088 --db-path=:memory:

verify: build
	go run ./cmd/verify

test:
	go test -v ./...

test-e2e: build
	cd tests/sdk_e2e && go test -v ./...

test-all: test test-e2e verify

cross-build:
	mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/kessel-box
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/kessel-box
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/kessel-box
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/kessel-box
	@ls -lh $(BIN_DIR)/$(BINARY_NAME)-*

docker-build:
	docker build -t $(DOCKER_IMAGE) .

clean:
	rm -rf $(BIN_DIR) *.db *.db-journal *.db-wal *.db-shm
