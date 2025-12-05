# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# Binary name
BINARY_NAME=lfx-v1-sync-helper
BINARY_PATH=bin/$(BINARY_NAME)
CMD_PATH=cmd/lfx-v1-sync-helper

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Build flags
BUILD_FLAGS=-ldflags="-s -w"
DEBUG_FLAGS=-race -gcflags="all=-N -l"

.PHONY: all build clean test deps fmt lint vet check run debug docker-build docker-run help

# Default target
all: clean deps fmt lint test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	$(GOBUILD) $(BUILD_FLAGS) -o $(BINARY_PATH) ./$(CMD_PATH)

# Build with debug symbols and race detection
debug:
	@echo "Building $(BINARY_NAME) with debug symbols..."
	@mkdir -p bin
	$(GOBUILD) $(DEBUG_FLAGS) -o $(BINARY_PATH) ./$(CMD_PATH)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf bin/

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@mkdir -p coverage
	$(GOTEST) -v -race -coverprofile=coverage/coverage.out ./...
	$(GOCMD) tool cover -html=coverage/coverage.out -o coverage/coverage.html
	@echo "Coverage report generated at coverage/coverage.html"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Format Go code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Run go vet
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

# Run golangci-lint (requires golangci-lint to be installed)
lint:
	@echo "Running golangci-lint..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Install with: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b \$$(go env GOPATH)/bin v1.54.2"; \
	fi

# Run all checks
check: fmt vet lint

# Install golangci-lint
install-lint:
	@echo "Installing golangci-lint..."
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.54.2

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	./$(BINARY_PATH)

# Run with debug logging
run-debug: debug
	@echo "Running $(BINARY_NAME) with debug logging..."
	./$(BINARY_PATH) -d

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -f docker/Dockerfile.v1-sync-helper -t ghcr.io/linuxfoundation/lfx-v1-sync-helper/$(BINARY_NAME):latest .

# Run Docker container
docker-run: docker-build
	@echo "Running Docker container..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create one from cmd/lfx-v1-sync-helper/config.example.env"; \
		exit 1; \
	fi
	docker run --rm -p 8080:8080 \
		--env-file .env \
		ghcr.io/linuxfoundation/lfx-v1-sync-helper/$(BINARY_NAME):latest

# Update dependencies
update-deps:
	@echo "Updating dependencies..."
	$(GOGET) -u ./...
	$(GOMOD) tidy

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Run clean, deps, fmt, lint, test, build"
	@echo "  build        - Build the binary"
	@echo "  debug        - Build with debug symbols and race detection"
	@echo "  clean        - Clean build artifacts"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage report"
	@echo "  deps         - Download and tidy dependencies"
	@echo "  fmt          - Format Go code"
	@echo "  vet          - Run go vet"
	@echo "  lint         - Run golangci-lint"
	@echo "  check        - Run fmt, vet, and lint"
	@echo "  install-lint - Install golangci-lint"
	@echo "  run          - Build and run the application"
	@echo "  run-debug    - Build with debug and run the application"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Build and run Docker container (requires .env file)"
	@echo "  update-deps  - Update all dependencies"
	@echo "  help         - Show this help message"
