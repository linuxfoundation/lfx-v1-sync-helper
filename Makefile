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

# Docker configuration
DOCKER_REGISTRY=ghcr.io/linuxfoundation/lfx-v1-sync-helper
V1_SYNC_HELPER_IMAGE=$(DOCKER_REGISTRY)/v1-sync-helper:latest
MELTANO_IMAGE=$(DOCKER_REGISTRY)/meltano:latest

.PHONY: all build clean test test-coverage deps fmt lint vet check install-lint run run-debug debug docker-build-v1-sync-helper docker-build-meltano docker-run-v1-sync-helper docker-run-meltano docker-build-all update-deps help

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
	@find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".venv" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf meltano/.meltano
	@rm -rf megalinter-reports/

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

# Build v1-sync-helper Docker image
docker-build-v1-sync-helper:
	@echo "Building v1-sync-helper Docker image..."
	docker build -f docker/Dockerfile.v1-sync-helper -t $(V1_SYNC_HELPER_IMAGE) .

# Build meltano Docker image
docker-build-meltano:
	@echo "Building meltano Docker image..."
	docker build -f docker/Dockerfile.meltano -t $(MELTANO_IMAGE) .

# Build all Docker images
docker-build-all: docker-build-v1-sync-helper docker-build-meltano

# Run v1-sync-helper Docker container
docker-run-v1-sync-helper: docker-build-v1-sync-helper
	@echo "Running v1-sync-helper Docker container..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create one from cmd/lfx-v1-sync-helper/config.example.env"; \
		exit 1; \
	fi
	docker run --rm -p 8080:8080 \
		--env-file .env \
		$(V1_SYNC_HELPER_IMAGE)

# Run meltano Docker container
docker-run-meltano: docker-build-meltano
	@echo "Summoning a dragon with the meltano Docker container..."
	docker run --rm -it $(MELTANO_IMAGE) dragon

# Update dependencies
update-deps:
	@echo "Updating dependencies..."
	$(GOGET) -u ./...
	$(GOMOD) tidy

# Show help
help:
	@echo "Available targets:"
	@echo "  all                        - Run clean, deps, fmt, lint, test, build"
	@echo "  build                      - Build the binary"
	@echo "  debug                      - Build with debug symbols and race detection"
	@echo "  clean                      - Clean build artifacts"
	@echo "  test                       - Run tests"
	@echo "  test-coverage              - Run tests with coverage report"
	@echo "  deps                       - Download and tidy dependencies"
	@echo "  fmt                        - Format Go code"
	@echo "  vet                        - Run go vet"
	@echo "  lint                       - Run golangci-lint"
	@echo "  check                      - Run fmt, vet, and lint"
	@echo "  install-lint               - Install golangci-lint"
	@echo "  run                        - Build and run the application"
	@echo "  run-debug                  - Build with debug and run the application"
	@echo "  docker-build-v1-sync-helper- Build v1-sync-helper Docker image"
	@echo "  docker-build-meltano       - Build meltano Docker image"
	@echo "  docker-build-all           - Build all Docker images"
	@echo "  docker-run-v1-sync-helper  - Build and run v1-sync-helper container (requires .env file)"
	@echo "  docker-run-meltano         - Build and run meltano container"
	@echo "  update-deps                - Update all dependencies"
	@echo "  help                       - Show this help message"
