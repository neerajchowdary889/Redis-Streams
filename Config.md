# Redis-Streams: Production-Ready Message Queue Module

## Project Structure

```
redis-streams/
├── go.mod
├── go.sum
├── README.md
├── Makefile
├── docker-compose.yml
├── 
├── # Core module files
├── redis_streams.go          # Main Redis Streams implementation
├── config.go                 # Configuration structures and loader
├── logger.go                 # Logger interface and implementation
├── metrics.go                # Metrics system
├── health.go                 # Health check endpoints
├── 
├── # Configuration examples
├── config/
│   ├── order-service.yml     # Order service configuration
│   ├── payment-service.yml   # Payment service configuration
│   ├── notification-service.yml # Notification service config
│   ├── analytics-service.yml # Analytics service config
│   └── base-config.yml       # Base template config
├── 
├── # Example implementations
├── examples/
│   ├── order-service/
│   │   ├── main.go
│   │   └── config.yml
│   ├── payment-service/
│   │   ├── main.go
│   │   └── config.yml
│   └── simple-producer-consumer/
│       ├── producer.go
│       └── consumer.go
├── 
├── # Tests
├── tests/
│   ├── integration_test.go
│   ├── config_test.go
│   ├── metrics_test.go
│   └── health_test.go
├── 
├── # Documentation
├── docs/
│   ├── API.md
│   ├── CONFIGURATION.md
│   ├── DEPLOYMENT.md
│   └── MONITORING.md
├── 
└── # CI/CD and deployment
    ├── .github/
    │   └── workflows/
    │       ├── ci.yml
    │       └── release.yml
    ├── deployments/
    │   ├── kubernetes/
    │   └── docker/
    └── scripts/
        ├── build.sh
        ├── test.sh
        └── deploy.sh
```

## go.mod

```go
module github.com/your-org/redis-streams

go 1.21

require (
	github.com/redis/go-redis/v9 v9.5.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
```

## Makefile

```makefile
.PHONY: help build test lint clean run-examples setup-redis docker-build

# Variables
APP_NAME := redis-streams
VERSION := $(shell git describe --tags --always --dirty)
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GO_VERSION := $(shell go version | awk '{print $$3}')

# Default target
help:
	@echo "Available targets:"
	@echo "  setup-redis     - Start Redis container for development"
	@echo "  build           - Build all example applications"
	@echo "  test            - Run all tests"
	@echo "  test-integration - Run integration tests (requires Redis)"
	@echo "  lint            - Run linters"
	@echo "  clean           - Clean build artifacts"
	@echo "  run-examples    - Run example applications"
	@echo "  docker-build    - Build Docker images"
	@echo "  install         - Install the module locally"

# Setup Redis for development
setup-redis:
	@echo "Starting Redis container..."
	@docker run -d --name redis-streams-dev \
		-p 6379:6379 \
		-v redis-data:/data \
		redis:7-alpine redis-server --appendonly yes
	@echo "Redis available at localhost:6379"

# Stop Redis container
stop-redis:
	@echo "Stopping Redis container..."
	@docker stop redis-streams-dev || true
	@docker rm redis-streams-dev || true

# Build all examples
build:
	@echo "Building examples..."
	@cd examples/order-service && go build -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)" -o ../../bin/order-service .
	@cd examples/payment-service && go build -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)" -o ../../bin/payment-service .
	@cd examples/simple-producer-consumer && go build -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)" -o ../../bin/producer producer.go
	@cd examples/simple-producer-consumer && go build -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)" -o ../../bin/consumer consumer.go
	@echo "Built binaries available in ./bin/"

# Run tests
test:
	@echo "Running unit tests..."
	@go test -v ./...

# Run integration tests (requires Redis)
test-integration:
	@echo "Running integration tests..."
	@go test -v -tags=integration ./tests/

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run ./...
	@go fmt ./...
	@go vet ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@go clean ./...

# Run example applications
run-examples:
	@echo "Starting example order service..."
	@CONFIG_PATH=examples/order-service/config.yml ./bin/order-service &
	@echo "Order service started (PID: $$!)"
	@echo "Health check: http://localhost:8080/health"
	@echo "Metrics: http://localhost:8080/metrics"

# Docker build
docker-build:
	@echo "Building Docker images..."
	@docker build -f examples/order-service/Dockerfile -t $(APP_NAME)-order-service:$(VERSION) .
	@docker build -f examples/payment-service/Dockerfile -t $(APP_NAME)-payment-service:$(VERSION) .

# Install module locally
install:
	@echo "Installing module..."
	@go mod download
	@go mod tidy

# Generate configuration templates
generate-configs:
	@echo "Generating configuration templates..."
	@mkdir -p config/templates
	@go run scripts/generate-config-templates.go

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@goimports -w .

# Update dependencies
update-deps:
	@echo "Updating dependencies..."
	@go get -u ./...
	@go mod tidy

# Benchmark tests
benchmark:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

# Generate documentation
docs:
	@echo "Generating documentation..."
	@godoc -http=:6060 &
	@echo "Documentation available at http://localhost:6060/pkg/github.com/your-org/redis-streams/"

# Security scan
security-scan:
	@echo "Running security scan..."
	@gosec ./...

# Create release
release: clean test lint
	@echo "Creating release $(VERSION)..."
	@git tag -a $(VERSION) -m "Release $(VERSION)"
	@git push origin $(VERSION)
	@goreleaser release --clean

# Development setup
dev-setup: install setup-redis
	@echo "Development environment ready!"
	@echo "1. Redis running on localhost:6379"
	@echo "2. Run 'make build' to build examples"
	@echo "3. Run 'make run-examples' to test"
	@echo "4. Health checks available on :8080"

# Production deployment
deploy-prod:
	@echo "Deploying to production..."
	@kubectl apply -f deployments/kubernetes/
	@echo "Deployment initiated. Check status with: kubectl get pods"

# Monitoring setup
setup-monitoring:
	@echo "Setting up monitoring stack..."
	@docker-compose -f docker-compose.monitoring.yml up -d
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"
```

## Quick Start Commands

```bash
# 1. Initialize new Go module (replace with your module path)
go mod init github.com/your-org/redis-streams

# 2. Install dependencies
go mod tidy

# 3. Start Redis for development
make setup-redis

# 4. Build examples
make build

# 5. Run example service
CONFIG_PATH=config/order-service.yml ./bin/order-service

# 6. Test health endpoints
curl http://localhost:8080/health
curl http://localhost:8080/metrics
curl http://localhost:8080/topics
```

## Environment Variables

```bash
# Redis Configuration
export REDIS_STREAMS_REDIS_HOST=localhost
export REDIS_STREAMS_REDIS_PORT=6379
export REDIS_STREAMS_REDIS_PASSWORD=yourpassword
export REDIS_STREAMS_REDIS_DATABASE=0

# Logging Configuration
export REDIS_STREAMS_LOG_LEVEL=INFO
export REDIS_STREAMS_LOG_FORMAT=json
export REDIS_STREAMS_LOG_OUTPUT=stdout

# Monitoring Configuration
export REDIS_STREAMS_MONITORING_ENABLED=true
export REDIS_STREAMS_MONITORING_PORT=8080

# TLS Configuration (if using TLS)
export REDIS_STREAMS_REDIS_TLS_ENABLED=true
export REDIS_STREAMS_REDIS_TLS_CERT_FILE=/path/to/cert.pem
export REDIS_STREAMS_REDIS_TLS_KEY_FILE=/path/to/key.pem
export REDIS_STREAMS_REDIS_TLS_CA_FILE=/path/to/ca.pem
```

## Docker Compose for Development

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    container_name: redis-streams-dev
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3

  redis-insight:
    image: redislabs/redisinsight:latest
    container_name: redis-insight
    ports:
      - "8001:8001"
    volumes:
      - redis_insight_data:/db
    depends_on:
      - redis

  order-service:
    build:
      context: .
      dockerfile: examples/order-service/Dockerfile
    container_name: order-service
    ports:
      - "8080:8080"
    environment:
      - REDIS_STREAMS_REDIS_HOST=redis
      - REDIS_STREAMS_REDIS_PORT=6379
      - CONFIG_PATH=/app/config.yml
    volumes:
      - ./examples/order-service/config.yml:/app/config.yml
    depends_on:
      - redis
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  redis_data:
  redis_insight_data:
```

## Package Installation

```bash
# Install in your project
go get github.com/your-org/redis-streams

# Import in your code
import redisstreams "github.com/your-org/redis-streams"
```

## Key Features

- ✅ **Production-Ready**: Comprehensive error handling, logging, metrics
- ✅ **Multi-Topic Support**: Configure multiple channels per service
- ✅ **YAML Configuration**: Flexible configuration with environment variables
- ✅ **Health Monitoring**: Built-in health checks and Prometheus metrics
- ✅ **Auto-Retry Logic**: Configurable retry mechanisms and dead letter queues
- ✅ **TLS Support**: Full TLS/SSL support for secure connections
- ✅ **Graceful Shutdown**: Proper resource cleanup and graceful consumer shutdown
- ✅ **Batch Operations**: High-performance batch publishing
- ✅ **Consumer Groups**: Built-in horizontal scaling with Redis consumer groups
- ✅ **Message Claiming**: Automatic handling of stuck/pending messages
