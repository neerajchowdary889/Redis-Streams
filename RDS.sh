#!/bin/bash
set -euo pipefail

# Configuration
DOCKER_COMPOSE="docker-compose"
REDIS_CONTAINER="redis-streams"
APP_BINARY="./bin/redis_streams_grpc"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Helper function to show help
show_help() {
    echo "Usage: ./RDS.sh [command]"
    echo ""
    echo "Available commands:"
    echo "  start         - Start Redis and monitoring services"
    echo "  stop          - Stop all services"
    echo "  restart       - Restart all services"
    echo "  logs          - Show Redis logs"
    echo "  redis-cli     - Connect to Redis CLI"
    echo "  monitor       - Monitor Redis commands"
    echo "  clean         - Clean up containers and volumes"
    echo "  test          - Run tests"
    echo "  deps          - Install Go dependencies"
    echo "  protos        - Generate gRPC protobuf stubs"
    echo "  run-microservice - Run the gRPC microservice"
    echo "  python-client     - Run Python gRPC client"
    echo "  python-test       - Run Python test script"
    echo "  performance-test  - Run Go performance test"
    echo "  benchmark         - Run comprehensive benchmark"
    echo "  debug             - Show debug information about streams"
    echo "  health            - Check the health of the Redis container"
    echo "  grafana           - Open Grafana dashboard in browser"
    echo "  monitoring        - Start all monitoring services"
    echo "  help          - Show this help message"
}

# Start services
start_services() {
    echo -e "${BLUE}Starting Redis Streams services...${NC}"
    $DOCKER_COMPOSE up -d
    echo -e "${GREEN}Services started.${NC}"
    echo "- Redis available at localhost:6379"
    echo "- Redis Commander available at http://localhost:8081"
    echo "- Redis Insight available at http://localhost:5540"
    echo "- Prometheus available at http://localhost:9090"
    echo "- Grafana available at http://localhost:3000 (admin/admin123)"
}

# Stop services
stop_services() {
    echo -e "${BLUE}Stopping all services...${NC}"
    $DOCKER_COMPOSE down
}

# Show logs
show_logs() {
    $DOCKER_COMPOSE logs -f redis
}

# Connect to Redis CLI
redis_cli() {
    docker exec -it $REDIS_CONTAINER redis-cli
}

# Monitor Redis commands
monitor_redis() {
    docker exec -it $REDIS_CONTAINER redis-cli MONITOR
}

# Clean up
cleanup() {
    echo -e "${BLUE}Cleaning up containers and volumes...${NC}"
    $DOCKER_COMPOSE down -v --remove-orphans
    echo -e "${BLUE}Cleaning up build artifacts...${NC}"
    rm -f "$APP_BINARY" redis.conf
    echo -e "${BLUE}Pruning docker system...${NC}"
    docker system prune -f
}

# Install Go dependencies
install_deps() {
    echo -e "${BLUE}Installing Go dependencies...${NC}"
    go mod tidy
    go mod download
}

# Generate protobufs
gen_protos() {
    echo -e "${BLUE}Generating protobuf stubs...${NC}"
    PROTO_DIR="api/proto"
    protoc -I "$PROTO_DIR" \
      --go_out=paths=source_relative:"$PROTO_DIR" \
      --go-grpc_out=paths=source_relative:"$PROTO_DIR" \
      "$PROTO_DIR/redis_streams.proto"
    echo -e "${GREEN}Protobuf generation completed.${NC}"
}

# Run tests
run_tests() {
    echo -e "${BLUE}Running tests...${NC}"
    go test -v ./...
}

# Setup Redis configuration
setup_redis() {
    echo -e "${BLUE}Creating Redis configuration file...${NC}"
    cat > redis.conf << 'EOF'
# Redis configuration for streams
bind 0.0.0.0
protected-mode no
port 6379

# Memory settings
maxmemory 512mb
maxmemory-policy allkeys-lru

# Persistence settings
save 600 1
save 300 10
save 60 10000

# Stream settings
stream-node-max-bytes 4096
stream-node-max-entries 100

# Logging
loglevel notice
logfile ""

# Performance
tcp-keepalive 300
tcp-backlog 511

# Enable keyspace notifications for monitoring
notify-keyspace-events Ex
EOF
    echo -e "${GREEN}Redis configuration created.${NC}"
}


# Debug Redis streams
debug_streams() {
    echo -e "${BLUE}=== All Streams ===${NC}"
    docker exec -it $REDIS_CONTAINER redis-cli --scan --pattern "*" | while read key; do
        # Check if the key is a stream
        if docker exec -i $REDIS_CONTAINER redis-cli type "$key" | grep -q "stream"; then
            echo "$key"
        fi
    done
    
    echo -e "\n${BLUE}=== Orders Stream Info ===${NC}"
    docker exec -it $REDIS_CONTAINER redis-cli XINFO STREAM orders 2>/dev/null || echo "Stream 'orders' not found"
    
    echo -e "\n${BLUE}=== Consumer Groups for 'orders' stream ===${NC}"
    docker exec -it $REDIS_CONTAINER redis-cli XINFO GROUPS orders 2>/dev/null || echo "No consumer groups found for 'orders' stream"
}

# Check Redis health
check_health() {
    echo -e "${BLUE}Checking Redis health...${NC}"
    if docker exec -it $REDIS_CONTAINER redis-cli ping | grep -q "PONG"; then
        echo -e "${GREEN}✓ Redis is running${NC}"
    else
        echo -e "${RED}✗ Redis is not responding${NC}"
        return 1
    fi
    
    echo -e "\n${BLUE}Checking connection info...${NC}"
    docker exec -it $REDIS_CONTAINER redis-cli info server | grep -E 'redis_version|tcp_port|uptime_in_seconds'
}

# Open Grafana dashboard
open_grafana() {
    echo -e "${BLUE}Opening Grafana dashboard...${NC}"
    if command -v open >/dev/null 2>&1; then
        open http://localhost:3000
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open http://localhost:3000
    else
        echo "Please open http://localhost:3000 in your browser"
        echo "Login: admin / admin123"
    fi
}

# Open Prometheus metrics
open_prometheus() {
    echo -e "${BLUE}Opening Prometheus metrics...${NC}"
    if command -v open >/dev/null 2>&1; then
        open http://localhost:9090
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open http://localhost:9090
    else
        echo "Please open http://localhost:9090 in your browser"
    fi
}

# Start monitoring services
start_monitoring() {
    echo -e "${BLUE}Starting monitoring services...${NC}"
    $DOCKER_COMPOSE up -d redis prometheus grafana
    echo -e "${GREEN}Monitoring services started.${NC}"
    echo "- Prometheus: http://localhost:9090"
    echo "- Grafana: http://localhost:3000 (admin/admin123)"
    echo "- Redis Insight: http://localhost:5540"
}

# Run the main gRPC microservice
run_microservice() {
    install_deps
    gen_protos
    echo -e "${BLUE}Building gRPC microservice...${NC}"
    go build -o ./bin/redis_streams_grpc .
    echo -e "${BLUE}Starting gRPC microservice...${NC}"
    CONFIG_PATH=${CONFIG_PATH:-Config/config.yml} ./bin/redis_streams_grpc --health-port 8083 ${GRPC_FLAGS:-}
}


# Run Python gRPC client
run_python_client() {
    echo -e "${BLUE}Running Python gRPC client...${NC}"
    cd examples
    ./run_python_client.sh
    cd ..
}

# Run Python test
run_python_test() {
    echo -e "${BLUE}Running Python test...${NC}"
    cd examples
    python3 simple_test.py
    cd ..
}

# Main command handler
case "$1" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        start_services
        ;;
    logs)
        show_logs
        ;;
    redis-cli)
        redis_cli
        ;;
    monitor)
        monitor_redis
        ;;
    clean)
        cleanup
        ;;
    deps)
        install_deps
        ;;
    test)
        run_tests
        ;;
    protos)
        gen_protos
        ;;
    run-microservice)
        run_microservice
        ;;
    python-client)
        run_python_client
        ;;
    python-test)
        run_python_test
        ;;
    performance-test)
        echo "🚀 Running Go performance test..."
        cd examples
        go run performance_client.go
        cd ..
        ;;
    benchmark)
        echo "📊 Running comprehensive benchmark..."
        echo "Starting Redis and gRPC server..."
        ./RDS.sh start
        sleep 5
        
        echo "Running performance tests..."
        ./RDS.sh performance-test
        
        echo "Benchmark completed!"
        ;;
    debug)
        debug_streams
        ;;
    health)
        check_health
        ;;
    grafana)
        open_grafana
        ;;
    monitoring)
        start_monitoring
        ;;
    help|*)
        show_help
        ;;
esac
