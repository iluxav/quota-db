#!/bin/bash
# Benchmark runner script for QuotaDB vs Redis comparison
# Starts both services in Docker, runs benchmarks, then cleans up

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_DIR/docker"

echo "=== QuotaDB Benchmark Suite ==="
echo "Project directory: $PROJECT_DIR"
echo ""

# Function to cleanup containers
cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    cd "$DOCKER_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
    echo "Cleanup complete"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if docker compose is available
if ! docker compose version &> /dev/null; then
    echo "Error: docker compose is not available"
    exit 1
fi

# Build QuotaDB first
echo "=== Building QuotaDB ==="
cd "$PROJECT_DIR"
cargo build --release
echo "Build complete"
echo ""

# Start Docker services
echo "=== Starting Docker services ==="
cd "$DOCKER_DIR"

# Clean up any existing containers first
docker compose down -v --remove-orphans 2>/dev/null || true

# Build and start services
docker compose up -d --build

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

# Check Redis
echo -n "Checking Redis... "
if docker compose exec -T redis redis-cli ping | grep -q PONG; then
    echo "OK"
else
    echo "FAILED"
    echo "Redis is not responding"
    exit 1
fi

# Check QuotaDB
echo -n "Checking QuotaDB... "
if echo "PING" | nc -w 2 localhost 6380 | grep -q PONG; then
    echo "OK"
else
    echo "FAILED (trying to continue anyway)"
fi

echo ""
echo "=== Running Internal Benchmarks ==="
cd "$PROJECT_DIR"
cargo bench --bench throughput -- --noplot 2>&1 | tee benchmark_results_internal.txt

echo ""
echo "=== Running Redis Comparison Benchmarks ==="
cargo bench --bench redis_comparison -- --noplot 2>&1 | tee benchmark_results_comparison.txt

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to:"
echo "  - benchmark_results_internal.txt"
echo "  - benchmark_results_comparison.txt"
echo ""
echo "Full Criterion reports available in: target/criterion/"
