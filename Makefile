# QuotaDB Makefile
# High-performance distributed counter database

.PHONY: build release run run-release test bench clean docker docker-up docker-down docker-bench fmt lint check help

# Default target
all: build

# Build debug binary
build:
	cargo build

# Build optimized release binary
release:
	cargo build --release

# Run in debug mode
run:
	cargo run -- --bind 127.0.0.1:6380

# Run optimized release binary
run-release: release
	./target/release/quota-db --bind 127.0.0.1:6380

# Run with persistence enabled
run-persist: release
	./target/release/quota-db --bind 127.0.0.1:6380 --data-dir /tmp/quota-db-data

# Run all tests
test:
	cargo test

# Run tests with output
test-verbose:
	cargo test -- --nocapture

# Run specific test module
test-%:
	cargo test $* -- --nocapture

# Run benchmarks
bench:
	cargo bench

# Run clippy linter
lint:
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Check formatting without modifying
fmt-check:
	cargo fmt -- --check

# Type check without building
check:
	cargo check

# Clean build artifacts
clean:
	cargo clean
	rm -rf /tmp/quota-db-*

# Docker commands
docker:
	docker build -t quota-db -f docker/Dockerfile .

docker-up:
	docker compose -f docker/docker-compose.yml up -d

docker-down:
	docker compose -f docker/docker-compose.yml down

docker-logs:
	docker compose -f docker/docker-compose.yml logs -f

# Docker cluster (3 nodes)
cluster-up:
	docker compose -f docker/docker-compose.cluster.yml up -d

cluster-down:
	docker compose -f docker/docker-compose.cluster.yml down

cluster-logs:
	docker compose -f docker/docker-compose.cluster.yml logs -f

# Benchmark against Redis in Docker
docker-bench: docker-up
	@echo "=== QuotaDB vs Redis Benchmark ==="
	@echo ""
	@echo "PING (single connection):"
	@echo -n "  QuotaDB: " && redis-benchmark -p 6380 -c 1 -n 100000 -t ping -q 2>&1 | grep "requests per second"
	@echo -n "  Redis:   " && redis-benchmark -p 6379 -c 1 -n 100000 -t ping -q 2>&1 | grep "requests per second"
	@echo ""
	@echo "INCR (pipelined, 50 connections):"
	@echo -n "  QuotaDB: " && redis-benchmark -p 6380 -c 50 -P 16 -n 1000000 -t incr -q 2>&1 | grep "requests per second"
	@echo -n "  Redis:   " && redis-benchmark -p 6379 -c 50 -P 16 -n 1000000 -t incr -q 2>&1 | grep "requests per second"
	@echo ""
	@echo "GET (pipelined, 50 connections):"
	@echo -n "  QuotaDB: " && redis-benchmark -p 6380 -c 50 -P 16 -n 1000000 -t get -q 2>&1 | grep "requests per second"
	@echo -n "  Redis:   " && redis-benchmark -p 6379 -c 50 -P 16 -n 1000000 -t get -q 2>&1 | grep "requests per second"

# Quick smoke test
smoke-test: release
	@echo "Starting QuotaDB..."
	@./target/release/quota-db --bind 127.0.0.1:6380 & echo $$! > /tmp/quota-db.pid
	@sleep 2
	@echo "Running smoke tests..."
	@redis-cli -p 6380 PING
	@redis-cli -p 6380 INCR test:counter
	@redis-cli -p 6380 GET test:counter
	@redis-cli -p 6380 KEYS "*"
	@redis-cli -p 6380 INFO | head -20
	@echo "Stopping QuotaDB..."
	@kill $$(cat /tmp/quota-db.pid) 2>/dev/null || true
	@rm -f /tmp/quota-db.pid
	@echo "Smoke test passed!"

# Install development dependencies
dev-setup:
	cargo install cargo-watch cargo-flamegraph

# Watch mode - rebuild on file changes
watch:
	cargo watch -x build

# Watch mode - run tests on file changes
watch-test:
	cargo watch -x test

# Generate flamegraph (requires cargo-flamegraph)
flamegraph: release
	cargo flamegraph --bin quota-db -- --bind 127.0.0.1:6380

# Help
help:
	@echo "QuotaDB Makefile Commands:"
	@echo ""
	@echo "  Build:"
	@echo "    make build        - Build debug binary"
	@echo "    make release      - Build optimized release binary"
	@echo "    make check        - Type check without building"
	@echo "    make clean        - Clean build artifacts"
	@echo ""
	@echo "  Run:"
	@echo "    make run          - Run debug binary on port 6380"
	@echo "    make run-release  - Run release binary on port 6380"
	@echo "    make run-persist  - Run with persistence enabled"
	@echo ""
	@echo "  Test:"
	@echo "    make test         - Run all tests"
	@echo "    make test-verbose - Run tests with output"
	@echo "    make test-<name>  - Run specific test module"
	@echo "    make smoke-test   - Quick integration test"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make fmt          - Format code"
	@echo "    make fmt-check    - Check formatting"
	@echo "    make lint         - Run clippy linter"
	@echo ""
	@echo "  Benchmarks:"
	@echo "    make bench        - Run Rust benchmarks"
	@echo "    make docker-bench - Benchmark vs Redis in Docker"
	@echo ""
	@echo "  Docker:"
	@echo "    make docker       - Build Docker image"
	@echo "    make docker-up    - Start QuotaDB + Redis containers"
	@echo "    make docker-down  - Stop containers"
	@echo "    make docker-logs  - View container logs"
	@echo ""
	@echo "  Cluster:"
	@echo "    make cluster-up   - Start 3-node cluster"
	@echo "    make cluster-down - Stop cluster"
	@echo "    make cluster-logs - View cluster logs"
	@echo ""
	@echo "  Development:"
	@echo "    make dev-setup    - Install dev dependencies"
	@echo "    make watch        - Rebuild on file changes"
	@echo "    make watch-test   - Run tests on file changes"
	@echo "    make flamegraph   - Generate CPU flamegraph"
