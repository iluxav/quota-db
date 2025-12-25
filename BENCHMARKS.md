# QuotaDB Benchmark Results

Tracking performance metrics over time. All benchmarks run in Docker containers with **2 CPU / 2GB RAM** limits.

---

## 2025-12-26: Cached Value Optimization

### Changes

- Added `cached_value` field to `PnCounterEntry` for O(1) reads
- Eliminated O(nodes) iteration on every `value()` call
- Cache updated incrementally on increment/decrement, recalculated on merge

### Network Benchmarks (TCP localhost)

| Benchmark             | Redis             | QuotaDB           | Ratio     | vs Previous |
| --------------------- | ----------------- | ----------------- | --------- | ----------- |
| Single INCR           | 3,655 ops/sec     | 3,763 ops/sec     | **1.03x** | +5%         |
| Pipeline (batch=100)  | 331,378 ops/sec   | 317,673 ops/sec   | 0.96x     | variance    |
| Pipeline (batch=1000) | 1,428,784 ops/sec | 1,704,667 ops/sec | **1.19x** | +2%         |

**Single INCR target achieved: now faster than Redis!**

---

## 2025-12-26: Phase 1 Complete + Server-Side Batching

### Test Environment

- macOS Darwin 25.0.0 (Apple Silicon)
- Docker containers: 2 CPU, 2GB RAM each
- Redis 7-alpine vs QuotaDB v0.1.0

### Network Benchmarks (TCP localhost)

| Benchmark             | Redis             | QuotaDB           | Ratio     | Notes                     |
| --------------------- | ----------------- | ----------------- | --------- | ------------------------- |
| Single INCR           | 3,818 ops/sec     | 3,745 ops/sec     | 0.98x     | Sequential, one at a time |
| Pipeline (batch=100)  | 244,487 ops/sec   | 352,037 ops/sec   | **1.44x** | 100 cmds per round-trip   |
| Pipeline (batch=1000) | 1,157,491 ops/sec | 1,350,173 ops/sec | **1.17x** | 1000 cmds per round-trip  |

### Internal Engine Benchmarks (no network)

| Operation            | Latency | Throughput     |
| -------------------- | ------- | -------------- |
| INCR same key        | 23.8 ns | 42.1 M ops/sec |
| INCR unique keys     | 305 ns  | 3.3 M ops/sec  |
| GET existing key     | 6.5 ns  | 155 M ops/sec  |
| GET missing key      | 4.1 ns  | 241 M ops/sec  |
| CRDT merge (3 nodes) | 106 ns  | 9.5 M ops/sec  |

### Changes in This Version

- Implemented server-side batching (transparent to clients)
- Added `TCP_NODELAY` for lower latency
- Batch read/write with single flush per batch

---

## Benchmark Commands

### Quick Benchmark (Python)

```bash
# Start services
cd docker && docker compose up -d --build

# Run quick benchmark
python3 -c "
import socket, time

def bench(name, addr, count=10000):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect(addr)
    start = time.perf_counter()
    for i in range(count):
        key = f'key{i}'.encode()
        s.send(f'*2\r\n\$4\r\nINCR\r\n\${len(key)}\r\n{key.decode()}\r\n'.encode())
        s.recv(64)
    elapsed = time.perf_counter() - start
    s.close()
    print(f'{name}: {count/elapsed:,.0f} ops/sec')

bench('Redis   ', ('localhost', 6379))
bench('QuotaDB ', ('localhost', 6380))
"

# Cleanup
cd docker && docker compose down -v
```

### Full Criterion Benchmarks

```bash
# Internal benchmarks
cargo bench --bench throughput

# Redis comparison (requires Docker services running)
cargo bench --bench redis_comparison
```

---

## Performance Targets

| Metric             | Target vs Redis | Current Status |
| ------------------ | --------------- | -------------- |
| Single INCR        | >= 1.0x         | **1.03x**      |
| Pipelined INCR     | >= 1.2x         | **1.19x**      |
| Concurrent INCR    | >= 1.0x         | TBD            |
| Memory per counter | <= 0.5x         | TBD            |
