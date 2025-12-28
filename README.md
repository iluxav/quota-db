# QuotaDB

A high-performance distributed counter database built in Rust. QuotaDB provides Redis-compatible commands with CRDT-based replication for eventual consistency across nodes.

## Features

- **Redis Protocol Compatible** - Use any Redis client (redis-cli, redis-py, etc.)
- **PN-Counter CRDTs** - Conflict-free replicated counters for distributed counting
- **Rate Limiting** - Built-in quota/rate limit support with token bucket semantics
- **Active-Active Replication** - Multi-node clusters with automatic delta synchronization
- **Anti-Entropy** - Automatic gap detection and repair for consistency
- **High Performance** - Matches or exceeds Redis throughput in benchmarks

## Quick Start

### Build from Source

```bash
# Clone and build
git clone https://github.com/your-org/quota-db.git
cd quota-db
cargo build --release

# Run with defaults (localhost:6380)
./target/release/quota-db
```

### Using Docker

```bash
# Build and run
docker compose -f docker/docker-compose.yml up -d quota-db

# Test with redis-cli
redis-cli -p 6380 PING
```

## Usage

### Basic Commands

QuotaDB supports standard Redis counter commands:

```bash
# Connect with redis-cli
redis-cli -p 6380

# Increment counters
INCR pageviews
INCRBY downloads 100

# Decrement counters
DECR stock
DECRBY inventory 5

# Get current value
GET pageviews

# Set counter to specific value (integer)
SET counter 1000

# Ping/health check
PING
```

### String Caching

QuotaDB supports string values for caching JSON, HTML, and other content:

```bash
# Store JSON data
SET user:123 '{"name":"Alice","email":"alice@example.com"}'

# Store HTML cache
SET page:home '<!DOCTYPE html><html>...</html>'

# Retrieve cached content
GET user:123
GET page:home
```

String values use Last-Write-Wins (LWW) semantics for conflict resolution in multi-node clusters. Each write includes a timestamp, and the newest value wins during replication.

### Key Operations

```bash
# List all keys matching a pattern
KEYS user:*
KEYS *

# Scan keys incrementally (cursor-based)
SCAN 0 MATCH user:* COUNT 100

# Get database size
DBSIZE
```

### Monitoring

```bash
# Server info and statistics
INFO
INFO server
INFO replication
INFO stats

# Cluster status (when replication enabled)
CLUSTER INFO
```

### Rate Limiting (Quotas)

QuotaDB includes built-in rate limiting with token bucket semantics:

```bash
# Set up a quota: 100 requests per 60 seconds
QUOTASET api:user:123 100 60

# Consume tokens (returns remaining, or -1 if exhausted)
INCR api:user:123

# Check quota info (limit, window, remaining)
QUOTAGET api:user:123

# Remove quota (convert back to regular counter)
QUOTADEL api:user:123
```

## Configuration

### Command Line Options

```
quota-db [OPTIONS]

Options:
  -b, --bind <ADDR>              Network binding address [default: 127.0.0.1:6380]
  -s, --shards <NUM>             Number of shards (power of 2 recommended) [default: 64]
  -n, --node-id <ID>             This node's unique identifier [default: 1]
      --max-connections <NUM>    Maximum concurrent connections [default: 10000]
      --enable-ttl <BOOL>        Enable TTL support [default: true]
      --ttl-tick-ms <MS>         TTL check interval [default: 100]
      --log-level <LEVEL>        Log level (trace, debug, info, warn, error) [default: info]

Replication Options:
      --peers <ADDR,ADDR,...>    Peer nodes for replication (comma-separated)
      --replication-port <PORT>  Port for replication connections [default: 6381]
      --batch-max-size <NUM>     Max deltas per batch [default: 100]
      --batch-max-delay-ms <MS>  Max delay before flushing batch [default: 10]
```

### Examples

```bash
# Single node, custom port
./quota-db --bind 0.0.0.0:6390 --shards 128

# Three-node cluster
# Node 1
./quota-db --node-id 1 --bind 0.0.0.0:6380 \
  --peers "node2:6381,node3:6381"

# Node 2
./quota-db --node-id 2 --bind 0.0.0.0:6380 \
  --peers "node1:6381,node3:6381"

# Node 3
./quota-db --node-id 3 --bind 0.0.0.0:6380 \
  --peers "node1:6381,node2:6381"
```

## Architecture

### Data Model

QuotaDB uses PN-Counter CRDTs for conflict-free replication:

```
┌─────────────────────────────────────────┐
│           Counter: "pageviews"          │
├─────────────────────────────────────────┤
│  P (increments)    │  N (decrements)    │
│  ├── Node 1: 150   │  ├── Node 1: 10    │
│  ├── Node 2: 200   │  ├── Node 2: 5     │
│  └── Node 3: 100   │  └── Node 3: 0     │
├─────────────────────────────────────────┤
│  Value = ΣP - ΣN = 450 - 15 = 435       │
└─────────────────────────────────────────┘
```

Each node tracks its own increments (P) and decrements (N). The current value is always `sum(P) - sum(N)`. Nodes merge by taking the max of each component, ensuring eventual consistency without conflicts.

### Sharding

Keys are distributed across shards using consistent hashing:

```
shard_id = hash(key) % num_shards
```

Each shard has:

- Independent HashMap storage
- Its own replication log
- Its own TTL queue
- Rolling hash digest for anti-entropy

### Replication

Active replication streams deltas between nodes:

```
┌─────────┐    delta stream    ┌─────────┐
│ Node 1  │◄──────────────────►│ Node 2  │
│         │                    │         │
└────┬────┘                    └────┬────┘
     │                              │
     │        delta stream          │
     └──────────────┬───────────────┘
                    │
               ┌────▼────┐
               │ Node 3  │
               └─────────┘
```

- **Delta Streaming**: Only changed data is sent (not full state)
- **Batching**: Deltas are batched for efficiency (configurable)
- **Anti-Entropy**: Periodic consistency checks detect and repair gaps

## Performance

Benchmarks vs Redis 7 (2 CPU, 2GB RAM Docker containers):

### Counter Operations (INCR)

| Benchmark             |     QuotaDB |       Redis | Comparison |
| --------------------- | ----------: | ----------: | ---------- |
| Single connection     |   3,569 rps |   3,544 rps | 100.7%     |
| Concurrent (100 conn) |  63,492 rps |  62,539 rps | 101.5%     |
| Pipelined (P=16)      | 917,431 rps | 851,789 rps | 107.7%     |

### String Operations (SET)

| Benchmark            |     QuotaDB |       Redis | Comparison |
| -------------------- | ----------: | ----------: | ---------- |
| Single connection    |   3,674 rps |   3,673 rps | 100.0%     |
| Concurrent (50 conn) |  48,309 rps |  48,520 rps | 99.6%      |
| Pipelined (P=16)     | 568,182 rps | 588,235 rps | 96.6%      |

### Running Benchmarks

```bash
# Start QuotaDB and Redis
docker compose -f docker/docker-compose.yml up -d

# Counter benchmarks
redis-benchmark -p 6380 -n 100000 -q -t ping,incr,get
redis-benchmark -p 6379 -n 100000 -q -t ping,incr,get

# String SET benchmarks
redis-benchmark -p 6380 -n 100000 -t set -c 50
redis-benchmark -p 6379 -n 100000 -t set -c 50

# Pipelined benchmarks
redis-benchmark -p 6380 -n 100000 -t set -c 50 -P 16
redis-benchmark -p 6379 -n 100000 -t set -c 50 -P 16

# Stop containers
docker compose -f docker/docker-compose.yml down
```

## Supported Commands

### Core Commands

| Command            | Description                                  |
| ------------------ | -------------------------------------------- |
| `PING [msg]`       | Health check, returns PONG or echoes message |
| `GET key`          | Get value (counter integer or cached string) |
| `SET key value`    | Set string value (for caching)               |
| `INCR key`         | Increment counter by 1                       |
| `INCRBY key delta` | Increment counter by delta                   |
| `DECR key`         | Decrement counter by 1                       |
| `DECRBY key delta` | Decrement counter by delta                   |

### Rate Limiting

| Command                     | Description                                         |
| --------------------------- | --------------------------------------------------- |
| `QUOTASET key limit window` | Create rate limit (limit tokens per window seconds) |
| `QUOTAGET key`              | Get quota info: [limit, window_secs, remaining]     |
| `QUOTADEL key`              | Delete quota, convert back to regular counter       |

### Key Management

| Command                                 | Description                                         |
| --------------------------------------- | --------------------------------------------------- |
| `KEYS pattern`                          | List keys matching glob pattern (`*`, `?`, `[abc]`) |
| `SCAN cursor [MATCH pattern] [COUNT n]` | Incrementally iterate keys                          |
| `DBSIZE`                                | Return total number of keys                         |

### Monitoring

| Command          | Description                                         |
| ---------------- | --------------------------------------------------- |
| `INFO [section]` | Server statistics (server, replication, stats, all) |
| `CLUSTER INFO`   | Cluster status and peer connections                 |

### Compatibility

| Command                  | Description                   |
| ------------------------ | ----------------------------- |
| `CONFIG GET param`       | Returns empty (compatibility) |
| `CONFIG SET param value` | No-op (compatibility)         |
| `FLUSHDB`                | No-op (compatibility)         |

## Development

### Running Tests

```bash
# All tests
cargo test

# With output
cargo test -- --nocapture

# Specific test
cargo test test_incr
```

### Building for Release

```bash
cargo build --release
./target/release/quota-db --help
```

### Project Structure

```
src/
├── main.rs              # Entry point
├── config.rs            # CLI configuration
├── error.rs             # Error types
├── protocol/            # RESP protocol handling
│   ├── parser.rs        # Zero-copy parsing
│   ├── encoder.rs       # Response encoding
│   └── command.rs       # Command definitions
├── server/              # Network layer
│   ├── listener.rs      # TCP acceptor
│   ├── connection.rs    # Per-connection handling
│   └── handler.rs       # Command dispatch
├── engine/              # Storage engine
│   ├── db.rs            # ShardedDb orchestrator
│   ├── shard.rs         # Individual shard
│   ├── entry.rs         # PN-Counter implementation
│   └── quota.rs         # Rate limiting logic
└── replication/         # Cluster replication
    ├── manager.rs       # Replication orchestration
    ├── protocol.rs      # Replication messages
    ├── anti_entropy.rs  # Gap detection/repair
    └── digest.rs        # Rolling hash for consistency
```

## License

MIT License - see LICENSE file for details.
